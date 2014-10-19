#include <cstring>
#include <exception>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <boost/thread/thread.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"
#include "mmap_container.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace {

using namespace simulation_grid::grid_db;

static const char* MVCC_MMAP_FILE_TYPE_TAG = "simulation_grid::grid_db::mvcc_mmap_container";

size_t get_size(const mvcc_mmap_container& container)
{
    return container.file.get_size();
}

void init(mvcc_mmap_container& container)
{
    container.file.construct<mvcc_mmap_header>(HEADER_KEY)();
    container.file.construct<mvcc_mmap_resource_pool>(RESOURCE_POOL_KEY)(&container.file);
}

void check(const mvcc_mmap_container& container)
{
    const mvcc_mmap_header* header = find_const<mvcc_mmap_header>(container, HEADER_KEY);
    if (UNLIKELY_EXT(!header))
    {
	throw malformed_db_error("Could not find header")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
    // If endianess is different the indicator will be 65280 instead of 255
    if (UNLIKELY_EXT(header->endianess_indicator != std::numeric_limits<boost::uint8_t>::max()))
    {
	throw unsupported_db_error("Container requires byte swapping")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_version_found(header->container_version);
    }
    if (UNLIKELY_EXT(strncmp(header->file_type_tag, MVCC_MMAP_FILE_TYPE_TAG, sizeof(header->file_type_tag))))
    {
	throw malformed_db_error("Incorrect file type tag found")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
    if (UNLIKELY_EXT(header->container_version < mvcc_mmap_container::MIN_SUPPORTED_VERSION ||
	    header->container_version > mvcc_mmap_container::MAX_SUPPORTED_VERSION))
    {
	throw unsupported_db_error("Unsuported container version")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_version_found(header->container_version)
		<< info_min_supported_version(mvcc_mmap_container::MIN_SUPPORTED_VERSION)
		<< info_max_supported_version(mvcc_mmap_container::MAX_SUPPORTED_VERSION);
    }
    if (UNLIKELY_EXT(sizeof(mvcc_mmap_header) != header->header_size))
    {
	throw malformed_db_error("Wrong header size")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
}

reader_token_id acquire_reader_token(mvcc_mmap_container& container)
{
    reader_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool(container).reader_free_list.pop(reservation)))
    {
	throw busy_condition("No reader token available")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container");
    }
    return reservation;
}

void release_reader_token(mvcc_mmap_container& container, const reader_token_id& id)
{
    bra::mt19937 seed;
    bra::uniform_int_distribution<> generator(100, 200);
    while (UNLIKELY_EXT(!mut_resource_pool(container).reader_free_list.push(id)))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

writer_token_id acquire_writer_token(mvcc_mmap_container& container)
{
    writer_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool(container).writer_free_list.pop(reservation)))
    {
	throw busy_condition("No writer token available")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_mmap_container");
    }
    return reservation;
}

void release_writer_token(mvcc_mmap_container& container, const writer_token_id& id)
{
    bra::mt19937 seed;
    bra::uniform_int_distribution<> generator(100, 200);
    while (!UNLIKELY_EXT(mut_resource_pool(container).writer_free_list.push(id)))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

mmap_key::mmap_key()
{
    c_str[0] = '\0';
}

mmap_key::mmap_key(const char* key)
{
    if (UNLIKELY_EXT(strlen(key) > MVCC_MAX_KEY_LENGTH))
    {
	throw grid_db_error("Maximum key length exceeded")
		<< info_component_identity("mmap_key")
		<< info_data_identity(key);
    }
    strncpy(c_str, key, sizeof(c_str));
}

mmap_key::mmap_key(const mmap_key& other)
{
    strncpy(c_str, other.c_str, sizeof(c_str));
}

mmap_key::~mmap_key()
{ }

mmap_key& mmap_key::operator=(const mmap_key& other)
{
    if (this != &other)
    {
	strncpy(c_str, other.c_str, sizeof(c_str));
    }
    return *this;
}

bool mmap_key::operator<(const mmap_key& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) < 0;
}

mmap_deleter::mmap_deleter() :
    key(), function()
{ }

mmap_deleter::mmap_deleter(const mmap_key& k, const delete_function& fn) :
    key(k), function(fn)
{ }

mmap_deleter::mmap_deleter(const mmap_deleter& other) :
    key(other.key), function(other.function)
{ }

mmap_deleter::~mmap_deleter()
{ }

mmap_deleter& mmap_deleter::operator=(const mmap_deleter& other)
{
    if (this != &other)
    {
	key = other.key;
	function = other.function;
    }
    return *this;
}

const version mvcc_mmap_container::MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version mvcc_mmap_container::MAX_SUPPORTED_VERSION(1, 1, 1, 1);

const mvcc_mmap_header& const_header(const mvcc_mmap_container& container)
{
    return *find_const<const mvcc_mmap_header>(container, HEADER_KEY);
}

mvcc_mmap_header& mut_header(mvcc_mmap_container& container)
{
    return *find_mut<mvcc_mmap_header>(container, HEADER_KEY);
}

const mvcc_mmap_resource_pool& const_resource_pool(const mvcc_mmap_container& container)
{
    return *find_const<const mvcc_mmap_resource_pool>(container, RESOURCE_POOL_KEY);
}

// Note: the parameter is const since const operations on the container still
// need to modify the mutexes inside the resource pool
mvcc_mmap_resource_pool& mut_resource_pool(const mvcc_mmap_container& container)
{
    return *find_mut<mvcc_mmap_resource_pool>(const_cast<mvcc_mmap_container&>(container), RESOURCE_POOL_KEY);
}

mvcc_mmap_header::mvcc_mmap_header() :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    container_version(mvcc_mmap_container::MAX_SUPPORTED_VERSION), 
    header_size(sizeof(mvcc_mmap_header))
{
    strncpy(file_type_tag, MVCC_MMAP_FILE_TYPE_TAG, sizeof(file_type_tag));
}

mvcc_mmap_owner_token::mvcc_mmap_owner_token(bip::managed_mapped_file* file) :
    registry(std::less<registry_map::key_type>(), file->get_segment_manager())
{ }

mvcc_mmap_resource_pool::mvcc_mmap_resource_pool(bip::managed_mapped_file* file) :
    global_revision(1),
    owner_token(file),
    reader_free_list(file->get_segment_manager()),
    writer_free_list(file->get_segment_manager()),
    deleter_list(file->get_segment_manager())
{
    for (reader_token_id id = 0; id < MVCC_READER_LIMIT; ++id)
    {
	reader_free_list.push(id);
    }
    for (writer_token_id id = 0; id < MVCC_WRITER_LIMIT; ++id)
    {
	writer_free_list.push(id);
    }
}

mvcc_mmap_container::mvcc_mmap_container(const owner_t, const bfs::path& path, size_t size) :
    exists(bfs::exists(path)), path(path), file(bip::open_or_create, path.string().c_str(), size)
{ }

mvcc_mmap_container::mvcc_mmap_container(const reader_t, const bfs::path& path) :
    exists(bfs::exists(path)), path(path), file(bip::open_only, path.string().c_str())
{ }

std::size_t mvcc_mmap_container::available_space() const
{
    return file.get_free_memory();
}

mvcc_mmap_reader::mvcc_mmap_reader(const bfs::path& path) :
    container_(reader, path), reader_handle_(container_.file)
{
    if (bfs::exists(path))
    {
	reader_handle_.check();
    }
    else
    {
	throw grid_db_error("Given container does not exist")
		<< info_db_identity(path.string());
    }
}

mvcc_mmap_reader::~mvcc_mmap_reader()
{ }

mvcc_mmap_owner::mvcc_mmap_owner(const bfs::path& path, std::size_t size) :
    exists_(bfs::exists(path)),
    container_(owner, path, size),
    owner_handle_(exists_ ? open_existing : open_new, container_.file),
    writer_handle_(container_.file),
    reader_handle_(container_.file),
    file_lock_(path.string().c_str())
{
    flush();
    file_lock_.lock();
}

mvcc_mmap_owner::~mvcc_mmap_owner()
{
    try
    {
	file_lock_.unlock();
    }
    catch(...)
    {
	// Do nothing
    }
}

void mvcc_mmap_owner::process_read_metadata(reader_token_id from, reader_token_id to)
{
    return owner_handle_.process_read_metadata(from, to);
}

void mvcc_mmap_owner::process_write_metadata(std::size_t max_attempts)
{
    return owner_handle_.process_write_metadata(max_attempts);
}

std::string mvcc_mmap_owner::collect_garbage(std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(max_attempts);
}

std::string mvcc_mmap_owner::collect_garbage(const std::string& from, std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(from, max_attempts);
}

void mvcc_mmap_owner::flush()
{
    boost::function<void ()> flush_func(boost::bind(&mvcc_mmap_owner::flush_impl, boost::ref(*this)));
    container_.file.get_segment_manager()->atomic_func(flush_func);
}

void mvcc_mmap_owner::flush_impl()
{
    mut_resource_pool(container_).owner_token.last_flush_timestamp = bpt::microsec_clock::local_time();
    mut_resource_pool(container_).owner_token.last_flush_revision = const_resource_pool(container_).global_revision;
    container_.file.flush();
}

} // namespace grid_db
} // namespace simulation_grid
