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
#include "mvcc_container.hpp"
#include "mvcc_container.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace {

using namespace simulation_grid::grid_db;

static const char* HEADER_KEY = "@@HEADER@@";
static const char* RESOURCE_POOL_KEY = "@@RESOURCE_POOL@@";
static const char* MVCC_FILE_TYPE_TAG = "simulation_grid::grid_db::mvcc_container";

void init(mvcc_container& container)
{
    container.memory.construct<mvcc_header>(HEADER_KEY)();
    container.memory.construct<mvcc_resource_pool>(RESOURCE_POOL_KEY)(&container.memory);
}

void check(const mvcc_container& container)
{
    const mvcc_header* header = find_const<mvcc_header>(container, HEADER_KEY);
    if (UNLIKELY_EXT(!header))
    {
	throw malformed_db_error("Could not find header")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container")
		<< info_data_identity(HEADER_KEY);
    }
    // If endianess is different the indicator will be 65280 instead of 255
    if (UNLIKELY_EXT(header->endianess_indicator != std::numeric_limits<boost::uint8_t>::max()))
    {
	throw unsupported_db_error("Container requires byte swapping")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container")
		<< info_version_found(header->container_version);
    }
    if (UNLIKELY_EXT(strncmp(header->file_type_tag, MVCC_FILE_TYPE_TAG, sizeof(header->file_type_tag))))
    {
	throw malformed_db_error("Incorrect file type tag found")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container")
		<< info_data_identity(HEADER_KEY);
    }
    if (UNLIKELY_EXT(header->container_version < mvcc_container::MIN_SUPPORTED_VERSION ||
	    header->container_version > mvcc_container::MAX_SUPPORTED_VERSION))
    {
	throw unsupported_db_error("Unsuported container version")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container")
		<< info_version_found(header->container_version)
		<< info_min_supported_version(mvcc_container::MIN_SUPPORTED_VERSION)
		<< info_max_supported_version(mvcc_container::MAX_SUPPORTED_VERSION);
    }
    if (UNLIKELY_EXT(sizeof(mvcc_header) != header->header_size))
    {
	throw malformed_db_error("Wrong header size")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container")
		<< info_data_identity(HEADER_KEY);
    }
}

reader_token_id acquire_reader_token(mvcc_container& container)
{
    reader_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool(container).reader_free_list.pop(reservation)))
    {
	throw busy_condition("No reader token available")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container");
    }
    return reservation;
}

void release_reader_token(mvcc_container& container, const reader_token_id& id)
{
    bra::mt19937 seed;
    bra::uniform_int_distribution<> generator(100, 200);
    while (UNLIKELY_EXT(!mut_resource_pool(container).reader_free_list.push(id)))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

writer_token_id acquire_writer_token(mvcc_container& container)
{
    writer_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool(container).writer_free_list.pop(reservation)))
    {
	throw busy_condition("No writer token available")
		<< info_db_identity(container.path.string())
		<< info_component_identity("mvcc_container");
    }
    return reservation;
}

void release_writer_token(mvcc_container& container, const writer_token_id& id)
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

mvcc_key::mvcc_key()
{
    c_str[0] = '\0';
}

mvcc_key::mvcc_key(const char* key)
{
    if (UNLIKELY_EXT(strlen(key) > MVCC_MAX_KEY_LENGTH))
    {
	throw grid_db_error("Maximum key length exceeded")
		<< info_component_identity("mvcc_key")
		<< info_data_identity(key);
    }
    strncpy(c_str, key, sizeof(c_str));
}

mvcc_key::mvcc_key(const mvcc_key& other)
{
    strncpy(c_str, other.c_str, sizeof(c_str));
}

mvcc_key::~mvcc_key()
{ }

mvcc_key& mvcc_key::operator=(const mvcc_key& other)
{
    if (this != &other)
    {
	strncpy(c_str, other.c_str, sizeof(c_str));
    }
    return *this;
}

bool mvcc_key::operator<(const mvcc_key& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) < 0;
}

mvcc_deleter::mvcc_deleter() :
    key(), function()
{ }

mvcc_deleter::mvcc_deleter(const mvcc_key& k, const delete_function& fn) :
    key(k), function(fn)
{ }

mvcc_deleter::mvcc_deleter(const mvcc_deleter& other) :
    key(other.key), function(other.function)
{ }

mvcc_deleter::~mvcc_deleter()
{ }

mvcc_deleter& mvcc_deleter::operator=(const mvcc_deleter& other)
{
    if (this != &other)
    {
	key = other.key;
	function = other.function;
    }
    return *this;
}

const version mvcc_container::MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version mvcc_container::MAX_SUPPORTED_VERSION(1, 1, 1, 1);

const mvcc_header& const_header(const mvcc_container& container)
{
    return *find_const<const mvcc_header>(container, HEADER_KEY);
}

mvcc_header& mut_header(mvcc_container& container)
{
    return *find_mut<mvcc_header>(container, HEADER_KEY);
}

const mvcc_resource_pool& const_resource_pool(const mvcc_container& container)
{
    return *find_const<const mvcc_resource_pool>(container, RESOURCE_POOL_KEY);
}

// Note: the parameter is const since const operations on the container still
// need to modify the mutexes inside the resource pool
mvcc_resource_pool& mut_resource_pool(const mvcc_container& container)
{
    return *find_mut<mvcc_resource_pool>(const_cast<mvcc_container&>(container), RESOURCE_POOL_KEY);
}

mvcc_header::mvcc_header() :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    container_version(mvcc_container::MAX_SUPPORTED_VERSION), 
    header_size(sizeof(mvcc_header))
{
    strncpy(file_type_tag, MVCC_FILE_TYPE_TAG, sizeof(file_type_tag));
}

mvcc_owner_token::mvcc_owner_token(bip::managed_mapped_file* file) :
    registry(std::less<registry_map::key_type>(), file->get_segment_manager())
{ }

mvcc_resource_pool::mvcc_resource_pool(bip::managed_mapped_file* file) :
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

mvcc_container::mvcc_container(const owner_t, const bfs::path& path, size_t size) :
    path(path), memory(bip::open_or_create, path.string().c_str(), size)
{
    if (bfs::exists(path))
    {
	check(*this);
    }
    else
    {
	boost::function<void ()> init_func(boost::bind(&init, boost::ref(*this)));
	memory.get_segment_manager()->atomic_func(init_func);
    }
}

mvcc_container::mvcc_container(const reader_t, const bfs::path& path) :
    path(path), memory(bip::open_only, path.string().c_str())
{
    if (bfs::exists(path))
    {
	check(*this);
    }
    else
    {
	throw grid_db_error("Given container does not exist")
		<< info_db_identity(path.string());
    }
}

std::size_t mvcc_container::available_space() const
{
    return memory.get_free_memory();
}

mvcc_reader_handle::mvcc_reader_handle(mvcc_container& container) :
    container(container), token_id(acquire_reader_token(container))
{ }

mvcc_reader_handle::~mvcc_reader_handle()
{
    try
    {
	release_reader_token(container, token_id);
    }
    catch(...)
    {
	// do nothing
    }
}

mvcc_writer_handle::mvcc_writer_handle(mvcc_container& container) :
    container(container), token_id(acquire_writer_token(container))
{ }

mvcc_writer_handle::~mvcc_writer_handle()
{
    try
    {
	release_writer_token(container, token_id);
    }
    catch(...)
    {
	// do nothing
    }
}

mvcc_reader::mvcc_reader(const bfs::path& path) :
    container_(reader, path), reader_handle_(container_)
{ }

mvcc_reader::~mvcc_reader()
{ }

mvcc_owner::mvcc_owner(const bfs::path& path, std::size_t size) :
    container_(owner, path, size),
    writer_handle_(container_),
    reader_handle_(container_),
    file_lock_(path.string().c_str())
{
    flush();
    file_lock_.lock();
}

mvcc_owner::~mvcc_owner()
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

void mvcc_owner::process_read_metadata(reader_token_id from, reader_token_id to)
{
    if (from >= MVCC_READER_LIMIT)
    {
	return;
    }
    mvcc_resource_pool& pool = mut_resource_pool(container_);
    if (pool.owner_token.oldest_reader_id_found && pool.owner_token.oldest_revision_found)
    {
	reader_token_id token_id = pool.owner_token.oldest_reader_id_found.get();
	if (pool.reader_token_pool[token_id].last_read_revision &&
	    pool.owner_token.oldest_revision_found.get() != pool.reader_token_pool[token_id].last_read_revision.get())
	{
	    pool.owner_token.oldest_reader_id_found.reset();
	    pool.owner_token.oldest_revision_found.reset();
	    pool.owner_token.oldest_timestamp_found.reset();
	}
    }
    for (reader_token_id iter = from; iter < MVCC_READER_LIMIT && iter < to; ++iter)
    {
	if ((!pool.owner_token.oldest_revision_found && pool.reader_token_pool[iter].last_read_revision) || 
	    (pool.owner_token.oldest_revision_found && pool.reader_token_pool[iter].last_read_revision &&
	    pool.reader_token_pool[iter].last_read_revision.get() < pool.owner_token.oldest_revision_found.get()))
	{
	    pool.owner_token.oldest_reader_id_found.reset(iter);
	    pool.owner_token.oldest_revision_found.reset(pool.reader_token_pool[iter].last_read_revision.get());
	    pool.owner_token.oldest_timestamp_found.reset(pool.reader_token_pool[iter].last_read_timestamp.get());
	}
    }
}

void mvcc_owner::process_write_metadata(std::size_t max_attempts)
{
    mvcc_deleter deleter;
    mvcc_resource_pool& pool = mut_resource_pool(container_);
    for (std::size_t attempts = 0; !pool.deleter_list.empty() && (max_attempts == 0 || attempts < max_attempts); ++attempts)
    {
	if (pool.deleter_list.pop(deleter))
	{
	    pool.owner_token.registry.insert(std::make_pair(deleter.key, deleter));
	}
    }
}

std::string mvcc_owner::collect_garbage(std::size_t max_attempts)
{
    std::string from;
    return collect_garbage(from, max_attempts);
}

std::string mvcc_owner::collect_garbage(const std::string& from, std::size_t max_attempts)
{
    mvcc_resource_pool& pool = mut_resource_pool(container_);
    if (pool.owner_token.registry.empty())
    {
	return "";
    }
    mvcc_key key(from.c_str());
    registry_map::const_iterator iter = from.empty() ?
	    pool.owner_token.registry.begin() :
	    pool.owner_token.registry.find(key);
    if (pool.owner_token.oldest_revision_found)
    {
	mvcc_revision oldest = pool.owner_token.oldest_revision_found.get();
	for (std::size_t attempts = 0; iter != pool.owner_token.registry.end() && (max_attempts == 0 || attempts < max_attempts); ++attempts, ++iter)
	{
	    iter->second.function(container_, iter->first.c_str, oldest);
	}
    }
    if (iter == pool.owner_token.registry.end())
    {
	// Next collect attempt should start again from beginning
	iter = pool.owner_token.registry.begin();
    }
    return iter->first.c_str;
}

void mvcc_owner::flush()
{
    boost::function<void ()> flush_func(boost::bind(&mvcc_owner::flush_impl, boost::ref(*this)));
    container_.memory.get_segment_manager()->atomic_func(flush_func);
}

void mvcc_owner::flush_impl()
{
    mut_resource_pool(container_).owner_token.last_flush_timestamp = bpt::microsec_clock::local_time();
    mut_resource_pool(container_).owner_token.last_flush_revision = const_resource_pool(container_).global_revision;
    container_.memory.flush();
}

} // namespace grid_db
} // namespace simulation_grid
