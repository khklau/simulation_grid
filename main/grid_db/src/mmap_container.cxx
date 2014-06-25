#include <cstring>
#include <boost/bind.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/thread_time.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"
#include "mmap_container.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bra = boost::random;

namespace {

using namespace simulation_grid::grid_db;

static const char* HEADER_KEY = "@@HEADER@@";
static const char* RESOURCE_POOL_KEY = "@@RESOURCE_POOL@@";
static const char* MVCC_MMAP_FILE_TYPE_TAG = "simulation_grid::grid_db::mvcc_mmap_container";

mvcc_mmap_header::mvcc_mmap_header() :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    container_version(mvcc_mmap_container::MAX_SUPPORTED_VERSION), 
    header_size(sizeof(mvcc_mmap_header))
{
    strncpy(file_type_tag, MVCC_MMAP_FILE_TYPE_TAG, sizeof(file_type_tag));
}

mvcc_mmap_resource_pool::mvcc_mmap_resource_pool(bip::managed_mapped_file* file) :
    reader_allocator(file->get_segment_manager()),
    writer_allocator(file->get_segment_manager()),
    reader_free_list(file->construct<reader_token_list>(bi::anonymous_instance)(reader_allocator)),
    writer_free_list(file->construct<writer_token_list>(bi::anonymous_instance)(writer_allocator))
{
    for (reader_token_id id = 0; id < MVCC_READER_LIMIT; ++id)
    {
	reader_free_list->push(id);
    }
    for (writer_token_id id = 0; id < MVCC_WRITER_LIMIT; ++id)
    {
	writer_free_list->push(id);
    }
}

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

size_t get_size(const mvcc_mmap_container& container)
{
    return container.file.get_size();
}

void flush(mvcc_mmap_container& container)
{
    container.file.flush();
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
    if (UNLIKELY_EXT(sizeof(mvcc_mmap_header) == header->header_size))
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
    if (!mut_resource_pool(container).reader_free_list->pop(reservation))
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
    while (!mut_resource_pool(container).reader_free_list->push(id))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

writer_token_id acquire_writer_token(mvcc_mmap_container& container)
{
    writer_token_id reservation;
    if (!mut_resource_pool(container).writer_free_list->pop(reservation))
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
    while (!mut_resource_pool(container).writer_free_list->push(id))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

const version mvcc_mmap_container::MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version mvcc_mmap_container::MAX_SUPPORTED_VERSION(1, 1, 1, 1);

mvcc_mmap_container::mvcc_mmap_container(const owner_t, const bfs::path& path, size_t size) :
    exists(bfs::exists(path)), path(path), file(bip::open_or_create, path.string().c_str(), size)
{
    if (exists)
    {
	check(*this);
    }
    else
    {
	boost::function<void ()> init_func(boost::bind(&init, boost::ref(*this)));
	file.get_segment_manager()->atomic_func(init_func);
    }
}

mvcc_mmap_container::mvcc_mmap_container(const reader_t, const bfs::path& path) :
    exists(bfs::exists(path)), path(path), file(bip::open_only, path.string().c_str())
{
    if (exists)
    {
	check(*this);
    }
}

std::size_t mvcc_mmap_container::available_space() const
{
    return file.get_free_memory();
}

mvcc_mmap_reader_handle::mvcc_mmap_reader_handle(mvcc_mmap_container& container) :
    container_(container), id_(acquire_reader_token(container_))
{ }

mvcc_mmap_reader_handle::~mvcc_mmap_reader_handle()
{
    try
    {
	release_reader_token(container_, id_);
    }
    catch(...)
    {
	// do nothing
    }
}

mvcc_mmap_writer_handle::mvcc_mmap_writer_handle(mvcc_mmap_container& container) :
    container_(container), id_(acquire_writer_token(container_))
{ }

mvcc_mmap_writer_handle::~mvcc_mmap_writer_handle()
{
    try
    {
	release_writer_token(container_, id_);
    }
    catch(...)
    {
	// do nothing
    }
}

mvcc_mmap_reader::mvcc_mmap_reader(const bfs::path& path) :
    container_(reader, path), reader_handle_(container_)
{ }

mvcc_mmap_reader::~mvcc_mmap_reader()
{ }

mvcc_mmap_owner::mvcc_mmap_owner(const bfs::path& path, std::size_t size) :
    container_(owner, path, size),
    writer_handle_(container_),
    reader_handle_(container_)
{ }

mvcc_mmap_owner::~mvcc_mmap_owner()
{ }

} // namespace grid_db
} // namespace simulation_grid
