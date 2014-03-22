#include <cstring>
#include <boost/bind.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/crc.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/thread_time.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;
namespace bl = boost::lockfree;
namespace bg = boost::gregorian;
namespace br = boost::random;

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t mvcc_revision;
typedef boost::uint8_t history_depth;

static const char* HEADER_KEY = "@@HEADER@@";
static const char* METADATA_KEY = "@@METADATA@@";
static const char* RESOURCE_KEY = "@@RESOURCE@@";
static const char* READER_FREE_LIST_KEY = "@@READER_FREE_LIST@@";
static const char* WRITER_FREE_LIST_KEY = "@@WRITER_FREE_LIST@@";
static const char* MVCC_MMAP_FILE_TYPE_TAG = "simulation_grid::grid_db::mvcc_mmap_container";
static const size_t HISTORY_DEPTH_SIZE = 1 <<  std::numeric_limits<history_depth>::digits;

namespace {

struct mvcc_mmap_reader_token
{
    boost::optional<mvcc_revision> read_revision;
};

struct mvcc_mmap_writer_token
{
    boost::optional<bg::date> last_flush;
};

} // anonymous namespace

struct mvcc_mmap_resource
{
    mvcc_mmap_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_mmap_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
};

struct mvcc_mmap_metadata
{
    boost::uint32_t resource_size;
    boost::optional<boost::uint64_t> data_size;
    boost::optional<mvcc_revision> cached_revision;
    boost::optional<mvcc_revision> flushed_revision;
    mvcc_mmap_metadata() :
	resource_size(sizeof(mvcc_mmap_resource)), 
	data_size(boost::optional<boost::uint64_t>()),
	cached_revision(boost::optional<mvcc_revision>()),
	flushed_revision(boost::optional<mvcc_revision>())
    { }
};

struct mvcc_mmap_header
{
    boost::uint16_t endianess_indicator;
    char file_type_tag[48];
    version container_version;
    boost::uint16_t header_size;
    boost::uint16_t metadata_size;
    boost::optional<boost::uint32_t> metadata_crc_checksum;
    mvcc_mmap_header() :
	endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
	container_version(mvcc_mmap_container::MAX_SUPPORTED_VERSION), 
	header_size(sizeof(mvcc_mmap_header)), metadata_size(sizeof(mvcc_mmap_metadata)),
	metadata_crc_checksum(boost::optional<boost::uint32_t>())
    {
	strncpy(file_type_tag, MVCC_MMAP_FILE_TYPE_TAG, sizeof(file_type_tag));
    }
};

const version mvcc_mmap_container::MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version mvcc_mmap_container::MAX_SUPPORTED_VERSION(1, 1, 1, 1);

mvcc_mmap_container::mvcc_mmap_container(const owner_t role, const bf::path& path, size_t size) :
    exists_(bf::exists(path)), role_(role), file_(bi::open_or_create, path.string().c_str(), size), path_(path)
{
    if (exists_)
    {
	check();
    }
    else
    {
	boost::function<void ()> init_func(boost::bind(&mvcc_mmap_container::init, this));
	file_.get_segment_manager()->atomic_func(init_func);
    }
}

mvcc_mmap_container::mvcc_mmap_container(const reader_t role, const bf::path& path) :
    exists_(bf::exists(path)), role_(role), file_(bi::open_only, path.string().c_str()), path_(path)
{
    if (exists_)
    {
	check();
    }
}

void mvcc_mmap_container::init()
{
    mvcc_mmap_header* header = file_.construct<mvcc_mmap_header>(HEADER_KEY)();
    mvcc_mmap_metadata* metadata = file_.construct<mvcc_mmap_metadata>(METADATA_KEY)();
    file_.construct<mvcc_mmap_resource>(RESOURCE_KEY)();
    mvcc_mmap_reader_token_list* reader_list = file_.construct<mvcc_mmap_reader_token_list>(READER_FREE_LIST_KEY)(file_.get_segment_manager());
    for (reader_token_id id = 0; id < MVCC_READER_LIMIT; ++id)
    {
	reader_list->push(id);
    }
    mvcc_mmap_writer_token_list* writer_list = file_.construct<mvcc_mmap_writer_token_list>(WRITER_FREE_LIST_KEY)(file_.get_segment_manager());
    for (writer_token_id id = 0; id < MVCC_WRITER_LIMIT; ++id)
    {
	writer_list->push(id);
    }
    metadata->cached_revision = 0;
    metadata->flushed_revision = 0;
    boost::crc_32_type result;
    result.process_bytes(metadata, sizeof(*metadata));
    header->metadata_crc_checksum = result.checksum();
    file_.flush();
}

void mvcc_mmap_container::check() const
{
    const mvcc_mmap_header* header = get_header();
    if (!header)
    {
	throw malformed_db_error("Could not find header")
		<< info_db_identity(path_.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
    // If endianess is different the indicator will be 65280 instead of 255
    if (header->endianess_indicator != std::numeric_limits<boost::uint8_t>::max())
    {
	throw unsupported_db_error("Container requires byte swapping")
		<< info_db_identity(path_.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_version_found(header->container_version)
		<< info_min_supported_version(MIN_SUPPORTED_VERSION)
		<< info_max_supported_version(MAX_SUPPORTED_VERSION);
    }
    if (strncmp(header->file_type_tag, MVCC_MMAP_FILE_TYPE_TAG, sizeof(header->file_type_tag)))
    {
	throw malformed_db_error("Incorrect file type tag found")
		<< info_db_identity(path_.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
    if (header->container_version < MIN_SUPPORTED_VERSION ||
	    header->container_version > MAX_SUPPORTED_VERSION)
    {
	throw unsupported_db_error("Unsuported container version")
		<< info_db_identity(path_.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_version_found(header->container_version)
		<< info_min_supported_version(MIN_SUPPORTED_VERSION)
		<< info_max_supported_version(MAX_SUPPORTED_VERSION);
    }
    if (sizeof(mvcc_mmap_header) == header->header_size)
    {
	throw malformed_db_error("Wrong header size")
		<< info_db_identity(path_.string())
		<< info_component_identity("mvcc_mmap_container")
		<< info_data_identity(HEADER_KEY);
    }
}

const mvcc_mmap_header* mvcc_mmap_container::get_header() const
{
    return file_.find<mvcc_mmap_header>(HEADER_KEY).first;
}

mvcc_mmap_header* mvcc_mmap_container::get_mutable_header()
{
    return file_.find<mvcc_mmap_header>(HEADER_KEY).first;
}

const mvcc_mmap_metadata* mvcc_mmap_container::get_metadata() const
{
    return file_.find<mvcc_mmap_metadata>(METADATA_KEY).first;
}

mvcc_mmap_metadata* mvcc_mmap_container::get_mutable_metadata()
{
    return file_.find<mvcc_mmap_metadata>(METADATA_KEY).first;
}

const mvcc_mmap_resource* mvcc_mmap_container::get_resource() const
{
    return file_.find<mvcc_mmap_resource>(RESOURCE_KEY).first;
}

mvcc_mmap_resource* mvcc_mmap_container::get_mutable_resource()
{
    return file_.find<mvcc_mmap_resource>(RESOURCE_KEY).first;
}

const mvcc_mmap_reader_token_list* mvcc_mmap_container::get_reader_free_list() const
{
    return file_.find<mvcc_mmap_reader_token_list>(READER_FREE_LIST_KEY).first;
}

mvcc_mmap_reader_token_list* mvcc_mmap_container::get_mutable_reader_free_list()
{
    return file_.find<mvcc_mmap_reader_token_list>(READER_FREE_LIST_KEY).first;
}

const mvcc_mmap_writer_token_list* mvcc_mmap_container::get_writer_free_list() const
{
    return file_.find<mvcc_mmap_writer_token_list>(WRITER_FREE_LIST_KEY).first;
}

mvcc_mmap_writer_token_list* mvcc_mmap_container::get_mutable_writer_free_list()
{
    return file_.find<mvcc_mmap_writer_token_list>(WRITER_FREE_LIST_KEY).first;
}

const bf::path& mvcc_mmap_container::get_path() const
{
    return path_;
}

size_t mvcc_mmap_container::get_size() const
{
    return file_.get_size();
}

reader_token_id mvcc_mmap_container::acquire_reader_token()
{
    reader_token_id reservation;
    if (!get_mutable_reader_free_list()->pop(reservation))
    {
	throw busy_condition("No reader token available")
		<< info_db_identity(get_path().string())
		<< info_component_identity("mvcc_mmap_container");
    }
    return reservation;
}

void mvcc_mmap_container::release_reader_token(const reader_token_id& id)
{
    br::mt19937 seed;
    br::uniform_int_distribution<> generator(100, 200);
    while (!get_mutable_reader_free_list()->push(id))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

mvcc_mmap_reader::mvcc_mmap_reader(const bf::path& path) :
    container_(reader, path), token_id_(container_.acquire_reader_token())
{ }

mvcc_mmap_reader::~mvcc_mmap_reader()
{
    container_.release_reader_token(token_id_);
}

} // namespace grid_db
} // namespace simulation_grid
