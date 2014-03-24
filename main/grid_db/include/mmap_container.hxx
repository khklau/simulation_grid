#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include <cstring>
#include <boost/config.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"

namespace bi = boost::interprocess;
namespace bg = boost::gregorian;

namespace {

using namespace simulation_grid::grid_db;

typedef boost::uint64_t mvcc_revision;

static const char* RESOURCE_KEY = "@@RESOURCE@@";

struct mvcc_mmap_reader_token
{
    boost::optional<mvcc_revision> read_revision;
};

struct mvcc_mmap_writer_token
{
    boost::optional<bg::date> last_flush;
};

struct mvcc_mmap_resource
{
    mvcc_mmap_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_mmap_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
};

namespace mvcc_utility {

template <class content_t>
content_t* find(const mvcc_mmap_container& container, const bi::managed_mapped_file::char_type* key)
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_mmap_container&>(container).file.find<content_t>(key).first;
}

const mvcc_mmap_resource& get_resource(const mvcc_mmap_container& container)
{
    return *find<mvcc_mmap_resource>(container, RESOURCE_KEY);
}

mvcc_mmap_resource& get_mutable_resource(const mvcc_mmap_container& container)
{
    return *find<mvcc_mmap_resource>(container, RESOURCE_KEY);
}

} // namespace mvcc_utility

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

template <class content_t>
bool mvcc_mmap_reader::exists(const char* id) const
{
    return mvcc_utility::find<content_t>(container_, id).first != 0;
}

template <class content_t>
const content_t& mvcc_mmap_reader::read(const char* id) const
{
    const content_t* ptr = mvcc_utility::find<content_t>(container_, id).first;
    if (BOOST_UNLIKELY(!ptr))
    {
	throw malformed_db_error("Could not find data")
		<< info_db_identity(container_.path.string())
		<< info_component_identity("mvcc_mmap_reader")
		<< info_data_identity(id);
    }
    // the reader_token will ensure the returned reference remains valid
    return *ptr;
}

} // namespace grid_db
} // namespace simulation_grid

#endif
