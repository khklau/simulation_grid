#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include <cstring>
#include <boost/config.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"

namespace bi = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

template <class content_t>
bool mvcc_mmap_reader::exists(const char* id)
{
    //return container_.find<content_t>(id).first != 0;
    return true;
}

template <class content_t>
const content_t& mvcc_mmap_reader::read(const char* id)
{
    const content_t* ptr = container_.file.find<content_t>(id).first;
    if (BOOST_UNLIKELY(!ptr))
    {
	throw malformed_db_error("Could not find data")
		<< info_db_identity(container_.path.string())
		<< info_component_identity("mvcc_mmap_reader")
		<< info_data_identity(id);
    }
    return *ptr;
}

} // namespace grid_db
} // namespace simulation_grid

#endif
