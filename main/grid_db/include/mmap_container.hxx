#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include <cstring>
#include <boost/config.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <simulation_grid/grid_db/mmap_container.hpp>
#include <simulation_grid/grid_db/exception.hpp>

namespace bi = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

template <class content_t>
bool mmap_container_reader::exists(const char* id)
{
    return mmap_.find<content_t>(id).first != 0;
}

template <class content_t>
const content_t& mmap_container_reader::read(const char* id)
{
    const content_t* ptr = mmap_.find<content_t>(id).first;
    if (BOOST_UNLIKELY(!ptr))
    {
	throw content_error("Could not find content")
		<< info_content_identity(id)
		<< info_container_identity(path_.string());
    }
    return *ptr;
}

} // namespace grid_db
} // namespace simulation_grid

#endif
