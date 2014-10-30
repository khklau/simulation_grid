#ifndef SIMULATION_GRID_GRID_DB_LOG_MEMORY_HPP
#define SIMULATION_GRID_GRID_DB_LOG_MEMORY_HPP

#include <boost/noncopyable.hpp>
#include <simulation_grid/grid_db/about.hpp>

namespace simulation_grid {
namespace grid_db {

const version LOG_MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version LOG_MAX_SUPPORTED_VERSION(1, 1, 1, 1);

template <class memory_t>
class log_reader_handle : private boost::noncopyable
{
};

template <class memory_t>
class log_owner_handle : private boost::noncopyable
{
};

} // namespace grid_db
} // namespace simulation_grid

#endif
