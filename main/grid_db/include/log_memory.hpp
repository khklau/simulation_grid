#ifndef SIMULATION_GRID_GRID_DB_LOG_MEMORY_HPP
#define SIMULATION_GRID_GRID_DB_LOG_MEMORY_HPP

#include <boost/cstdint.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/noncopyable.hpp>
#include <simulation_grid/grid_db/about.hpp>
#include "mode.hpp"

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t log_index;

const version LOG_MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version LOG_MAX_SUPPORTED_VERSION(1, 1, 1, 1);

template <class entry_t>
class log_reader_handle : private boost::noncopyable
{
public:
    log_reader_handle(const boost::interprocess::mapped_region& region);
    ~log_reader_handle();
private:
    const boost::interprocess::mapped_region& region_;
};

template <class entry_t>
class log_owner_handle : private boost::noncopyable
{
public:
    log_owner_handle(open_mode mode, boost::interprocess::mapped_region& region);
    ~log_owner_handle();
private:
    boost::interprocess::mapped_region& region_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
