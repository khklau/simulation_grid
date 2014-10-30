#ifndef SIMULATION_GRID_GRID_DB_LOG_MMAP_HXX
#define SIMULATION_GRID_GRID_DB_LOG_MMAP_HXX

#include "log_mmap.hpp"
#include "log_memory.hxx"

namespace bfs = boost::filesystem;

namespace simulation_grid {
namespace grid_db {

template <class entry_t>
log_mmap_owner<entry_t>::log_mmap_owner(const bfs::path& path, std::size_t size)
{
}

template <class entry_t>
log_index log_mmap_owner<entry_t>::append(const entry_t& entry)
{
    return 0U;
}


} // namespace grid_db
} // namespace simulation_grid

#endif
