#ifndef SIMULATION_GRID_GRID_DB_LOG_MMAP_HPP
#define SIMULATION_GRID_GRID_DB_LOG_MMAP_HPP

#include <boost/cstdint.hpp>
#include "log_memory.hpp"

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t log_index;

template <class entry_t>
class log_mmap_reader
{
};

template <class entry_t>
class log_mmap_owner
{
public:
    log_mmap_owner(const boost::filesystem::path& path, std::size_t size);
    log_index append(const entry_t& entry);
};

} // namespace grid_db
} // namespace simulation_grid

#endif
