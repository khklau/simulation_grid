#ifndef SIMULATION_GRID_GRID_DB_LOG_MMAP_HXX
#define SIMULATION_GRID_GRID_DB_LOG_MMAP_HXX

#include "log_mmap.hpp"
#include "log_memory.hxx"
#include <simulation_grid/grid_db/exception.hpp>

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

const bfs::path& init_file(const bfs::path& file, std::size_t size);

template <class entry_t>
log_mmap_owner<entry_t>::log_mmap_owner(const bfs::path& path, std::size_t size)
try :
    file_(init_file(path, size).string().c_str(), bip::read_write),
    region_(file_, bip::read_write, size, size)
{
}
catch (grid_db_condition& cond)
{
    cond << info_db_identity(path.string());
    throw cond;
}
catch (grid_db_error& err)
{
    err << info_db_identity(path.string());
    throw err;
}

template <class entry_t>
log_index log_mmap_owner<entry_t>::append(const entry_t& entry)
{
    return 0U;
}


} // namespace grid_db
} // namespace simulation_grid

#endif
