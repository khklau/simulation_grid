#ifndef SIMULATION_GRID_GRID_DB_LOG_MMAP_HPP
#define SIMULATION_GRID_GRID_DB_LOG_MMAP_HPP

#include <boost/filesystem/path.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include "log_memory.hpp"

namespace simulation_grid {
namespace grid_db {

template <class entry_t>
class log_mmap_reader
{
public:
    log_mmap_reader(const boost::filesystem::path& path);
private:
    boost::interprocess::file_mapping file_;
    boost::interprocess::mapped_region region_;
    log_reader_handle<entry_t> reader_handle;
};

template <class entry_t>
class log_mmap_owner
{
public:
    log_mmap_owner(const boost::filesystem::path& path, std::size_t size);
    log_index append(const entry_t& entry);
private:
    bool exists_;
    boost::interprocess::file_lock flock_;
    boost::interprocess::scoped_lock<boost::interprocess::file_lock> slock_;
    boost::interprocess::file_mapping file_;
    boost::interprocess::mapped_region region_;
    log_owner_handle<entry_t> owner_handle;
    log_reader_handle<entry_t> reader_handle;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
