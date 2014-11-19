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
    ~log_mmap_reader();
    inline boost::optional<const entry_t&> read(const log_index& index) const;
    inline boost::optional<log_index> get_front_index() const;
    inline boost::optional<log_index> get_back_index() const;
    inline log_index get_max_index() const;
private:
    boost::interprocess::file_mapping file_;
    boost::interprocess::mapped_region region_;
    log_reader_handle<entry_t> reader_handle_;
};

template <class entry_t>
class log_mmap_owner
{
public:
    log_mmap_owner(const boost::filesystem::path& path, std::size_t size);
    ~log_mmap_owner();
    inline boost::optional<log_index> append(const entry_t& entry);
    inline boost::optional<const entry_t&> read(const log_index& index) const;
    inline boost::optional<log_index> get_front_index() const;
    inline boost::optional<log_index> get_back_index() const;
    inline log_index get_max_index() const;
private:
    bool exists_;
    boost::interprocess::file_lock flock_;
    boost::interprocess::scoped_lock<boost::interprocess::file_lock> slock_;
    boost::interprocess::file_mapping file_;
    boost::interprocess::mapped_region region_;
    log_owner_handle<entry_t> owner_handle_;
    log_reader_handle<entry_t> reader_handle_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
