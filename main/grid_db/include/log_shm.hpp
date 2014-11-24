#ifndef SIMULATION_GRID_GRID_DB_LOG_SHM_HPP
#define SIMULATION_GRID_GRID_DB_LOG_SHM_HPP

#include <string>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/noncopyable.hpp>
#include "log_memory.hpp"

namespace simulation_grid {
namespace grid_db {

template <class entry_t>
class log_shm_reader : private boost::noncopyable
{
public:
    log_shm_reader(const std::string& name);
    ~log_shm_reader();
    inline boost::optional<const entry_t&> read(const log_index& index) const;
    inline boost::optional<log_index> get_front_index() const;
    inline boost::optional<log_index> get_back_index() const;
    inline log_index get_max_index() const;
private:
    const std::string name_;
    boost::interprocess::shared_memory_object shm_;
    boost::interprocess::mapped_region region_;
    log_reader_handle<entry_t> reader_handle_;
};

template <class entry_t>
class log_shm_owner : private boost::noncopyable
{
public:
    log_shm_owner(const std::string& name, std::size_t size);
    ~log_shm_owner();
    inline boost::optional<log_index> append(const entry_t& entry);
    inline boost::optional<const entry_t&> read(const log_index& index) const;
    inline boost::optional<log_index> get_front_index() const;
    inline boost::optional<log_index> get_back_index() const;
    inline log_index get_max_index() const;
private:
    static bool does_shm_exist(const std::string& name);
    bool exists_;
    const std::string name_;
    boost::interprocess::shared_memory_object shm_;
    boost::interprocess::mapped_region region_;
    log_owner_handle<entry_t> owner_handle_;
    log_reader_handle<entry_t> reader_handle_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
