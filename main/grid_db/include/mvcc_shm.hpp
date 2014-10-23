#ifndef SIMULATION_GRID_GRID_DB_MVCC_SHM_HPP
#define SIMULATION_GRID_GRID_DB_MVCC_SHM_HPP

#include <string>
#include <limits>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <simulation_grid/grid_db/about.hpp>
#include "role.hpp"
#include "mvcc_memory.hpp"

namespace simulation_grid {
namespace grid_db {

class mvcc_shm_reader : private boost::noncopyable
{
public:
    mvcc_shm_reader(const std::string& name);
    ~mvcc_shm_reader();
    template <class element_t> bool exists(const char* key) const;
    template <class element_t> const boost::optional<const element_t&> read(const char* key) const;
    std::size_t get_available_space() const;
    std::size_t get_size() const;
#ifdef SIMGRID_GRIDDB_MVCCMEMORY_DEBUG
    reader_token_id get_reader_token_id() const;
    boost::uint64_t get_last_read_revision() const;
    template <class element_t> boost::uint64_t get_oldest_revision(const char* key) const;
    template <class element_t> boost::uint64_t get_newest_revision(const char* key) const;
#endif
private:
    const std::string name_;
    boost::interprocess::managed_shared_memory share_;
    mvcc_reader_handle<boost::interprocess::managed_shared_memory> reader_handle_;
};

#ifdef SIMGRID_GRIDDB_MVCCMEMORY_DEBUG
#include <vector>
#endif

class mvcc_shm_owner : private boost::noncopyable
{
public:
    mvcc_shm_owner(const std::string& name, std::size_t size);
    ~mvcc_shm_owner();
    template <class element_t> bool exists(const char* key) const;
    template <class element_t> const boost::optional<const element_t&> read(const char* key) const;
    template <class element_t> void write(const char* key, const element_t& value);
    template <class element_t> void remove(const char* key);
    void process_read_metadata(reader_token_id from = 0, reader_token_id to = MVCC_READER_LIMIT);
    void process_write_metadata(std::size_t max_attempts = 0);
    std::string collect_garbage(std::size_t max_attempts = 0);
    std::string collect_garbage(const std::string& from, std::size_t max_attempts = 0);
    std::size_t get_available_space() const;
    std::size_t get_size() const;
#ifdef SIMGRID_GRIDDB_MVCCMEMORY_DEBUG
    reader_token_id get_reader_token_id() const;
    boost::uint64_t get_last_read_revision() const;
    template <class element_t> boost::uint64_t get_oldest_revision(const char* key) const;
    template <class element_t> boost::uint64_t get_newest_revision(const char* key) const;
    boost::uint64_t get_global_oldest_revision_read() const;
    std::vector<std::string> get_registered_keys() const;
    template <class element_t> std::size_t get_history_depth(const char* key) const;
#endif
private:
    bool exists_;
    const std::string name_;
    boost::interprocess::managed_shared_memory share_;
    mvcc_owner_handle<boost::interprocess::managed_shared_memory> owner_handle_;
    mvcc_writer_handle<boost::interprocess::managed_shared_memory> writer_handle_;
    mvcc_reader_handle<boost::interprocess::managed_shared_memory> reader_handle_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
