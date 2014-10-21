#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP

#include <string>
#include <limits>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/segment_manager.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <simulation_grid/grid_db/about.hpp>
#include "role.hpp"
#include "mvcc_container.hpp"

namespace simulation_grid {
namespace grid_db {

struct mvcc_mmap_container
{
    static const version MIN_SUPPORTED_VERSION;
    static const version MAX_SUPPORTED_VERSION;
    mvcc_mmap_container(const owner_t, const boost::filesystem::path& path, size_t size);
    mvcc_mmap_container(const reader_t, const boost::filesystem::path& path);
    std::size_t available_space() const;
    bool exists;
    const boost::filesystem::path path;
    boost::interprocess::managed_mapped_file file;
};

class mvcc_mmap_reader : private boost::noncopyable
{
public:
    mvcc_mmap_reader(const boost::filesystem::path& path);
    ~mvcc_mmap_reader();
    template <class element_t> bool exists(const char* key) const;
    template <class element_t> const boost::optional<const element_t&> read(const char* key) const;
#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
    reader_token_id get_reader_token_id() const;
    boost::uint64_t get_last_read_revision() const;
    template <class element_t> boost::uint64_t get_oldest_revision(const char* key) const;
    template <class element_t> boost::uint64_t get_newest_revision(const char* key) const;
#endif
private:
    mvcc_mmap_container container_;
    mvcc_reader_handle<boost::interprocess::managed_mapped_file> reader_handle_;
};

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
#include <vector>
#endif

class mvcc_mmap_owner : private boost::noncopyable
{
public:
    mvcc_mmap_owner(const boost::filesystem::path& path, std::size_t size);
    ~mvcc_mmap_owner();
    template <class element_t> bool exists(const char* key) const;
    template <class element_t> const boost::optional<const element_t&> read(const char* key) const;
    template <class element_t> void write(const char* key, const element_t& value);
    template <class element_t> void remove(const char* key);
    void process_read_metadata(reader_token_id from = 0, reader_token_id to = MVCC_READER_LIMIT);
    void process_write_metadata(std::size_t max_attempts = 0);
    std::string collect_garbage(std::size_t max_attempts = 0);
    std::string collect_garbage(const std::string& from, std::size_t max_attempts = 0);
    void flush();
#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
    reader_token_id get_reader_token_id() const;
    boost::uint64_t get_last_read_revision() const;
    template <class element_t> boost::uint64_t get_oldest_revision(const char* key) const;
    template <class element_t> boost::uint64_t get_newest_revision(const char* key) const;
    boost::uint64_t get_global_oldest_revision_read() const;
    std::vector<std::string> get_registered_keys() const;
    template <class element_t> std::size_t get_history_depth(const char* key) const;
#endif
private:
    void flush_impl();
    bool exists_;
    mvcc_mmap_container container_;
    mvcc_owner_handle<boost::interprocess::managed_mapped_file> owner_handle_;
    mvcc_writer_handle<boost::interprocess::managed_mapped_file> writer_handle_;
    mvcc_reader_handle<boost::interprocess::managed_mapped_file> reader_handle_;
    boost::interprocess::file_lock file_lock_;
    boost::optional<boost::posix_time::ptime> last_flush_timestamp_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
