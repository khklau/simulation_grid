#ifndef SIMULATION_GRID_GRID_DB_MVCC_CONTAINER_HPP
#define SIMULATION_GRID_GRID_DB_MVCC_CONTAINER_HPP

#include <string>
#include <limits>
#include <boost/cstdint.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/segment_manager.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <simulation_grid/grid_db/about.hpp>
#include "role.hpp"

namespace simulation_grid {
namespace grid_db {

typedef boost::uint16_t reader_token_id;
typedef boost::uint16_t writer_token_id;

static const size_t MVCC_READER_LIMIT = (1 << std::numeric_limits<reader_token_id>::digits) - 4; // due to boost::lockfree limit
static const size_t MVCC_WRITER_LIMIT = 1;
static const size_t MVCC_MAX_KEY_LENGTH = 31;
const version MVCC_MIN_SUPPORTED_VERSION(1, 1, 1, 1);
const version MVCC_MAX_SUPPORTED_VERSION(1, 1, 1, 1);

template <class memory_t>
class mvcc_reader_handle : private boost::noncopyable
{
public:
    mvcc_reader_handle(memory_t& memory);
    ~mvcc_reader_handle();
    void check();
    template <class value_t> bool exists(const char* key) const;
    template <class value_t> const boost::optional<const value_t&> read(const char* key) const;
#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
    reader_token_id get_reader_token_id() const;
    boost::uint64_t get_last_read_revision() const;
    template <class value_t> boost::uint64_t get_oldest_revision(const char* key) const;
    template <class value_t> boost::uint64_t get_newest_revision(const char* key) const;
    template <class value_t> std::size_t get_history_depth(const char* key) const;
#endif
private:
    static reader_token_id acquire_reader_token(memory_t& memory);
    static void release_reader_token(memory_t& memory, const reader_token_id& id);
    template <class value_t> const value_t* find_const(const char* key) const;
    memory_t& memory;
    const reader_token_id token_id;
};

template <class memory_t>
class mvcc_writer_handle : private boost::noncopyable
{
public:
    mvcc_writer_handle(memory_t& memory);
    ~mvcc_writer_handle();
    void check();
    template <class value_t> void write(const char* key, const value_t& value);
    template <class value_t> void remove(const char* key);
#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
    writer_token_id get_writer_token_id() const;
    boost::uint64_t get_last_write_revision() const;
#endif
private:
    static writer_token_id acquire_writer_token(memory_t& memory);
    static void release_writer_token(memory_t& memory, const writer_token_id& id);
    template <class value_t> const value_t* find_const(const char* key) const;
    template <class value_t> value_t* find_mut(const char* key);
    memory_t& memory;
    const writer_token_id token_id;
};

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
#include <vector>
#endif

enum open_mode
{
    open_existing,
    open_new
};

template <class memory_t>
class mvcc_owner_handle : private boost::noncopyable
{
public:
    mvcc_owner_handle(open_mode mode, memory_t& memory);
    ~mvcc_owner_handle();
    void process_read_metadata(reader_token_id from = 0, reader_token_id to = MVCC_READER_LIMIT);
    void process_write_metadata(std::size_t max_attempts = 0);
    std::string collect_garbage(std::size_t max_attempts = 0);
    std::string collect_garbage(const std::string& from, std::size_t max_attempts = 0);
#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG
    boost::uint64_t get_global_oldest_revision_read() const;
    std::vector<std::string> get_registered_keys() const;
#endif
private:
    void check();
    void init();
    template <class value_t> const value_t* find_const(const char* key) const;
    template <class value_t> value_t* find_mut(const char* key);
    memory_t& memory;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
