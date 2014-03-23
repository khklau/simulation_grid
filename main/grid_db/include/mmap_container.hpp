#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP

#include <utility>
#include <limits>
#include <boost/cstdint.hpp>
#include <boost/noncopyable.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/interprocess/segment_manager.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/containers/stable_vector.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <simulation_grid/grid_db/about.hpp>
#include "role.hpp"

namespace simulation_grid {
namespace grid_db {

typedef boost::uint16_t reader_token_id;
typedef boost::uint16_t writer_token_id;

static const size_t MVCC_READER_LIMIT = (1 << std::numeric_limits<reader_token_id>::digits) - 4; // due to boost::lockfree limit
static const size_t MVCC_WRITER_LIMIT = 1;

// TODO: replace the following with type aliases after moving to a C++11 compiler

template <class content_t>
struct mmap_allocator
{
    typedef boost::interprocess::allocator<content_t, 
	    boost::interprocess::managed_mapped_file::segment_manager> type;
};

template <class content_t>
struct mmap_vector 
{
    typedef typename mmap_allocator<content_t>::type allocator_t;
    typedef boost::interprocess::stable_vector<content_t, allocator_t> type;
};

template <class key_t, class value_t>
struct mmap_map
{
    typedef std::pair<key_t, value_t> content_t;
    typedef typename mmap_allocator<content_t>::type allocator_t;
    typedef boost::interprocess::map<content_t, allocator_t> type;
};

template <class content_t, size_t size>
struct mmap_queue 
{
    typedef typename boost::lockfree::capacity<size> capacity;
    typedef typename boost::lockfree::fixed_sized<true> fixed;
    typedef typename boost::lockfree::allocator<typename mmap_allocator<content_t>::type> allocator_t;
    typedef boost::lockfree::queue<content_t, capacity, fixed, allocator_t> type;
};

struct mvcc_mmap_container
{
    static const version MIN_SUPPORTED_VERSION;
    static const version MAX_SUPPORTED_VERSION;
    mvcc_mmap_container(const owner_t, const boost::filesystem::path& path, size_t size);
    mvcc_mmap_container(const reader_t, const boost::filesystem::path& path);
    bool exists;
    boost::interprocess::managed_mapped_file file;
    const boost::filesystem::path path;
};

class mvcc_mmap_reader : private boost::noncopyable
{
public:
    mvcc_mmap_reader(const boost::filesystem::path& path);
    ~mvcc_mmap_reader();
    template <class content_t> bool exists(const char* id);
    template <class content_t> const content_t& read(const char* id);
private:
    mvcc_mmap_container container_;
    const reader_token_id token_id_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
