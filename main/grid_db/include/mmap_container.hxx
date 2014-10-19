#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include <cstring>
#include <utility>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/optional.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/ref.hpp>
#include <boost/thread/thread_time.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"
#include "mvcc_container.hxx"
#include "mmap_container.hpp"

namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;
namespace bra = boost::random;

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t mvcc_revision;
typedef boost::uint8_t history_depth;
typedef boost::function<void(mvcc_mmap_container&, const char*, mvcc_revision)> delete_function;

struct mmap_key
{
    mmap_key();
    mmap_key(const char* key);
    mmap_key(const mmap_key& other);
    ~mmap_key();
    mmap_key& operator=(const mmap_key& other);
    bool operator<(const mmap_key& other) const;
    char c_str[MVCC_MAX_KEY_LENGTH + 1];
};

struct mmap_deleter
{
    mmap_deleter();
    mmap_deleter(const mmap_key& k, const delete_function& fn);
    mmap_deleter(const mmap_deleter& other);
    ~mmap_deleter();
    mmap_deleter& operator=(const mmap_deleter& other);
    mmap_key key;
    delete_function function;
};

} // namespace grid_db
} // namespace simulation_grid

namespace boost {

namespace sgd = simulation_grid::grid_db;

template <>
struct has_trivial_destructor<sgd::mmap_deleter>
{
    static const bool value = true;
};

template <>
struct has_trivial_assign<sgd::mmap_deleter>
{
    static const bool value = true;
};

} // namespace boost

namespace simulation_grid {
namespace grid_db {

// TODO: replace the following with type aliases after moving to a C++11 compiler

template <class content_t>
struct mmap_allocator
{
    typedef boost::interprocess::allocator<content_t, 
	    boost::interprocess::managed_mapped_file::segment_manager> type;
};

template <class key_t, class value_t>
struct mmap_map
{
    typedef typename mmap_allocator< std::pair<const key_t, value_t> >::type allocator_t;
    typedef boost::interprocess::map<key_t, value_t, std::less<key_t>, allocator_t> type;
};

template <class content_t, size_t size>
struct mmap_queue 
{
    typedef typename boost::lockfree::capacity<size> capacity;
    typedef typename boost::lockfree::fixed_sized<true> fixed;
    typedef typename boost::lockfree::allocator<typename mmap_allocator<content_t>::type> allocator_t;
    typedef boost::lockfree::queue<content_t, capacity, fixed, allocator_t> type;
};

struct mvcc_mmap_header
{
    boost::uint16_t endianess_indicator;
    char file_type_tag[48];
    version container_version;
    boost::uint16_t header_size;
    mvcc_mmap_header();
};

#ifdef LEVEL1_DCACHE_LINESIZE

struct mvcc_mmap_reader_token
{
    boost::optional<mvcc_revision> last_read_revision;
    boost::optional<bpt::ptime> last_read_timestamp;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

struct mvcc_mmap_writer_token
{
    boost::optional<mvcc_revision> last_write_revision;
    boost::optional<bpt::ptime> last_write_timestamp;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

#endif

typedef mmap_map<mmap_key, mmap_deleter>::type registry_map;

struct mvcc_mmap_owner_token
{
    mvcc_mmap_owner_token(bip::managed_mapped_file* file);
    boost::optional<mvcc_revision> last_flush_revision;
    boost::optional<bpt::ptime> last_flush_timestamp;
    boost::optional<reader_token_id> oldest_reader_id_found;
    boost::optional<mvcc_revision> oldest_revision_found;
    boost::optional<bpt::ptime> oldest_timestamp_found;
    registry_map registry;
};

struct mvcc_mmap_resource_pool
{
    mvcc_mmap_resource_pool(bip::managed_mapped_file* file);
    mvcc_mmap_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_mmap_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
    boost::atomic<mvcc_revision> global_revision;
    mvcc_mmap_owner_token owner_token;
    mmap_queue<reader_token_id, MVCC_READER_LIMIT>::type reader_free_list;
    mmap_queue<writer_token_id, MVCC_WRITER_LIMIT>::type writer_free_list;
    mmap_queue<mmap_deleter, DEFAULT_HISTORY_DEPTH>::type deleter_list;
};

const mvcc_mmap_header& const_header(const mvcc_mmap_container& container);
mvcc_mmap_header& mut_header(mvcc_mmap_container& container);
const mvcc_mmap_resource_pool& const_resource_pool(const mvcc_mmap_container& container);
mvcc_mmap_resource_pool& mut_resource_pool(const mvcc_mmap_container& container);

} // namespace grid_db
} // namespace simulation_grid

namespace {

using namespace simulation_grid::grid_db;

template <class value_t>
const value_t* find_const(const mvcc_mmap_container& container, const bip::managed_mapped_file::char_type* key)
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_mmap_container&>(container).file.find<value_t>(key).first;
}

template <class value_t>
value_t* find_mut(mvcc_mmap_container& container, const bip::managed_mapped_file::char_type* key)
{
    return container.file.find<value_t>(key).first;
}

template <class element_t>
void delete_oldest(mvcc_mmap_container& container, const char* key, mvcc_revision threshold)
{
    mvcc_record<element_t>* record = find_mut< mvcc_record<element_t> >(container, key);
    if (record)
    {
	const mvcc_value<element_t>& value = record->ringbuf.back();
	if ((record->ringbuf.element_count() > 1 && value.revision < threshold) || record->want_removed)
	{
	    record->ringbuf.pop_back(value);
	}
    }
}

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

const mvcc_mmap_header& const_header(const mvcc_mmap_container& container);
mvcc_mmap_header& mut_header(mvcc_mmap_container& container);
const mvcc_mmap_resource_pool& const_resource_pool(const mvcc_mmap_container& container);
mvcc_mmap_resource_pool& mut_resource_pool(const mvcc_mmap_container& container);

template <class element_t>
bool mvcc_mmap_reader::exists(const char* key) const
{
    return reader_handle_.template exists<element_t>(key);
}

template <class element_t>
const boost::optional<const element_t&> mvcc_mmap_reader::read(const char* key) const
{
    return reader_handle_.template read<element_t>(key);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_mmap_reader::get_reader_token_id() const
{
    return reader_handle_.get_reader_token_id();
}

boost::uint64_t mvcc_mmap_reader::get_last_read_revision() const
{
    return reader_handle_.get_last_read_revision();
}

template <class element_t>
boost::uint64_t mvcc_mmap_reader::get_oldest_revision(const char* key) const
{
    return reader_handle_.template get_oldest_revision<element_t>(key);
}

template <class element_t>
boost::uint64_t mvcc_mmap_reader::get_newest_revision(const char* key) const
{
    return reader_handle_.template get_newest_revision<element_t>(key);
}

#endif

template <class element_t>
bool mvcc_mmap_owner::exists(const char* key) const
{
    return reader_handle_.template exists<element_t>(key);
}

template <class element_t>
const boost::optional<const element_t&> mvcc_mmap_owner::read(const char* key) const
{
    return reader_handle_.template read<element_t>(key);
}

template <class element_t>
void mvcc_mmap_owner::write(const char* key, const element_t& value)
{
    writer_handle_.template write(key, value);
}

template <class element_t>
void mvcc_mmap_owner::remove(const char* key)
{
    writer_handle_.template remove<element_t>(key);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_mmap_owner::get_reader_token_id() const
{
    return reader_handle_.get_reader_token_id();
}

boost::uint64_t mvcc_mmap_owner::get_last_read_revision() const
{
    return reader_handle_.get_last_read_revision();
}

template <class element_t>
boost::uint64_t mvcc_mmap_owner::get_oldest_revision(const char* key) const
{
    return reader_handle_.template get_oldest_revision<element_t>(key);
}

template <class element_t>
boost::uint64_t mvcc_mmap_owner::get_newest_revision(const char* key) const
{
    return reader_handle_.template get_newest_revision<element_t>(key);
}

boost::uint64_t mvcc_mmap_owner::get_global_oldest_revision_read() const
{
    return owner_handle_.get_global_oldest_revision_read();
}

std::vector<std::string> mvcc_mmap_owner::get_registered_keys() const
{
    return owner_handle_.get_registered_keys();
}

template <class element_t> 
std::size_t mvcc_mmap_owner::get_history_depth(const char* key) const
{
    return reader_handle_.get_history_depth<element_t>(key);
}

#endif

} // namespace grid_db
} // namespace simulation_grid

#endif
