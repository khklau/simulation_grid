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
#include "mmap_container.hpp"

namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;
namespace bra = boost::random;

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t mvcc_revision;
typedef boost::uint8_t history_depth;
typedef boost::function<void(mvcc_mmap_container&, const char*, mvcc_revision)> delete_function;
static const size_t DEFAULT_HISTORY_DEPTH = 1 <<  std::numeric_limits<history_depth>::digits;

struct mvcc_key
{
    mvcc_key();
    mvcc_key(const char* key);
    mvcc_key(const mvcc_key& other);
    ~mvcc_key();
    mvcc_key& operator=(const mvcc_key& other);
    bool operator<(const mvcc_key& other) const;
    char c_str[MVCC_MAX_KEY_LENGTH + 1];
};

struct mvcc_deleter
{
    mvcc_deleter();
    mvcc_deleter(const mvcc_key& k, const delete_function& fn);
    mvcc_deleter(const mvcc_deleter& other);
    ~mvcc_deleter();
    mvcc_deleter& operator=(const mvcc_deleter& other);
    mvcc_key key;
    delete_function function;
};

} // namespace grid_db
} // namespace simulation_grid

namespace boost {

namespace sgd = simulation_grid::grid_db;

template <>
struct has_trivial_destructor<sgd::mvcc_deleter>
{
    static const bool value = true;
};

template <>
struct has_trivial_assign<sgd::mvcc_deleter>
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

typedef mmap_map<mvcc_key, mvcc_deleter>::type registry_map;

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
    mmap_queue<mvcc_deleter, DEFAULT_HISTORY_DEPTH>::type deleter_list;
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
struct mvcc_record
{
    mvcc_record(const value_t& v, const mvcc_revision& r, const bpt::ptime& t) :
	    value(v), revision(r), timestamp(t)
    { }
    value_t value;
    mvcc_revision revision;
    bpt::ptime timestamp;
};

template <class content_t>
struct mmap_ring_buffer
{
    typedef multi_reader_ring_buffer<content_t, bip::allocator<content_t, bip::managed_mapped_file::segment_manager> > type;
};

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
bool exists_(const mvcc_mmap_reader_handle& handle, const char* key)
{
    typedef typename mmap_ring_buffer< mvcc_record<element_t> >::type recringbuf_t;
    const recringbuf_t* ringbuf = find_const<recringbuf_t>(handle.container, key);
    return ringbuf && !ringbuf->empty();
}

template <class element_t>
const mvcc_record<element_t>& read_(const mvcc_mmap_reader_handle& handle, const char* key)
{
    typedef typename mmap_ring_buffer< mvcc_record<element_t> >::type recringbuf_t;
    const recringbuf_t* ringbuf = find_const<recringbuf_t>(handle.container, key);
    if (UNLIKELY_EXT(!ringbuf || ringbuf->empty()))
    {
	throw malformed_db_error("Could not find data")
		<< info_db_identity(handle.container.path.string())
		<< info_component_identity("mvcc_mmap_reader")
		<< info_data_identity(key);
    }
    const mvcc_record<element_t>& record = ringbuf->front();
    // the mvcc_mmap_reader_token will ensure the returned reference remains valid
    mut_resource_pool(handle.container).reader_token_pool[handle.token_id].
	    last_read_timestamp.reset(record.timestamp);
    mut_resource_pool(handle.container).reader_token_pool[handle.token_id].
	    last_read_revision.reset(record.revision);
    return record;
}

template <class element_t>
void delete_oldest(mvcc_mmap_container& container, const char* key, mvcc_revision threshold)
{
    typedef mvcc_record<element_t> record_t;
    typedef typename mmap_ring_buffer<record_t>::type recringbuf_t;
    recringbuf_t* ringbuf = find_mut<recringbuf_t>(container, key);
    if (ringbuf)
    {
	const mvcc_record<element_t>& record = ringbuf->back();
	if (record.revision < threshold)
	{
	    ringbuf->pop_back(record);
	}
    }
}

// TODO: provide strong exception guarantee
template <class element_t>
void write_(mvcc_mmap_writer_handle& handle, const char* key, const element_t& value)
{
    typedef mvcc_record<element_t> record_t;
    typedef typename mmap_ring_buffer<record_t>::type recringbuf_t;
    mvcc_key mkey(key);
    if (!find_const<recringbuf_t>(handle.container, mkey.c_str))
    {
	bra::mt19937 seed;
	bra::uniform_int_distribution<> generator(100, 200);
	mvcc_deleter deleter(mkey, &delete_oldest<element_t>);
	while (UNLIKELY_EXT(!mut_resource_pool(handle.container).deleter_list.push(deleter)))
	{
	    boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
	}
    }
    recringbuf_t* ringbuf = handle.container.file.find_or_construct<recringbuf_t>(mkey.c_str)(
	    DEFAULT_HISTORY_DEPTH, 
	    handle.container.file.get_segment_manager());
    if (UNLIKELY_EXT(ringbuf->full()))
    {
	// TODO: need a smarter growth algorithm
	ringbuf->grow(ringbuf->capacity() * 1.5);
    }
    record_t tmp(value, mut_resource_pool(handle.container).global_revision.fetch_add(
	    1, boost::memory_order_relaxed),
	    bpt::microsec_clock::local_time());
    ringbuf->push_front(tmp);
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_timestamp.reset(tmp.timestamp);
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_revision.reset(tmp.revision);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

boost::uint64_t get_last_read_revision_(const mvcc_mmap_container& container, const mvcc_mmap_reader_handle& handle)
{
    const mvcc_mmap_resource_pool& pool = const_resource_pool(container);
    if (pool.reader_token_pool[handle.token_id].last_read_revision)
    {
	return pool.reader_token_pool[handle.token_id].last_read_revision.get();
    }
    else
    {
	return 0U;
    }
}

#endif

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
    return ::exists_<element_t>(reader_handle_, key);
}

template <class element_t>
const element_t& mvcc_mmap_reader::read(const char* key) const
{
    return ::read_<element_t>(reader_handle_, key).value;
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_mmap_reader::get_reader_token_id() const
{
    return reader_handle_.token_id;
}

boost::uint64_t mvcc_mmap_reader::get_last_read_revision() const
{
    return get_last_read_revision_(container_, reader_handle_);
}

#endif

template <class element_t>
bool mvcc_mmap_owner::exists(const char* key) const
{
    return ::exists_<element_t>(reader_handle_, key);
}

template <class element_t>
const element_t& mvcc_mmap_owner::read(const char* key) const
{
    return ::read_<element_t>(reader_handle_, key).value;
}

template <class element_t>
void mvcc_mmap_owner::write(const char* key, const element_t& value)
{
    ::write_(writer_handle_, key, value);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_mmap_owner::get_reader_token_id() const
{
    return reader_handle_.token_id;
}

boost::uint64_t mvcc_mmap_owner::get_last_read_revision() const
{
    return get_last_read_revision_(container_, reader_handle_);
}

boost::uint64_t mvcc_mmap_owner::get_global_oldest_revision_read() const
{
    if (const_resource_pool(container_).owner_token.oldest_revision_found)
    {
	return const_resource_pool(container_).owner_token.oldest_revision_found.get();
    }
    else
    {
	return 0U;
    }
}

std::vector<std::string> mvcc_mmap_owner::get_registered_keys() const
{
    const mvcc_mmap_resource_pool& pool = const_resource_pool(container_);
    std::vector<std::string> result;
    for (registry_map::const_iterator iter = pool.owner_token.registry.begin(); iter != pool.owner_token.registry.end(); ++iter)
    {
	result.push_back(iter->first.c_str);
    }
    return result;
}

template <class element_t> 
std::size_t mvcc_mmap_owner::get_history_depth(const char* key) const
{
    typedef typename mmap_ring_buffer< mvcc_record<element_t> >::type recringbuf_t;
    const recringbuf_t* ringbuf = find_const<recringbuf_t>(container_, key);
    if (!ringbuf || ringbuf->empty())
    {
	return 0U;
    }
    else
    {
	return ringbuf->element_count();
    }
}

#endif

} // namespace grid_db
} // namespace simulation_grid

#endif
