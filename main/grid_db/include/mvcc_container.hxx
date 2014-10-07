#ifndef SIMULATION_GRID_GRID_DB_MVCC_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MVCC_CONTAINER_HXX

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
#include "mvcc_container.hpp"

namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;
namespace bra = boost::random;

namespace simulation_grid {
namespace grid_db {

typedef boost::uint64_t mvcc_revision;
typedef boost::uint8_t history_depth;
typedef boost::function<void(mvcc_container&, const char*, mvcc_revision)> delete_function;
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
struct mvcc_allocator
{
    typedef boost::interprocess::allocator<content_t, 
	    boost::interprocess::managed_mapped_file::segment_manager> type;
};

template <class key_t, class value_t>
struct mvcc_map
{
    typedef typename mvcc_allocator< std::pair<const key_t, value_t> >::type allocator_t;
    typedef boost::interprocess::map<key_t, value_t, std::less<key_t>, allocator_t> type;
};

template <class content_t, size_t size>
struct mvcc_queue 
{
    typedef typename boost::lockfree::capacity<size> capacity;
    typedef typename boost::lockfree::fixed_sized<true> fixed;
    typedef typename boost::lockfree::allocator<typename mvcc_allocator<content_t>::type> allocator_t;
    typedef boost::lockfree::queue<content_t, capacity, fixed, allocator_t> type;
};

struct mvcc_header
{
    boost::uint16_t endianess_indicator;
    char file_type_tag[48];
    version container_version;
    boost::uint16_t header_size;
    mvcc_header();
};

#ifdef LEVEL1_DCACHE_LINESIZE

struct mvcc_reader_token
{
    boost::optional<mvcc_revision> last_read_revision;
    boost::optional<bpt::ptime> last_read_timestamp;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

struct mvcc_writer_token
{
    boost::optional<mvcc_revision> last_write_revision;
    boost::optional<bpt::ptime> last_write_timestamp;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

#endif

typedef mvcc_map<mvcc_key, mvcc_deleter>::type registry_map;

struct mvcc_owner_token
{
    mvcc_owner_token(bip::managed_mapped_file* file);
    boost::optional<mvcc_revision> last_flush_revision;
    boost::optional<bpt::ptime> last_flush_timestamp;
    boost::optional<reader_token_id> oldest_reader_id_found;
    boost::optional<mvcc_revision> oldest_revision_found;
    boost::optional<bpt::ptime> oldest_timestamp_found;
    registry_map registry;
};

struct mvcc_resource_pool
{
    mvcc_resource_pool(bip::managed_mapped_file* file);
    mvcc_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
    boost::atomic<mvcc_revision> global_revision;
    mvcc_owner_token owner_token;
    mvcc_queue<reader_token_id, MVCC_READER_LIMIT>::type reader_free_list;
    mvcc_queue<writer_token_id, MVCC_WRITER_LIMIT>::type writer_free_list;
    mvcc_queue<mvcc_deleter, DEFAULT_HISTORY_DEPTH>::type deleter_list;
};

const mvcc_header& const_header(const mvcc_container& container);
mvcc_header& mut_header(mvcc_container& container);
const mvcc_resource_pool& const_resource_pool(const mvcc_container& container);
mvcc_resource_pool& mut_resource_pool(const mvcc_container& container);

} // namespace grid_db
} // namespace simulation_grid

namespace {

using namespace simulation_grid::grid_db;

template <class value_t>
struct mvcc_value
{
    mvcc_value(const value_t& v, const mvcc_revision& r, const bpt::ptime& t) :
	    value(v), revision(r), timestamp(t)
    { }
    value_t value;
    mvcc_revision revision;
    bpt::ptime timestamp;
};

template <class content_t>
struct mvcc_ring_buffer
{
    typedef bip::allocator<content_t, bip::managed_mapped_file::segment_manager> allocator_type;
    typedef multi_reader_ring_buffer<content_t, allocator_type> type;
};

#ifdef LEVEL1_DCACHE_LINESIZE

template <class content_t>
struct mvcc_record
{
    typedef typename mvcc_ring_buffer< mvcc_value<content_t> >::type ringbuf_t;
    mvcc_record(const typename mvcc_ring_buffer< mvcc_value<content_t> >::allocator_type& allocator, std::size_t depth = DEFAULT_HISTORY_DEPTH) :
	    ringbuf(depth, allocator),
	    want_removed(false)
    { }
    typename mvcc_ring_buffer< mvcc_value<content_t> >::type ringbuf;
    boost::atomic<bool> want_removed;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

#endif

template <class element_t>
bool exists_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<element_t>* record = handle.container.find_const< mvcc_record<element_t> >(key);
    return record && !record->ringbuf.empty() && !record->want_removed;
}

template <class element_t>
const boost::optional<const element_t&> read_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<element_t>* record = handle.container.find_const< mvcc_record<element_t> >(key);
    boost::optional<const element_t&> result;
    if (record && !record->ringbuf.empty() && !record->want_removed)
    {
	const mvcc_value<element_t>& value = record->ringbuf.front();
	result = value.value;
	// the mvcc_reader_token will ensure the returned reference remains valid
	mut_resource_pool(handle.container).reader_token_pool[handle.token_id].
		last_read_timestamp.reset(value.timestamp);
	mut_resource_pool(handle.container).reader_token_pool[handle.token_id].
		last_read_revision.reset(value.revision);
    }
    return result;
}

template <class element_t>
void delete_oldest(mvcc_container& container, const char* key, mvcc_revision threshold)
{
    mvcc_record<element_t>* record = container.find_mut< mvcc_record<element_t> >(key);
    if (record)
    {
	const mvcc_value<element_t>& value = record->ringbuf.back();
	if ((record->ringbuf.element_count() > 1 && value.revision < threshold) || record->want_removed)
	{
	    record->ringbuf.pop_back(value);
	}
    }
}

// TODO: provide strong exception guarantee
template <class element_t>
void write_(mvcc_writer_handle& handle, const char* key, const element_t& value)
{
    mvcc_key mkey(key);
    if (!handle.container.find_const< mvcc_record<element_t> >(mkey.c_str))
    {
	bra::mt19937 seed;
	bra::uniform_int_distribution<> generator(100, 200);
	mvcc_deleter deleter(mkey, &delete_oldest<element_t>);
	while (UNLIKELY_EXT(!mut_resource_pool(handle.container).deleter_list.push(deleter)))
	{
	    boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
	}
    }
    mvcc_record<element_t>* record = handle.container.get_file().find_or_construct< mvcc_record<element_t> >(mkey.c_str)(
	    handle.container.get_file().get_segment_manager());
    if (UNLIKELY_EXT(record->ringbuf.full()))
    {
	// TODO: need a smarter growth algorithm
	record->ringbuf.grow(record->ringbuf.capacity() * 1.5);
    }
    mvcc_value<element_t> tmp(value, mut_resource_pool(handle.container).global_revision.fetch_add(
	    1, boost::memory_order_relaxed),
	    bpt::microsec_clock::local_time());
    record->ringbuf.push_front(tmp);
    record->want_removed = false;
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_timestamp.reset(tmp.timestamp);
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_revision.reset(tmp.revision);
}

template <class element_t>
void remove_(mvcc_writer_handle& handle, const char* key)
{
    mvcc_record<element_t>* record = handle.container.find_mut< mvcc_record<element_t> >(key);
    if (record)
    {
	record->want_removed = true;
    }
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

boost::uint64_t get_last_read_revision_(const mvcc_container& container, const mvcc_reader_handle& handle)
{
    const mvcc_resource_pool& pool = const_resource_pool(container);
    if (pool.reader_token_pool[handle.token_id].last_read_revision)
    {
	return pool.reader_token_pool[handle.token_id].last_read_revision.get();
    }
    else
    {
	return 0U;
    }
}

template <class element_t>
boost::uint64_t get_oldest_revision_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<element_t>* record = handle.container.find_const< mvcc_record<element_t> >(key);
    if (UNLIKELY_EXT(!record || record->ringbuf.empty()))
    {
	return 0U;
    }
    else
    {
	return record->ringbuf.back().revision;
    }
}

template <class element_t>
boost::uint64_t get_newest_revision_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<element_t>* record = handle.container.find_const< mvcc_record<element_t> >(key);
    if (UNLIKELY_EXT(!record || record->ringbuf.empty()))
    {
	return 0U;
    }
    else
    {
	return record->ringbuf.front().revision;
    }
}

#endif

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

template <class element_t>
const element_t* mvcc_container::find_const(const bip::managed_mapped_file::char_type* key) const
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_container*>(this)->file_.find<element_t>(key).first;
}

template <class element_t>
element_t* mvcc_container::find_mut(const bip::managed_mapped_file::char_type* key)
{
    return file_.find<element_t>(key).first;
}

const mvcc_header& const_header(const mvcc_container& container);
mvcc_header& mut_header(mvcc_container& container);
const mvcc_resource_pool& const_resource_pool(const mvcc_container& container);
mvcc_resource_pool& mut_resource_pool(const mvcc_container& container);

template <class element_t>
bool mvcc_reader::exists(const char* key) const
{
    return ::exists_<element_t>(reader_handle_, key);
}

template <class element_t>
const boost::optional<const element_t&> mvcc_reader::read(const char* key) const
{
    return ::read_<element_t>(reader_handle_, key);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_reader::get_reader_token_id() const
{
    return reader_handle_.token_id;
}

boost::uint64_t mvcc_reader::get_last_read_revision() const
{
    return get_last_read_revision_(container_, reader_handle_);
}

template <class element_t>
boost::uint64_t mvcc_reader::get_oldest_revision(const char* key) const
{
    return ::get_oldest_revision_<element_t>(reader_handle_, key);
}

template <class element_t>
boost::uint64_t mvcc_reader::get_newest_revision(const char* key) const
{
    return ::get_newest_revision_<element_t>(reader_handle_, key);
}

#endif

template <class element_t>
bool mvcc_owner::exists(const char* key) const
{
    return ::exists_<element_t>(reader_handle_, key);
}

template <class element_t>
const boost::optional<const element_t&> mvcc_owner::read(const char* key) const
{
    return ::read_<element_t>(reader_handle_, key);
}

template <class element_t>
void mvcc_owner::write(const char* key, const element_t& value)
{
    ::write_(writer_handle_, key, value);
}

template <class element_t>
void mvcc_owner::remove(const char* key)
{
    ::remove_<element_t>(writer_handle_, key);
}

#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

reader_token_id mvcc_owner::get_reader_token_id() const
{
    return reader_handle_.token_id;
}

boost::uint64_t mvcc_owner::get_last_read_revision() const
{
    return get_last_read_revision_(container_, reader_handle_);
}

template <class element_t>
boost::uint64_t mvcc_owner::get_oldest_revision(const char* key) const
{
    return ::get_oldest_revision_<element_t>(reader_handle_, key);
}

template <class element_t>
boost::uint64_t mvcc_owner::get_newest_revision(const char* key) const
{
    return ::get_newest_revision_<element_t>(reader_handle_, key);
}

boost::uint64_t mvcc_owner::get_global_oldest_revision_read() const
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

std::vector<std::string> mvcc_owner::get_registered_keys() const
{
    const mvcc_resource_pool& pool = const_resource_pool(container_);
    std::vector<std::string> result;
    for (registry_map::const_iterator iter = pool.owner_token.registry.begin(); iter != pool.owner_token.registry.end(); ++iter)
    {
	result.push_back(iter->first.c_str);
    }
    return result;
}

template <class element_t> 
std::size_t mvcc_owner::get_history_depth(const char* key) const
{
    const mvcc_record<element_t>* record = container_.find_const< mvcc_record<element_t> >(key);
    if (!record || record->ringbuf.empty())
    {
	return 0U;
    }
    else
    {
	return record->ringbuf.element_count();
    }
}

#endif

} // namespace grid_db
} // namespace simulation_grid

#endif
