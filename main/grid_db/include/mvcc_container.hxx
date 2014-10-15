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
#include <boost/interprocess/managed_shared_memory.hpp>
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

template <class memory_t>
struct mvcc_deleter_
{
    typedef boost::function<void(memory_t&, const char*, mvcc_revision)> delete_function;
    mvcc_deleter_();
    mvcc_deleter_(const mvcc_key& k, const delete_function& fn);
    mvcc_deleter_(const mvcc_deleter_<memory_t>& other);
    ~mvcc_deleter_();
    mvcc_deleter_& operator=(const mvcc_deleter_<memory_t>& other);
    mvcc_key key;
    delete_function function;
};

struct mvcc_deleter
{
    typedef boost::function<void(mvcc_container&, const char*, mvcc_revision)> delete_function;
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

// FIXME: this is anti-generic but we don't have a better solution right now

template <>
struct has_trivial_destructor< sgd::mvcc_deleter_<bip::managed_mapped_file> >
{
    static const bool value = true;
};

template <>
struct has_trivial_destructor< sgd::mvcc_deleter_<bip::managed_shared_memory> >
{
    static const bool value = true;
};

template <>
struct has_trivial_assign<sgd::mvcc_deleter>
{
    static const bool value = true;
};

template <>
struct has_trivial_assign< sgd::mvcc_deleter_<bip::managed_mapped_file> >
{
    static const bool value = true;
};

template <>
struct has_trivial_assign< sgd::mvcc_deleter_<bip::managed_shared_memory> >
{
    static const bool value = true;
};

} // namespace boost

namespace simulation_grid {
namespace grid_db {

// TODO: replace the following with type aliases after moving to a C++11 compiler

template <class content_t, class memory_t = bip::managed_mapped_file>
struct mvcc_allocator
{
    typedef bip::allocator<content_t, typename memory_t::segment_manager> type;
};

template <class key_t, class value_t, class memory_t = bip::managed_mapped_file>
struct mvcc_map
{
    typedef typename mvcc_allocator<std::pair<const key_t, value_t>, memory_t>::type allocator_t;
    typedef boost::interprocess::map<key_t, value_t, std::less<key_t>, allocator_t> type;
};

template <class content_t, size_t size, class memory_t = bip::managed_mapped_file>
struct mvcc_queue 
{
    typedef typename boost::lockfree::capacity<size> capacity;
    typedef typename boost::lockfree::fixed_sized<true> fixed;
    typedef typename boost::lockfree::allocator<typename mvcc_allocator<content_t, memory_t>::type> allocator_t;
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

template <class memory_t>
struct mvcc_owner_token_
{
    typedef typename mvcc_map<mvcc_key, mvcc_deleter_<memory_t>, memory_t>::type registry_map_;
    mvcc_owner_token_(memory_t* file);
    boost::optional<mvcc_revision> last_flush_revision;
    boost::optional<bpt::ptime> last_flush_timestamp;
    boost::optional<reader_token_id> oldest_reader_id_found;
    boost::optional<mvcc_revision> oldest_revision_found;
    boost::optional<bpt::ptime> oldest_timestamp_found;
    registry_map_ registry;
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

template <class memory_t>
struct mvcc_resource_pool_
{
    mvcc_resource_pool_(memory_t* memory);
    mvcc_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
    boost::atomic<mvcc_revision> global_revision;
    mvcc_owner_token_<memory_t> owner_token;
    typename mvcc_queue<reader_token_id, MVCC_READER_LIMIT, memory_t>::type reader_free_list;
    typename mvcc_queue<writer_token_id, MVCC_WRITER_LIMIT, memory_t>::type writer_free_list;
    typename mvcc_queue<mvcc_deleter_<memory_t>, DEFAULT_HISTORY_DEPTH, memory_t>::type deleter_list;
};

const mvcc_header& const_header(const mvcc_container& container);
mvcc_header& mut_header(mvcc_container& container);
const mvcc_resource_pool& const_resource_pool(const mvcc_container& container);
mvcc_resource_pool& mut_resource_pool(const mvcc_container& container);

} // namespace grid_db
} // namespace simulation_grid

namespace {

using namespace simulation_grid::grid_db;

static const char* RESOURCE_POOL_KEY = "@@RESOURCE_POOL@@";

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

template <class value_t>
const value_t* find_const(const mvcc_container& container, const char* key)
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_container&>(container).memory.find<value_t>(key).first;
}

template <class value_t>
value_t* find_mut(mvcc_container& container, const char* key)
{
    return container.memory.find<value_t>(key).first;
}

template <class value_t>
void delete_oldest(mvcc_container& container, const char* key, mvcc_revision threshold)
{
    mvcc_record<value_t>* record = find_mut< mvcc_record<value_t> >(container, key);
    if (record)
    {
	const mvcc_value<value_t>& value = record->ringbuf.back();
	if ((record->ringbuf.element_count() > 1 && value.revision < threshold) || record->want_removed)
	{
	    record->ringbuf.pop_back(value);
	}
    }
}

template <class memory_t>
const mvcc_resource_pool& const_resource_pool_(const memory_t& memory)
{
    return *(const_cast<memory_t&>(memory).template find<const mvcc_resource_pool>(RESOURCE_POOL_KEY).first);
}

template <class memory_t>
mvcc_resource_pool_<memory_t>& mut_resource_pool_(memory_t& memory)
{
    return *(memory.template find< mvcc_resource_pool_<memory_t> >(RESOURCE_POOL_KEY).first);
}


#ifdef SIMGRID_GRIDDB_MVCCCONTAINER_DEBUG

boost::uint64_t get_last_read_revision_(const mvcc_container& container, const mvcc_reader_handle& handle)
{
    const mvcc_resource_pool& pool = const_resource_pool_(container.memory);
    if (pool.reader_token_pool[handle.token_id].last_read_revision)
    {
	return pool.reader_token_pool[handle.token_id].last_read_revision.get();
    }
    else
    {
	return 0U;
    }
}

template <class value_t>
boost::uint64_t get_oldest_revision_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<value_t>* record = find_const< mvcc_record<value_t> >(handle.container, key);
    if (UNLIKELY_EXT(!record || record->ringbuf.empty()))
    {
	return 0U;
    }
    else
    {
	return record->ringbuf.back().revision;
    }
}

template <class value_t>
boost::uint64_t get_newest_revision_(const mvcc_reader_handle& handle, const char* key)
{
    const mvcc_record<value_t>* record = find_const< mvcc_record<value_t> >(handle.container, key);
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

template <class memory_t>
mvcc_deleter_<memory_t>::mvcc_deleter_() :
    key(), function()
{ }

template <class memory_t>
mvcc_deleter_<memory_t>::mvcc_deleter_(const mvcc_key& k, const delete_function& fn) :
    key(k), function(fn)
{ }

template <class memory_t>
mvcc_deleter_<memory_t>::mvcc_deleter_(const mvcc_deleter_<memory_t>& other) :
    key(other.key), function(other.function)
{ }

template <class memory_t>
mvcc_deleter_<memory_t>::~mvcc_deleter_()
{ }

template <class memory_t>
mvcc_deleter_<memory_t>& mvcc_deleter_<memory_t>::operator=(const mvcc_deleter_<memory_t>& other)
{
    if (this != &other)
    {
	key = other.key;
	function = other.function;
    }
    return *this;
}

template <class memory_t>
mvcc_owner_token_<memory_t>::mvcc_owner_token_(memory_t* memory) :
    registry(std::less<typename registry_map_::key_type>(), memory->get_segment_manager())
{ }

template <class memory_t>
mvcc_resource_pool_<memory_t>::mvcc_resource_pool_(memory_t* memory) :
    global_revision(1),
    owner_token(memory),
    reader_free_list(memory->get_segment_manager()),
    writer_free_list(memory->get_segment_manager()),
    deleter_list(memory->get_segment_manager())
{
    for (reader_token_id id = 0; id < MVCC_READER_LIMIT; ++id)
    {
	reader_free_list.push(id);
    }
    for (writer_token_id id = 0; id < MVCC_WRITER_LIMIT; ++id)
    {
	writer_free_list.push(id);
    }
}

template <class memory_t>
mvcc_reader_handle<memory_t>::mvcc_reader_handle(memory_t& memory) :
    memory(memory), token_id(acquire_reader_token(memory))
{ }

template <class memory_t>
mvcc_reader_handle<memory_t>::~mvcc_reader_handle()
{
    try
    {
	release_reader_token(memory, token_id);
    }
    catch(...)
    {
	// do nothing
    }
}

template <class memory_t>
template <class value_t>
const value_t* mvcc_reader_handle<memory_t>::find_const(const char* key) const
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_reader_handle<memory_t>*>(this)->memory.template find<value_t>(key).first;
}

template <class memory_t>
template <class value_t>
bool mvcc_reader_handle<memory_t>::exists(const char* key) const
{
    const mvcc_record<value_t>* record = find_const< mvcc_record<value_t> >(key);
    return record && !record->ringbuf.empty() && !record->want_removed;
}

template <class memory_t>
template <class value_t>
const boost::optional<const value_t&> mvcc_reader_handle<memory_t>::read(const char* key) const
{
    const mvcc_record<value_t>* record = find_const< mvcc_record<value_t> >(key);
    boost::optional<const value_t&> result;
    if (record && !record->ringbuf.empty() && !record->want_removed)
    {
	const mvcc_value<value_t>& value = record->ringbuf.front();
	result = value.value;
	// the mvcc_reader_token will ensure the returned reference remains valid
	mut_resource_pool_(memory).reader_token_pool[token_id].
		last_read_timestamp.reset(value.timestamp);
	mut_resource_pool_(memory).reader_token_pool[token_id].
		last_read_revision.reset(value.revision);
    }
    return result;
}

template <class memory_t>
reader_token_id mvcc_reader_handle<memory_t>::acquire_reader_token(memory_t& memory)
{
    reader_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool_(memory).reader_free_list.pop(reservation)))
    {
	throw busy_condition("No reader token available")
		<< info_component_identity("mvcc_container");
    }
    return reservation;
}

template <class memory_t>
void mvcc_reader_handle<memory_t>::release_reader_token(memory_t& memory, const reader_token_id& id)
{
    bra::mt19937 seed;
    bra::uniform_int_distribution<> generator(100, 200);
    while (UNLIKELY_EXT(!mut_resource_pool_(memory).reader_free_list.push(id)))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

template <class memory_t>
mvcc_writer_handle<memory_t>::mvcc_writer_handle(memory_t& memory) :
    memory(memory), token_id(acquire_writer_token(memory))
{ }

template <class memory_t>
mvcc_writer_handle<memory_t>::~mvcc_writer_handle()
{
    try
    {
	release_writer_token(memory, token_id);
    }
    catch(...)
    {
	// do nothing
    }
}

template <class memory_t>
template <class value_t>
const value_t* mvcc_writer_handle<memory_t>::find_const(const char* key) const
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_writer_handle<memory_t>*>(this)->memory.template find<value_t>(key).first;
}

template <class memory_t>
template <class value_t>
value_t* mvcc_writer_handle<memory_t>::find_mut(const char* key)
{
    return memory.template find<value_t>(key).first;
}

// TODO: provide strong exception guarantee
template <class memory_t>
template <class value_t>
void mvcc_writer_handle<memory_t>::write(const char* key, const value_t& value)
{
    mvcc_key mkey(key);
    if (!find_const< mvcc_record<value_t> >(mkey.c_str))
    {
	bra::mt19937 seed;
	bra::uniform_int_distribution<> generator(100, 200);
	mvcc_deleter_<memory_t> deleter(mkey, &delete_oldest<value_t>);
	while (UNLIKELY_EXT(!mut_resource_pool_(memory).deleter_list.push(deleter)))
	{
	    boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
	}
    }
    mvcc_record<value_t>* record = memory.template find_or_construct< mvcc_record<value_t> >(mkey.c_str)(
	    memory.get_segment_manager());
    if (UNLIKELY_EXT(record->ringbuf.full()))
    {
	// TODO: need a smarter growth algorithm
	record->ringbuf.grow(record->ringbuf.capacity() * 1.5);
    }
    mvcc_value<value_t> tmp(value, mut_resource_pool_(memory).global_revision.fetch_add(
	    1, boost::memory_order_relaxed),
	    bpt::microsec_clock::local_time());
    record->ringbuf.push_front(tmp);
    record->want_removed = false;
    mut_resource_pool_(memory).writer_token_pool[token_id].
	    last_write_timestamp.reset(tmp.timestamp);
    mut_resource_pool_(memory).writer_token_pool[token_id].
	    last_write_revision.reset(tmp.revision);
}

template <class memory_t>
template <class value_t>
void mvcc_writer_handle<memory_t>::remove(const char* key)
{
    mvcc_record<value_t>* record = find_mut< mvcc_record<value_t> >(key);
    if (record)
    {
	record->want_removed = true;
    }
}

template <class memory_t>
writer_token_id mvcc_writer_handle<memory_t>::acquire_writer_token(memory_t& memory)
{
    writer_token_id reservation;
    if (UNLIKELY_EXT(!mut_resource_pool_(memory).writer_free_list.pop(reservation)))
    {
	throw busy_condition("No writer token available")
		<< info_component_identity("mvcc_container");
    }
    return reservation;
}

template <class memory_t>
void mvcc_writer_handle<memory_t>::release_writer_token(memory_t& memory, const writer_token_id& id)
{
    bra::mt19937 seed;
    bra::uniform_int_distribution<> generator(100, 200);
    while (!UNLIKELY_EXT(mut_resource_pool_(memory).writer_free_list.push(id)))
    {
	boost::this_thread::sleep_for(boost::chrono::nanoseconds(generator(seed)));
    }
}

template <class memory_t>
mvcc_owner_handle<memory_t>::mvcc_owner_handle(memory_t& memory) :
    memory(memory)
{ }

template <class memory_t>
mvcc_owner_handle<memory_t>::~mvcc_owner_handle()
{ }

template <class memory_t>
template <class value_t>
const value_t* mvcc_owner_handle<memory_t>::find_const(const char* key) const
{
    // Unfortunately boost::interprocess::managed_mapped_file::find is not a const function
    // due to use of internal locks which were not declared as mutable, so this function
    // has been provided to fake constness
    return const_cast<mvcc_owner_handle<memory_t>*>(this)->memory.template find<value_t>(key).first;
}

template <class memory_t>
template <class value_t>
value_t* mvcc_owner_handle<memory_t>::find_mut(const char* key)
{
    return memory.template find<value_t>(key).first;
}

template <class memory_t>
void mvcc_owner_handle<memory_t>::process_read_metadata(reader_token_id from, reader_token_id to)
{
    if (from >= MVCC_READER_LIMIT)
    {
	return;
    }
    mvcc_resource_pool& pool = mut_resource_pool_(memory);
    if (pool.owner_token.oldest_reader_id_found && pool.owner_token.oldest_revision_found)
    {
	reader_token_id token_id = pool.owner_token.oldest_reader_id_found.get();
	if (pool.reader_token_pool[token_id].last_read_revision &&
	    pool.owner_token.oldest_revision_found.get() != pool.reader_token_pool[token_id].last_read_revision.get())
	{
	    pool.owner_token.oldest_reader_id_found.reset();
	    pool.owner_token.oldest_revision_found.reset();
	    pool.owner_token.oldest_timestamp_found.reset();
	}
    }
    for (reader_token_id iter = from; iter < MVCC_READER_LIMIT && iter < to; ++iter)
    {
	if ((!pool.owner_token.oldest_revision_found && pool.reader_token_pool[iter].last_read_revision) || 
	    (pool.owner_token.oldest_revision_found && pool.reader_token_pool[iter].last_read_revision &&
	    pool.reader_token_pool[iter].last_read_revision.get() < pool.owner_token.oldest_revision_found.get()))
	{
	    pool.owner_token.oldest_reader_id_found.reset(iter);
	    pool.owner_token.oldest_revision_found.reset(pool.reader_token_pool[iter].last_read_revision.get());
	    pool.owner_token.oldest_timestamp_found.reset(pool.reader_token_pool[iter].last_read_timestamp.get());
	}
    }
}

template <class memory_t>
void mvcc_owner_handle<memory_t>::process_write_metadata(std::size_t max_attempts)
{
    mvcc_deleter deleter;
    mvcc_resource_pool& pool = mut_resource_pool_(memory);
    for (std::size_t attempts = 0; !pool.deleter_list.empty() && (max_attempts == 0 || attempts < max_attempts); ++attempts)
    {
	if (pool.deleter_list.pop(deleter))
	{
	    pool.owner_token.registry.insert(std::make_pair(deleter.key, deleter));
	}
    }
}

template <class memory_t>
std::string mvcc_owner_handle<memory_t>::collect_garbage(std::size_t max_attempts)
{
    std::string from;
    return collect_garbage(from, max_attempts);
}

template <class memory_t>
std::string mvcc_owner_handle<memory_t>::collect_garbage(const std::string& from, std::size_t max_attempts)
{
    mvcc_resource_pool& pool = mut_resource_pool_(memory);
    if (pool.owner_token.registry.empty())
    {
	return "";
    }
    mvcc_key key(from.c_str());
    registry_map::const_iterator iter = from.empty() ?
	    pool.owner_token.registry.begin() :
	    pool.owner_token.registry.find(key);
    if (pool.owner_token.oldest_revision_found)
    {
	mvcc_revision oldest = pool.owner_token.oldest_revision_found.get();
	for (std::size_t attempts = 0; iter != pool.owner_token.registry.end() && (max_attempts == 0 || attempts < max_attempts); ++attempts, ++iter)
	{
	    iter->second.function(memory, iter->first.c_str, oldest);
	}
    }
    if (iter == pool.owner_token.registry.end())
    {
	// Next collect attempt should start again from beginning
	iter = pool.owner_token.registry.begin();
    }
    return iter->first.c_str;
}

template <class value_t>
bool mvcc_reader::exists(const char* key) const
{
    return reader_handle_.exists<value_t>(key);
}

template <class value_t>
const boost::optional<const value_t&> mvcc_reader::read(const char* key) const
{
    return reader_handle_.template read<value_t>(key);
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

template <class value_t>
boost::uint64_t mvcc_reader::get_oldest_revision(const char* key) const
{
    return ::get_oldest_revision_<value_t>(reader_handle_, key);
}

template <class value_t>
boost::uint64_t mvcc_reader::get_newest_revision(const char* key) const
{
    return ::get_newest_revision_<value_t>(reader_handle_, key);
}

#endif

template <class value_t>
bool mvcc_owner::exists(const char* key) const
{
    return reader_handle_.exists<value_t>(key);
}

template <class value_t>
const boost::optional<const value_t&> mvcc_owner::read(const char* key) const
{
    return reader_handle_.template read_<value_t>(key);
}

template <class value_t>
void mvcc_owner::write(const char* key, const value_t& value)
{
    writer_handle_.write(key, value);
}

template <class value_t>
void mvcc_owner::remove(const char* key)
{
    writer_handle_.remove<value_t>(key);
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

template <class value_t>
boost::uint64_t mvcc_owner::get_oldest_revision(const char* key) const
{
    return ::get_oldest_revision_<value_t>(reader_handle_, key);
}

template <class value_t>
boost::uint64_t mvcc_owner::get_newest_revision(const char* key) const
{
    return ::get_newest_revision_<value_t>(reader_handle_, key);
}

boost::uint64_t mvcc_owner::get_global_oldest_revision_read() const
{
    if (const_resource_pool_(container_.memory).owner_token.oldest_revision_found)
    {
	return const_resource_pool_(container_.memory).owner_token.oldest_revision_found.get();
    }
    else
    {
	return 0U;
    }
}

std::vector<std::string> mvcc_owner::get_registered_keys() const
{
    const mvcc_resource_pool& pool = const_resource_pool_(container_.memory);
    std::vector<std::string> result;
    for (registry_map::const_iterator iter = pool.owner_token.registry.begin(); iter != pool.owner_token.registry.end(); ++iter)
    {
	result.push_back(iter->first.c_str);
    }
    return result;
}

template <class value_t> 
std::size_t mvcc_owner::get_history_depth(const char* key) const
{
    const mvcc_record<value_t>* record = find_const< mvcc_record<value_t> >(container_, key);
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
