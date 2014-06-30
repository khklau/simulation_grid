#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include <cstring>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"
#include "mmap_container.hpp"

namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace {

using namespace simulation_grid::grid_db;

typedef boost::uint8_t history_depth;
typedef boost::uint64_t mvcc_revision;
typedef mmap_queue<reader_token_id, MVCC_READER_LIMIT>::type reader_token_list;
typedef mmap_queue<writer_token_id, MVCC_WRITER_LIMIT>::type writer_token_list;

static const size_t DEFAULT_HISTORY_DEPTH = 1 <<  std::numeric_limits<history_depth>::digits;

struct mvcc_mmap_header
{
    boost::uint16_t endianess_indicator;
    char file_type_tag[48];
    version container_version;
    boost::uint16_t header_size;
    mvcc_mmap_header();
};

struct mvcc_mmap_reader_token
{
    boost::optional<mvcc_revision> last_read_revision;
    boost::optional<bpt::ptime> last_read_timestamp;
};

struct mvcc_mmap_writer_token
{
    boost::optional<mvcc_revision> last_write_revision;
    boost::optional<bpt::ptime> last_write_timestamp;
    boost::optional<mvcc_revision> last_flushed_revision;
    boost::optional<bpt::ptime> last_flush_timestamp;
};

struct mvcc_mmap_resource_pool
{
    mvcc_mmap_resource_pool(bip::managed_mapped_file* file);
    mvcc_mmap_reader_token reader_token_pool[MVCC_READER_LIMIT];
    mvcc_mmap_writer_token writer_token_pool[MVCC_WRITER_LIMIT];
    bip::allocator<reader_token_id, bip::managed_mapped_file::segment_manager> reader_allocator;
    bip::allocator<writer_token_id, bip::managed_mapped_file::segment_manager> writer_allocator;
    bip::offset_ptr<reader_token_list> reader_free_list;
    bip::offset_ptr<writer_token_list> writer_free_list;
    mvcc_revision global_revision;
};

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
    typedef typename mmap_allocator<content_t>::type allocator_t;
    typedef multi_reader_ring_buffer<content_t, allocator_t> type;
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

const mvcc_mmap_header& const_header(const mvcc_mmap_container& container);
mvcc_mmap_header& mut_header(mvcc_mmap_container& container);
const mvcc_mmap_resource_pool& const_resource_pool(const mvcc_mmap_container& container);
mvcc_mmap_resource_pool& mut_resource_pool(const mvcc_mmap_container& container);

template <class element_t>
bool exists(const mvcc_mmap_reader_handle& handle, const char* key)
{
    typedef typename mmap_ring_buffer< mvcc_record<element_t> >::type recringbuf_t;
    const recringbuf_t* ringbuf = find_const<recringbuf_t>(handle.container, key);
    return ringbuf && !ringbuf->empty();
}

template <class element_t>
const mvcc_record<element_t>& read_newest(const mvcc_mmap_reader_handle& handle, const char* key)
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
	    last_read_timestamp = record.timestamp;
    mut_resource_pool(handle.container).reader_token_pool[handle.token_id].
	    last_read_revision = record.revision;
    return record;
}

template <class element_t>
const mvcc_record<element_t>& read_oldest(const mvcc_mmap_reader_handle& handle, const char* key)
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
    return ringbuf->front();
}

template <class element_t>
void create_and_insert(const mvcc_mmap_writer_handle& handle, const char* key, const element_t& value)
{
    typedef mvcc_record<element_t> record_t;
    typedef typename mmap_ring_buffer<record_t>::type recringbuf_t;
    recringbuf_t* ringbuf = find_mut<recringbuf_t>(handle.container, key);
    if (!ringbuf)
    {
	ringbuf = handle.container.file.construct<recringbuf_t>(key)(DEFAULT_HISTORY_DEPTH, handle.container.file);
    }
    if (ringbuf->full())
    {
	// TODO: need a smarter growth algorithm
	ringbuf->grow(ringbuf->capacity() * 1.5);
    }
    record_t tmp(value, ++mut_resource_pool(handle.container).global_revision, bpt::microsec_clock::local_time());
    ringbuf->push_front(tmp);
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_timestamp = tmp.timestamp;
    mut_resource_pool(handle.container).writer_token_pool[handle.token_id].
	    last_write_revision = tmp.revision;
}

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

template <class element_t>
bool mvcc_mmap_reader::exists(const char* key) const
{
    return exists<element_t>(reader_handle_, key);
}

template <class element_t>
const element_t& mvcc_mmap_reader::read(const char* key) const
{
    return read_newest<element_t>(reader_handle_, key).value;
}

template <class element_t>
bool mvcc_mmap_owner::exists(const char* key) const
{
    return exists<element_t>(reader_handle_, key);
}

template <class element_t>
const element_t& mvcc_mmap_owner::read(const char* key) const
{
    return read_newest<element_t>(reader_handle_, key).value;
}

template <class element_t>
void mvcc_mmap_owner::write(const char* key, const element_t& value)
{
    boost::function<void ()> write_func(boost::bind(&create_and_insert<element_t>, 
	    boost::ref(writer_handle_), key, boost::ref(value)));
    writer_handle_.container.file.get_segment_manager()->atomic_func(write_func);
}

} // namespace grid_db
} // namespace simulation_grid

#endif
