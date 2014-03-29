#ifndef SIMULATION_GRID_GRID_DB_MULTI_READER_RING_RINGBUF_HPP
#define SIMULATION_GRID_GRID_DB_MULTI_READER_RING_RINGBUF_HPP

#include <memory>
#include <boost/noncopyable.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/smart_ptr/deleter.hpp>
#include <boost/interprocess/smart_ptr/scoped_ptr.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>
#include <boost/circular_buffer.hpp>

namespace simulation_grid {
namespace grid_db {

template <class element_t, class memory_t>
class multi_reader_ring_buffer : private boost::noncopyable
{
public:
    typedef boost::interprocess::allocator<element_t, typename memory_t::segment_manager> allocator_t;
    typedef typename allocator_t::reference element_ref_t;
    typedef typename allocator_t::const_reference const_element_ref_t;
    typedef typename allocator_t::size_type size_type;
    typedef boost::interprocess::sharable_lock<boost::interprocess::interprocess_sharable_mutex> read_lock;
    typedef boost::interprocess::scoped_lock<boost::interprocess::interprocess_sharable_mutex> write_lock;
    multi_reader_ring_buffer(size_type capacity, memory_t* file);
    const_element_ref_t front() const;
    void push_front(const_element_ref_t element);
    const_element_ref_t back() const;
    void pop_back(const_element_ref_t back_element);
    size_type capacity() const;
    size_type element_count() const;
    bool empty() const;
    bool full() const;
private:
    typedef boost::interprocess::deleter<boost::interprocess::interprocess_sharable_mutex, typename memory_t::segment_manager>  mutex_deleter_t;
    typedef boost::interprocess::deleter<boost::circular_buffer<element_t, allocator_t>, typename memory_t::segment_manager>  ringbuf_deleter_t;
    //boost::interprocess::interprocess_sharable_mutex mutex_;
    //boost::circular_buffer<element_t, allocator_t> buffer_;
    allocator_t allocator_;
    mutex_deleter_t mdeleter_;
    ringbuf_deleter_t rdeleter_;
    boost::interprocess::scoped_ptr<boost::interprocess::interprocess_sharable_mutex, mutex_deleter_t> mutex_;
    boost::interprocess::scoped_ptr<boost::circular_buffer<element_t, allocator_t>, ringbuf_deleter_t> ringbuf_;
};

// TODO : replace the following with type aliases after moving to a C++11 compiler
template <class element_t>
struct mmap_multi_reader_ring_buffer
{
    typedef multi_reader_ring_buffer<element_t, boost::interprocess::managed_mapped_file> type;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
