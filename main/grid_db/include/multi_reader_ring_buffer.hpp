#ifndef SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HPP
#define SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HPP

#include <memory>
#include <boost/noncopyable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/circular_buffer.hpp>

namespace simulation_grid {
namespace grid_db {

template <class element_t, class allocator_t = std::allocator<element_t> >
class multi_reader_ring_buffer : private boost::noncopyable
{
public:
    typedef typename allocator_t::reference element_ref_t;
    typedef typename allocator_t::const_reference const_element_ref_t;
    typedef typename allocator_t::size_type size_type;
    typedef boost::shared_lock<boost::shared_mutex> read_lock;
    typedef boost::unique_lock<boost::shared_mutex> write_lock;
    multi_reader_ring_buffer(size_type capacity);
    const_element_ref_t front() const;
    void push_front(const_element_ref_t element);
    const_element_ref_t back() const;
    void pop_back(const_element_ref_t back_element);
    size_type capacity() const;
    size_type element_count() const;
    bool empty() const;
    bool full() const;
private:
    boost::shared_mutex mutex_;
    boost::circular_buffer<element_t, allocator_t> buffer_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
