#ifndef SUPERNOVA_STORAGE_MULTI_READER_RING_BUFFER_HXX
#define SUPERNOVA_STORAGE_MULTI_READER_RING_BUFFER_HXX

#include <boost/circular_buffer.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include "multi_reader_ring_buffer.hpp"

namespace bi = boost::interprocess;

namespace supernova {
namespace storage {

template <class element_t, class allocator_t>
multi_reader_ring_buffer<element_t, allocator_t>::multi_reader_ring_buffer(size_type capacity, const allocator_t& allocator) :
    ringbuf_(capacity, allocator)
{ }

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t front_impl(
	const boost::circular_buffer<element_t, allocator_t>& buf)
{
    // This hack is required because the circular_buffer documentation says it has
    // an overload of the front member function returning a const reference, but 
    // it doesn't exist in the code
    return const_cast< boost::circular_buffer<element_t, allocator_t>& >(buf).front();
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t multi_reader_ring_buffer<element_t, allocator_t>::front() const
{
    read_lock(mutex_);
    return front_impl<element_t, allocator_t>(ringbuf_);
}

template <class element_t, class allocator_t>
void multi_reader_ring_buffer<element_t, allocator_t>::push_front(const_element_ref_t value)
{
    write_lock(mutex_);
    ringbuf_.push_front(value);
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t back_impl(
	const boost::circular_buffer<element_t, allocator_t>& buf)
{
    // This hack is required because the circular_buffer documentation says it has
    // an overload of the back member function returning a const reference, but 
    // it doesn't exist in the code
    return const_cast< boost::circular_buffer<element_t, allocator_t>& >(buf).back();
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t multi_reader_ring_buffer<element_t, allocator_t>::back() const
{
    read_lock(mutex_);
    return back_impl<element_t, allocator_t>(ringbuf_);
}

template <class element_t, class allocator_t>
void multi_reader_ring_buffer<element_t, allocator_t>::pop_back(const_element_ref_t back_element)
{
    write_lock(mutex_);
    const_element_ref_t current_back = back();
    if (&back_element == &current_back)
    {
	ringbuf_.pop_back();
    }
}

template <class element_t, class allocator_t>
void multi_reader_ring_buffer<element_t, allocator_t>::grow(size_type new_capacity)
{
    write_lock(mutex_);
    ringbuf_.set_capacity(new_capacity);
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::size_type multi_reader_ring_buffer<element_t, allocator_t>::capacity() const
{
    read_lock(mutex_);
    return ringbuf_.capacity();
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::size_type multi_reader_ring_buffer<element_t, allocator_t>::element_count() const
{
    read_lock(mutex_);
    return ringbuf_.size();
}

template <class element_t, class allocator_t>
bool multi_reader_ring_buffer<element_t, allocator_t>::empty() const
{
    read_lock(mutex_);
    return ringbuf_.empty();
}

template <class element_t, class allocator_t>
bool multi_reader_ring_buffer<element_t, allocator_t>::full() const
{
    read_lock(mutex_);
    return ringbuf_.full();
}

} // namespace storage
} // namespace supernova

#endif
