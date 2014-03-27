#ifndef SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HXX
#define SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HXX

#include "multi_reader_ring_buffer.hpp"

namespace simulation_grid {
namespace grid_db {

template <class element_t, class allocator_t>
multi_reader_ring_buffer<element_t, allocator_t>::multi_reader_ring_buffer(size_type capacity)
{
    buffer_.set_capacity(capacity);
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t multi_reader_ring_buffer<element_t, allocator_t>::front() const
{
    read_lock(mutex_);
    return buffer_.front();
}

template <class element_t, class allocator_t>
void multi_reader_ring_buffer<element_t, allocator_t>::push_front(const_element_ref_t value)
{
    write_lock(mutex_);
    buffer_.push_front(value);
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::const_element_ref_t multi_reader_ring_buffer<element_t, allocator_t>::back() const
{
    read_lock(mutex_);
    return buffer_.back();
}

template <class element_t, class allocator_t>
void multi_reader_ring_buffer<element_t, allocator_t>::pop_back(const_element_ref_t back_element)
{
    write_lock(mutex_);
    const_element_ref_t current_back = back();
    if (&back_element == &current_back)
    {
	buffer_.pop_back();
    }
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::size_type multi_reader_ring_buffer<element_t, allocator_t>::capacity() const
{
    read_lock(mutex_);
    return buffer_.capacity();
}

template <class element_t, class allocator_t>
typename multi_reader_ring_buffer<element_t, allocator_t>::size_type multi_reader_ring_buffer<element_t, allocator_t>::element_count() const
{
    read_lock(mutex_);
    return buffer_.size();
}

template <class element_t, class allocator_t>
bool multi_reader_ring_buffer<element_t, allocator_t>::empty() const
{
    read_lock(mutex_);
    return buffer_.empty();
}

template <class element_t, class allocator_t>
bool multi_reader_ring_buffer<element_t, allocator_t>::full() const
{
    read_lock(mutex_);
    return buffer_.full();
}

} // namespace grid_db
} // namespace simulation_grid

#endif
