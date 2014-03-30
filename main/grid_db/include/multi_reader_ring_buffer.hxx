#ifndef SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HXX
#define SIMULATION_GRID_GRID_DB_MULTI_READER_RING_BUFFER_HXX

#include <boost/circular_buffer.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include "multi_reader_ring_buffer.hpp"

namespace bi = boost::interprocess;

namespace {

static const char* MUTEX_KEY_ = "@@MUTEX@@";
static const char* RINGBUF_KEY_ = "@@RING_BUFFER@@";

} // annonymous namespace

namespace simulation_grid {
namespace grid_db {

template <class element_t, class memory_t>
multi_reader_ring_buffer<element_t, memory_t>::multi_reader_ring_buffer(size_type capacity, memory_t* file) :
    allocator_(file->get_segment_manager()),
    mdeleter_(file->get_segment_manager()),
    rdeleter_(file->get_segment_manager()),
    mutex_(file->template construct<bi::interprocess_sharable_mutex>(MUTEX_KEY_)(), mdeleter_),
    ringbuf_(file->template construct<boost::circular_buffer<element_t, allocator_t> >(RINGBUF_KEY_)(capacity, allocator_), rdeleter_)
{
    ringbuf_->set_capacity(capacity);
}

template <class element_t, class memory_t>
typename multi_reader_ring_buffer<element_t, memory_t>::const_element_ref_t multi_reader_ring_buffer<element_t, memory_t>::front() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->front();
}

template <class element_t, class memory_t>
void multi_reader_ring_buffer<element_t, memory_t>::push_front(const_element_ref_t value)
{
    write_lock(*(this->mutex_));
    ringbuf_->push_front(value);
}

template <class element_t, class memory_t>
typename multi_reader_ring_buffer<element_t, memory_t>::const_element_ref_t multi_reader_ring_buffer<element_t, memory_t>::back() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->back();
}

template <class element_t, class memory_t>
void multi_reader_ring_buffer<element_t, memory_t>::pop_back(const_element_ref_t back_element)
{
    write_lock(*(this->mutex_));
    const_element_ref_t current_back = back();
    if (&back_element == &current_back)
    {
	ringbuf_->pop_back();
    }
}

template <class element_t, class memory_t>
typename multi_reader_ring_buffer<element_t, memory_t>::size_type multi_reader_ring_buffer<element_t, memory_t>::capacity() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->capacity();
}

template <class element_t, class memory_t>
typename multi_reader_ring_buffer<element_t, memory_t>::size_type multi_reader_ring_buffer<element_t, memory_t>::element_count() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->size();
}

template <class element_t, class memory_t>
bool multi_reader_ring_buffer<element_t, memory_t>::empty() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->empty();
}

template <class element_t, class memory_t>
bool multi_reader_ring_buffer<element_t, memory_t>::full() const
{
    read_lock(*(this->mutex_));
    return ringbuf_->full();
}

} // namespace grid_db
} // namespace simulation_grid

#endif
