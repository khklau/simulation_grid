#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HXX

#include "mmap_container.hpp"
#include "mvcc_memory.hxx"

namespace simulation_grid {
namespace grid_db {

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

#ifdef SIMGRID_GRIDDB_MVCCMEMORY_DEBUG

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

#ifdef SIMGRID_GRIDDB_MVCCMEMORY_DEBUG

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
