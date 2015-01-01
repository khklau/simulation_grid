#ifndef SUPERNOVA_STORAGE_LOG_SHM_HXX
#define SUPERNOVA_STORAGE_LOG_SHM_HXX

#include "log_shm.hpp"
#include <supernova/storage/exception.hpp>
#include "mode.hpp"
#include "log_memory.hxx"

namespace bip = boost::interprocess;

namespace supernova {
namespace storage {

template <class entry_t>
log_shm_reader<entry_t>::log_shm_reader(const std::string& name)
try :
    shm_(bip::open_only, name.c_str(), bip::read_only),
    region_(shm_, bip::read_only),
    reader_handle_(region_)
{
}
catch (storage_condition& cond)
{
    cond << info_db_identity(name);
    throw cond;
}
catch (storage_error& err)
{
    err << info_db_identity(name);
    throw err;
}

template <class entry_t>
log_shm_reader<entry_t>::~log_shm_reader()
{ }

template <class entry_t>
boost::optional<const entry_t&> log_shm_reader<entry_t>::read(const log_index& index) const
{
    return reader_handle_.read(index);
}

template <class entry_t>
boost::optional<log_index> log_shm_reader<entry_t>::get_front_index() const
{
    return reader_handle_.get_front_index();
}

template <class entry_t>
boost::optional<log_index> log_shm_reader<entry_t>::get_back_index() const
{
    return reader_handle_.get_back_index();
}

template <class entry_t>
log_index log_shm_reader<entry_t>::get_max_index() const
{
    return reader_handle_.get_max_index();
}

bip::shared_memory_object& init_shared_memory(bip::shared_memory_object& shm, std::size_t size);

template <class entry_t>
log_shm_owner<entry_t>::log_shm_owner(const std::string& name, std::size_t size)
try :
    exists_(does_shm_exist(name)),
    shm_(bip::open_or_create, name.c_str(), bip::read_write),
    region_(init_shared_memory(shm_, size), bip::read_write, 0U, size),
    owner_handle_(exists_ ? open_existing : open_new, region_),
    reader_handle_(region_)
{
}
catch (storage_condition& cond)
{
    cond << info_db_identity(name);
    throw cond;
}
catch (storage_error& err)
{
    err << info_db_identity(name);
    throw err;
}

template <class entry_t>
log_shm_owner<entry_t>::~log_shm_owner()
{
    bip::shared_memory_object::remove(name_.c_str());
}

template <class entry_t>
boost::optional<log_index> log_shm_owner<entry_t>::append(const entry_t& entry)
{
    return owner_handle_.append(entry);
}

template <class entry_t>
boost::optional<const entry_t&> log_shm_owner<entry_t>::read(const log_index& index) const
{
    return reader_handle_.read(index);
}

template <class entry_t>
boost::optional<log_index> log_shm_owner<entry_t>::get_front_index() const
{
    return reader_handle_.get_front_index();
}

template <class entry_t>
boost::optional<log_index> log_shm_owner<entry_t>::get_back_index() const
{
    return reader_handle_.get_back_index();
}

template <class entry_t>
log_index log_shm_owner<entry_t>::get_max_index() const
{
    return reader_handle_.get_max_index();
}

template <class entry_t>
bool log_shm_owner<entry_t>::does_shm_exist(const std::string& name)
{
    bool result = true;
    try
    {
	bip::shared_memory_object(bip::open_only, name.c_str(), bip::read_only);
    }
    catch (...)
    {
	result = false;
    }
    return result;
}

} // namespace storage
} // namespace supernova

#endif
