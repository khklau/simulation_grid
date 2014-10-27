#include "mvcc_shm.hpp"
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/ref.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mvcc_shm.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace simulation_grid {
namespace grid_db {

mvcc_shm_reader::mvcc_shm_reader(const std::string& name)
try :
    name_(name),
    share_(bip::open_only, name.c_str()),
    reader_handle_(share_)
{
}
catch (grid_db_condition& cond)
{
    cond << info_db_identity(name);
    throw cond;
}
catch (grid_db_error& err)
{
    err << info_db_identity(name);
    throw err;
}

mvcc_shm_reader::~mvcc_shm_reader()
{ }

std::size_t mvcc_shm_reader::get_available_space() const
{
    return reader_handle_.get_available_space();
}

std::size_t mvcc_shm_reader::get_size() const
{
    return reader_handle_.get_size();
}

mvcc_shm_owner::mvcc_shm_owner(const std::string& name, std::size_t size)
try :
    exists_(does_shm_exist(name)),
    name_(name),
    share_(bip::open_or_create, name.c_str(), size),
    owner_handle_(exists_ ? open_existing : open_new, share_),
    writer_handle_(share_),
    reader_handle_(share_)
{
}
catch (grid_db_condition& cond)
{
    cond << info_db_identity(name);
    throw cond;
}
catch (grid_db_error& err)
{
    err << info_db_identity(name);
    throw err;
}

mvcc_shm_owner::~mvcc_shm_owner()
{ }

void mvcc_shm_owner::process_read_metadata(reader_token_id from, reader_token_id to)
{
    return owner_handle_.process_read_metadata(from, to);
}

void mvcc_shm_owner::process_write_metadata(std::size_t max_attempts)
{
    return owner_handle_.process_write_metadata(max_attempts);
}

std::string mvcc_shm_owner::collect_garbage(std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(max_attempts);
}

std::string mvcc_shm_owner::collect_garbage(const std::string& from, std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(from, max_attempts);
}

std::size_t mvcc_shm_owner::get_available_space() const
{
    return reader_handle_.get_available_space();
}

std::size_t mvcc_shm_owner::get_size() const
{
    return reader_handle_.get_size();
}

bool mvcc_shm_owner::does_shm_exist(const std::string& name)
{
    bool result = true;
    try
    {
	bip::managed_shared_memory(bip::open_only, name.c_str());
    }
    catch (...)
    {
	result = false;
    }
    return result;
}

} // namespace grid_db
} // namespace simulation_grid
