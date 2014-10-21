#include <cstring>
#include <exception>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <boost/thread/thread.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mmap_container.hpp"
#include "mmap_container.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace {

using namespace simulation_grid::grid_db;

size_t get_size(const mvcc_mmap_container& container)
{
    return container.file.get_size();
}

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

mvcc_mmap_container::mvcc_mmap_container(const owner_t, const bfs::path& path, size_t size) :
    exists(bfs::exists(path)), path(path), file(bip::open_or_create, path.string().c_str(), size)
{ }

mvcc_mmap_container::mvcc_mmap_container(const reader_t, const bfs::path& path) :
    exists(bfs::exists(path)), path(path), file(bip::open_only, path.string().c_str())
{ }

std::size_t mvcc_mmap_container::available_space() const
{
    return file.get_free_memory();
}

mvcc_mmap_reader::mvcc_mmap_reader(const bfs::path& path)
try :
    container_(reader, path), reader_handle_(container_.file)
{
    if (bfs::exists(path))
    {
	reader_handle_.check();
    }
    else
    {
	throw grid_db_error("Given container does not exist")
		<< info_db_identity(path.string());
    }
}
catch (grid_db_condition& cond)
{
    cond << info_db_identity(path.string());
    throw cond;
}
catch (grid_db_error& err)
{
    err << info_db_identity(path.string());
    throw err;
}

mvcc_mmap_reader::~mvcc_mmap_reader()
{ }

mvcc_mmap_owner::mvcc_mmap_owner(const bfs::path& path, std::size_t size)
try :
    exists_(bfs::exists(path)),
    container_(owner, path, size),
    owner_handle_(exists_ ? open_existing : open_new, container_.file),
    writer_handle_(container_.file),
    reader_handle_(container_.file),
    file_lock_(path.string().c_str())
{
    flush();
    file_lock_.lock();
}
catch (grid_db_condition& cond)
{
    cond << info_db_identity(path.string());
    throw cond;
}
catch (grid_db_error& err)
{
    err << info_db_identity(path.string());
    throw err;
}

mvcc_mmap_owner::~mvcc_mmap_owner()
{
    try
    {
	file_lock_.unlock();
    }
    catch(...)
    {
	// Do nothing
    }
}

void mvcc_mmap_owner::process_read_metadata(reader_token_id from, reader_token_id to)
{
    return owner_handle_.process_read_metadata(from, to);
}

void mvcc_mmap_owner::process_write_metadata(std::size_t max_attempts)
{
    return owner_handle_.process_write_metadata(max_attempts);
}

std::string mvcc_mmap_owner::collect_garbage(std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(max_attempts);
}

std::string mvcc_mmap_owner::collect_garbage(const std::string& from, std::size_t max_attempts)
{
    return owner_handle_.collect_garbage(from, max_attempts);
}

void mvcc_mmap_owner::flush()
{
    boost::function<void ()> flush_func(boost::bind(&mvcc_mmap_owner::flush_impl, boost::ref(*this)));
    container_.file.get_segment_manager()->atomic_func(flush_func);
}

void mvcc_mmap_owner::flush_impl()
{
    last_flush_timestamp_ = bpt::microsec_clock::local_time();
    container_.file.flush();
}

} // namespace grid_db
} // namespace simulation_grid
