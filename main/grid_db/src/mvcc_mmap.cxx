#include "mvcc_mmap.hpp"
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/ref.hpp>
#include <supernova/storage/exception.hpp>
#include "mvcc_mmap.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace supernova {
namespace storage {

mvcc_mmap_reader::mvcc_mmap_reader(const bfs::path& path)
try :
    path_(path),
    file_(bip::open_only, path.string().c_str()),
    reader_handle_(file_)
{
}
catch (storage_condition& cond)
{
    cond << info_db_identity(path.string());
    throw cond;
}
catch (storage_error& err)
{
    err << info_db_identity(path.string());
    throw err;
}

mvcc_mmap_reader::~mvcc_mmap_reader()
{ }

std::size_t mvcc_mmap_reader::get_available_space() const
{
    return reader_handle_.get_available_space();
}

std::size_t mvcc_mmap_reader::get_size() const
{
    return reader_handle_.get_size();
}

mvcc_mmap_owner::mvcc_mmap_owner(const bfs::path& path, std::size_t size)
try :
    exists_(bfs::exists(path)),
    path_(path),
    file_(bip::open_or_create, path.string().c_str(), size),
    owner_handle_(exists_ ? open_existing : open_new, file_),
    writer_handle_(file_),
    reader_handle_(file_),
    file_lock_(path.string().c_str())
{
    flush();
    file_lock_.lock();
}
catch (storage_condition& cond)
{
    cond << info_db_identity(path.string());
    throw cond;
}
catch (storage_error& err)
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
    file_.get_segment_manager()->atomic_func(flush_func);
}

void mvcc_mmap_owner::flush_impl()
{
    file_.flush();
    last_flush_timestamp_ = bpt::microsec_clock::local_time();
}

std::size_t mvcc_mmap_owner::get_available_space() const
{
    return reader_handle_.get_available_space();
}

std::size_t mvcc_mmap_owner::get_size() const
{
    return reader_handle_.get_size();
}

} // namespace storage
} // namespace supernova
