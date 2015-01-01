#ifndef SUPERNOVA_STORAGE_HANDLE_HPP
#define SUPERNOVA_STORAGE_HANDLE_HPP

#include <string>
#include <boost/noncopyable.hpp>

namespace supernova {
namespace storage {

class entry;
class read_handle_impl;
class write_handle_impl;

class read_handle : private boost::noncopyable
{
public:
    explicit read_handle(const std::string& db_id);
    ~read_handle();
    const entry& get_db_entry() const;
private:
    read_handle_impl* impl_;
};

class write_handle : private boost::noncopyable
{
public:
    explicit write_handle(const std::string& db_id);
    ~write_handle();
    entry& get_db_entry() const;
private:
    write_handle_impl* impl_;
};

} // namespace storage
} // namespace supernova

#endif
