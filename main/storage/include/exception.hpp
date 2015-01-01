#ifndef SUPERNOVA_STORAGE_EXCEPTION_HPP
#define SUPERNOVA_STORAGE_EXCEPTION_HPP

#include <stdexcept>
#include <string>
#include <boost/exception/all.hpp>
#include <supernova/storage/about.hpp>

namespace supernova {
namespace storage {

struct tag_db_identity;
typedef boost::error_info<tag_db_identity, std::string> info_db_identity;

struct tag_component_identity;
typedef boost::error_info<tag_component_identity, std::string> info_component_identity;

struct tag_data_identity;
typedef boost::error_info<tag_data_identity, std::string> info_data_identity;

struct storage_condition : virtual boost::exception, virtual std::runtime_error
{
    explicit storage_condition(const std::string& what) : std::runtime_error(what) { }
};

struct busy_condition : public virtual storage_condition
{
    explicit busy_condition(const std::string& what) : std::runtime_error(what), storage_condition(what) { }
};

struct storage_error : virtual boost::exception, virtual std::runtime_error
{
    explicit storage_error(const std::string& what) : std::runtime_error(what) { }
};

struct malformed_db_error : public virtual storage_error
{
    explicit malformed_db_error(const std::string& what) : std::runtime_error(what), storage_error(what) { }
};

struct tag_min_supported_version;
typedef boost::error_info<tag_min_supported_version, version> info_min_supported_version;

struct tag_max_supported_version;
typedef boost::error_info<tag_max_supported_version, version> info_max_supported_version;

struct tag_version_found;
typedef boost::error_info<tag_version_found, version> info_version_found;

struct unsupported_db_error : public virtual storage_error
{
    explicit unsupported_db_error(const std::string& what) : std::runtime_error(what), storage_error(what) { }
};

} // namespace storage
} // namespace supernova

#endif
