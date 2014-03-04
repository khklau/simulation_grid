#ifndef SIMULATION_GRID_GRID_DB_EXCEPTION_HPP
#define SIMULATION_GRID_GRID_DB_EXCEPTION_HPP

#include <stdexcept>
#include <string>
#include <boost/exception/all.hpp>

namespace simulation_grid {
namespace grid_db {

struct tag_db_identity;
typedef boost::error_info<tag_db_identity, std::string> info_db_identity;

struct handle_error : virtual boost::exception, virtual std::runtime_error
{
    explicit handle_error(const std::string& what) : runtime_error(what) { }
};

struct tag_storage_identity;
typedef boost::error_info<tag_storage_identity, std::string> info_storage_identity;

struct storage_error : virtual boost::exception, virtual std::runtime_error
{
    explicit storage_error(const std::string& what) : runtime_error(what) { }
};

struct tag_container_identity;
typedef boost::error_info<tag_container_identity, std::string> info_container_identity;

struct container_error : virtual boost::exception, virtual std::runtime_error
{
    explicit container_error(const std::string& what) : runtime_error(what) { }
};

struct tag_content_identity;
typedef boost::error_info<tag_content_identity, std::string> info_content_identity;

struct content_error : virtual boost::exception, virtual std::runtime_error
{
    explicit content_error(const std::string& what) : runtime_error(what) { }
};

} // namespace grid_db
} // namespace simulation_grid

#endif
