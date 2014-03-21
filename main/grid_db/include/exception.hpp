#ifndef SIMULATION_GRID_GRID_DB_EXCEPTION_HPP
#define SIMULATION_GRID_GRID_DB_EXCEPTION_HPP

#include <stdexcept>
#include <string>
#include <boost/exception/all.hpp>
#include <simulation_grid/grid_db/about.hpp>

namespace simulation_grid {
namespace grid_db {

struct tag_db_identity;
typedef boost::error_info<tag_db_identity, std::string> info_db_identity;

struct tag_component_identity;
typedef boost::error_info<tag_component_identity, std::string> info_component_identity;

struct tag_data_identity;
typedef boost::error_info<tag_data_identity, std::string> info_data_identity;

struct grid_db_condition : virtual boost::exception, virtual std::runtime_error
{
    explicit grid_db_condition(const std::string& what) : std::runtime_error(what) { }
};

struct busy_condition : public virtual grid_db_condition
{
    explicit busy_condition(const std::string& what) : std::runtime_error(what), grid_db_condition(what) { }
};

struct grid_db_error : virtual boost::exception, virtual std::runtime_error
{
    explicit grid_db_error(const std::string& what) : std::runtime_error(what) { }
};

struct malformed_db_error : public virtual grid_db_error
{
    explicit malformed_db_error(const std::string& what) : std::runtime_error(what), grid_db_error(what) { }
};

struct tag_min_supported_version;
typedef boost::error_info<tag_min_supported_version, version> info_min_supported_version;

struct tag_max_supported_version;
typedef boost::error_info<tag_max_supported_version, version> info_max_supported_version;

struct tag_version_found;
typedef boost::error_info<tag_version_found, version> info_version_found;

struct unsupported_db_error : public virtual grid_db_error
{
    explicit unsupported_db_error(const std::string& what) : std::runtime_error(what), grid_db_error(what) { }
};

} // namespace grid_db
} // namespace simulation_grid

#endif
