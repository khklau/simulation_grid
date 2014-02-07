#ifndef SIMULATION_GRID_GRID_DB_EXCEPTION_HPP
#define SIMULATION_GRID_GRID_DB_EXCEPTION_HPP

#include <stdexcept>
#include <string>
#include <boost/exception/all.hpp>

namespace simulation_grid {
namespace grid_db {

struct tag_db_id;

typedef boost::error_info<tag_db_id, std::string> info_db_id;

struct handle_error : virtual boost::exception, virtual std::runtime_error
{
    explicit handle_error(const std::string& what) : runtime_error(what) { }
};

} // namespace grid_db
} // namespace simulation_grid

#endif
