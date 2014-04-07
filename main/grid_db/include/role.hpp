#ifndef SIMULATION_GRID_GRID_DB_ROLE_HPP
#define SIMULATION_GRID_GRID_DB_ROLE_HPP

#include <iosfwd>
#include <boost/variant.hpp>

namespace simulation_grid {
namespace grid_db {

struct owner_t {};

struct reader_t {};

static const owner_t owner = owner_t();

static const reader_t reader = reader_t();

typedef boost::variant<owner_t, reader_t> role;
std::ostream& operator<<(std::ostream& out, const role& source);
std::istream& operator>>(std::istream& in, role& target);

} // namespace grid_db
} // namespace simulation_grid

#endif
