#ifndef SIMULATION_GRID_GRID_DB_ABOUT_HPP
#define SIMULATION_GRID_GRID_DB_ABOUT_HPP

#include <ostream>
#include <boost/cstdint.hpp>

namespace simulation_grid {
namespace grid_db {

struct version
{
    // unfortunately need to use uint32_t because Protocol Buffers
    // doesn't support smaller integrals
    boost::uint32_t num_a;
    boost::uint32_t num_b;
    boost::uint32_t num_c;
    boost::uint32_t num_d;
    version(boost::uint32_t a, boost::uint32_t b, boost::uint32_t c, boost::uint32_t d);
    bool operator==(const version& other) const;
    bool operator<(const version& other) const;
    bool operator>(const version& other) const;
    bool operator<=(const version& other) const;
    bool operator>=(const version& other) const;
    friend std::ostream& operator<<(std::ostream& out, const version& value);
};

static const version DB_VERSION(1, 1, 1, 1);

} // namespace grid_db
} // namespace simulation_grid

#endif
