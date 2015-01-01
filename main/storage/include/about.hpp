#ifndef SUPERNOVA_STORAGE_ABOUT_HPP
#define SUPERNOVA_STORAGE_ABOUT_HPP

#include <ostream>
#include <boost/cstdint.hpp>

namespace supernova {
namespace storage {

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

} // namespace storage
} // namespace supernova

#endif
