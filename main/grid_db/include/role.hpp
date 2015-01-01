#ifndef SUPERNOVA_STORAGE_ROLE_HPP
#define SUPERNOVA_STORAGE_ROLE_HPP

#include <iosfwd>
#include <boost/variant.hpp>

namespace supernova {
namespace storage {

struct owner_t {};

struct reader_t {};

static const owner_t owner = owner_t();

static const reader_t reader = reader_t();

typedef boost::variant<owner_t, reader_t> role;
std::ostream& operator<<(std::ostream& out, const role& source);
std::istream& operator>>(std::istream& in, role& target);

} // namespace storage
} // namespace supernova

#endif
