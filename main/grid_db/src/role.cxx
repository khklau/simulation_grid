#include <istream>
#include <ostream>
#include <string>
#include <boost/algorithm/string/case_conv.hpp>
#include "role.hpp"

namespace {

using namespace simulation_grid::grid_db;

class out_streamer : public boost::static_visitor<>
{
public:
    out_streamer(std::ostream& stream) : stream_(stream) { }
    void operator()(const owner_t&) const
    {
	stream_ << "owner";
    }
    void operator()(const reader_t&) const
    {
	stream_ << "reader";
    }
private:
    std::ostream& stream_;
};

} // anonymous namespace

namespace simulation_grid {
namespace grid_db {

std::ostream& operator<<(std::ostream& out, const role& source)
{
    boost::apply_visitor(out_streamer(out), source);
    return out;
}

std::istream& operator>>(std::istream& in, role& target)
{
    if (!in.good())
    {
        return in;
    }
    std::string tmp;
    in >> tmp;
    boost::algorithm::to_lower(tmp);
    std::string owner("owner");
    std::string reader("reader");
    if (tmp == owner)
    {
        target = owner_t();
    }
    else if (tmp == reader)
    {
        target = reader_t();
    }
    else
    {
        in.setstate(std::ios::failbit);
    }
    return in;
}

} // namespace grid_db
} // namespace simulation_grid
