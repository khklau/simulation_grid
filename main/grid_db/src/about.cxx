#include "about.hpp"

namespace supernova {
namespace storage {

version::version(boost::uint32_t a, boost::uint32_t b, boost::uint32_t c, boost::uint32_t d) :
	num_a(a), num_b(b), num_c(c), num_d(d)
{ }

bool version::operator==(const version& other) const
{
    return this->num_a == other.num_a && this->num_b == other.num_b &&
	    this->num_c == other.num_c && this->num_d == other.num_d;
}

bool version::operator<(const version& other) const
{
    bool result = false;
    if (this->num_a < other.num_a)
    {
	result = true;
    }
    else if (this->num_a == other.num_a)
    {
	if (this->num_b < other.num_b)
	{
	    result = true;
	}
	else if (this->num_b == other.num_b)
	{
	    if (this->num_c < other.num_c)
	    {
		result = true;
	    }
	    else if (this->num_c == other.num_c)
	    {
		result = (this->num_d < other.num_d);
	    }
	}
    }
    return result;
}

bool version::operator>(const version& other) const
{
    bool result = false;
    if (this->num_a > other.num_a)
    {
	result = true;
    }
    else if (this->num_a == other.num_a)
    {
	if (this->num_b > other.num_b)
	{
	    result = true;
	}
	else if (this->num_b == other.num_b)
	{
	    if (this->num_c > other.num_c)
	    {
		result = true;
	    }
	    else if (this->num_c == other.num_c)
	    {
		result = (this->num_d > other.num_d);
	    }
	}
    }
    return result;
}

bool version::operator<=(const version& other) const
{
    return *this < other || *this == other;
}

bool version::operator>=(const version& other) const
{
    return *this > other || *this == other;
}

std::ostream& operator<<(std::ostream& out, const version& value)
{
    out << value.num_a << "." << value.num_b << "." << value.num_c << "." << value.num_d;
    return out;
}

} // namespace storage
} // namespace supernova
