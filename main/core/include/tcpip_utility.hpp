#ifndef SIMULATION_GRID_CORE_TCPIP_UTILITY_HPP
#define SIMULATION_GRID_CORE_TCPIP_UTILITY_HPP

#include <boost/cstdint.hpp>

namespace simulation_grid {
namespace core {
namespace tcpip_utility {

namespace protocol
{
    enum enumeration
    {
	tcpv4,
	udpv4,
	tcpv6,
	udpv6
    };
}

bool is_port_open(protocol::enumeration protocol, const char* hostname, boost::uint32_t port);

} // namespace tcpip_utility
} // namespace core
} // namespace simulation_grid

#endif
