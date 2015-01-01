#ifndef SUPERNOVA_CORE_TCPIP_UTILITY_HPP
#define SUPERNOVA_CORE_TCPIP_UTILITY_HPP

#include <boost/cstdint.hpp>

namespace supernova {
namespace core {
namespace tcpip_utility {

bool is_tcp_port_open(const char* hostname, boost::uint32_t port);

bool is_udp_port_open(const char* hostname, boost::uint32_t port);

} // namespace tcpip_utility
} // namespace core
} // namespace supernova

#endif
