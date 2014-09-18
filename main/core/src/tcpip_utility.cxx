#include <boost/asio/connect.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/lexical_cast.hpp>
#include "tcpip_utility.hpp"

namespace simulation_grid {
namespace core {
namespace tcpip_utility {

namespace bas = boost::asio;

bool is_port_open(protocol::enumeration protocol, const char* hostname, boost::uint32_t port)
{
    bas::ip::tcp::resolver::query query(hostname, boost::lexical_cast<std::string>(port));
    bas::io_service service;
    bas::ip::tcp::resolver resolver(service);
    bas::ip::tcp::socket socket(service);
    bool result = false;

    try
    {
	boost::system::error_code resolve_err;
	bas::ip::tcp::resolver::iterator iter = resolver.resolve(query, resolve_err);
	bas::ip::tcp::resolver::iterator end;
	boost::system::error_code connect_err;
	if (!resolve_err)
	{
	    bas::ip::tcp::resolver::iterator connection = bas::connect(socket, iter, end, connect_err);
	    result = (connection != end && !connect_err);
	}
    }
    catch(...)
    {
	// Do nothing
    }
    if (socket.is_open())
    {
	boost::system::error_code close_err;
	socket.close(close_err);
    }
    return result;
}

} // namespace tcpip_utility
} // namespace core
} // namespace simulation_grid
