#include "request_reply_client.hpp"
#include <boost/format.hpp>
#include <supernova/core/compiler_extensions.hpp>

namespace supernova {
namespace communication {

request_reply_client::request_reply_client(const std::string& host, boost::uint16_t port) :
	context_(1),
	socket_(context_, ZMQ_REQ)
{
    socket_.connect(boost::str(boost::format("tcp://%s:%d") % host % port).c_str());
}

request_reply_client::~request_reply_client()
{
    socket_.close();
    context_.close();
}

void request_reply_client::send(sink& sink, source& source)
{
    socket_.send(sink.buf_);
    int event = 0;
    size_t size = sizeof(event);
    do
    {
        socket_.getsockopt(ZMQ_EVENTS, &event, &size);
        if (UNLIKELY_EXT(size != sizeof(event)))
        {
            throw std::runtime_error("Unable to read socket options");
        }
    } while (!(event & ZMQ_POLLIN));
    // Finally received a whole message
    socket_.recv(&source.buf_);
}

} // namespace communication
} // namespace supernova
