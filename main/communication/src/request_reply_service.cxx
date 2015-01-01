#include "request_reply_service.hpp"
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/ref.hpp>
#include <supernova/core/compiler_extensions.hpp>

namespace bsy = boost::system;

namespace supernova {
namespace communication {

request_reply_service::request_reply_service(
	const std::string& host,
	boost::uint16_t port,
	std::size_t insize,
	std::size_t outsize) :
    service_(),
    context_(1),
    socket_(context_, ZMQ_REP),
    stream_(service_, init_zmq_socket(socket_, host, port)),
    source_(insize),
    sink_(outsize)
{ }

request_reply_service::~request_reply_service()
{
    stream_.release();
    socket_.close();
    context_.close();
    service_.stop();
}

void request_reply_service::start()
{
    if (service_.stopped())
    {
	service_.reset();
    }
    service_.run();
}

void request_reply_service::stop()
{
    service_.stop();
}

void request_reply_service::submit(const receive_func& receiver)
{
    boost::function2<void, const bsy::error_code&, size_t> handler(
	    boost::bind(&request_reply_service::handle, this, receiver, _1, _2));
    stream_.async_read_some(boost::asio::null_buffers(), handler);
}

int request_reply_service::init_zmq_socket(zmq::socket_t& socket, const std::string& host, boost::uint16_t port)
{
    std::string address(str(boost::format("tcp://%s:%d") % host % port));
    socket.bind(address.c_str());

    // TODO this is currently POSIX specific, add a Windows version
    int fd = 0;
    size_t size = sizeof(fd);
    socket.getsockopt(ZMQ_FD, &fd, &size);
    if (UNLIKELY_EXT(size != sizeof(fd)))
    {
        throw std::runtime_error("Can't find ZeroMQ socket file descriptor");
    }
    else if (UNLIKELY_EXT(fd == 0))
    {
        throw std::runtime_error("Can't find ZeroMQ socket file descriptor");
    }
    return fd;
}

void request_reply_service::handle(receive_func receiver, const boost::system::error_code& error, size_t)
{
    bool received = false;
    int event = 0;
    size_t size = sizeof(event);
    socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    // More than 1 message may be available, so we need to consume all of them
    while (!error.value() && size == sizeof(event) && (event & ZMQ_POLLIN))
    {
	socket_.recv(&source_.buf_);
	receiver(source_, sink_);
	received = true;
	socket_.send(sink_.buf_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
    if (!received)
    {
	// handle false epoll/kqueue events
	submit(receiver);
    }
}

} // namespace communication
} // namespace supernova
