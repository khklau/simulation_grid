#ifndef SUPERNOVA_COMMUNICATION_REQUEST_REPLY_CLIENT_HPP
#define SUPERNOVA_COMMUNICATION_REQUEST_REPLY_CLIENT_HPP

#include <string>
#include <boost/cstdint.hpp>
#include <boost/noncopyable.hpp>
#include <zmq.hpp>
#include <supernova/communication/message_access.hpp>

namespace supernova {
namespace communication {

class request_reply_client : public boost::noncopyable
{
public:
    class source : public message_source, boost::noncopyable
    {
    public:
	source(std::size_t initial_size) : message_source(initial_size) { }
    private:
	friend class request_reply_client;
    };
    class sink : public message_sink, boost::noncopyable
    {
    public:
	sink(std::size_t initial_size) : message_sink(initial_size) { }
    private:
	friend class request_reply_client;
    };
    request_reply_client(const std::string& server, boost::uint16_t port);
    ~request_reply_client();
    void send(sink& sink, source& source);
private:
    zmq::context_t context_;
    zmq::socket_t socket_;
};

} // namespace communication
} // namespace supernova

#endif
