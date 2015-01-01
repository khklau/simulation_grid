#ifndef SUPERNOVA_COMMUNICATION_REQUEST_REPLY_SERVICE_HPP
#define SUPERNOVA_COMMUNICATION_REQUEST_REPLY_SERVICE_HPP

#include <string>
#include <queue>
#include <boost/cstdint.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/system/system_error.hpp>
#include <zmq.hpp>
#include <supernova/communication/message_access.hpp>

namespace supernova {
namespace communication {

class request_reply_service : public boost::noncopyable
{
public:
    class source : public message_source, boost::noncopyable
    {
    public:
	source(std::size_t initial_size) : message_source(initial_size) { }
    private:
	friend class request_reply_service;
    };
    class sink : public message_sink, boost::noncopyable
    {
    public:
	sink(std::size_t initial_size) : message_sink(initial_size) { }
    private:
	friend class request_reply_service;
    };
    typedef boost::function<void (const source&, sink&)> receive_func;
    request_reply_service(const std::string& host, boost::uint16_t port, std::size_t insize, std::size_t outsize);
    ~request_reply_service();
    void start();
    void stop();
    void submit(const receive_func& receiver);
    inline bool has_stopped() const { return service_.stopped(); }
    inline void reset() { return service_.reset(); }
private:
    static int init_zmq_socket(zmq::socket_t& socket, const std::string& host, boost::uint16_t port);
    void handle(receive_func receiver, const boost::system::error_code& error, size_t);
    boost::asio::io_service service_;
    zmq::context_t context_;
    zmq::socket_t socket_;
    boost::asio::posix::stream_descriptor stream_;
    source source_;
    sink sink_;
};

} // namespace communication
} // namespace supernova

#endif
