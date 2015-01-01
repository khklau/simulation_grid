#ifndef SUPERNOVA_CORE_SIGNAL_NOTIFIER_HPP
#define SUPERNOVA_CORE_SIGNAL_NOTIFIER_HPP

#include <boost/asio/io_service.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>
#include <boost/system/error_code.hpp>
#include <boost/thread/thread.hpp>

namespace supernova {
namespace core {

class signal_notifier : private boost::noncopyable
{
public:
    typedef boost::signals2::signal<void ()> channel;
    typedef boost::function<void ()> receiver;
    signal_notifier();
    ~signal_notifier();
    bool running() const { return thread_ != 0; }
    void add(int signal, const receiver& slot);
    void reset();
    void start();
    void stop();
private:
    static const size_t MAX_SIGNAL = 64U; // TODO : work out how to boost::asioe this on SIGRTMAX
    void run();
    void handle(const boost::system::error_code& error, int signal);
    boost::thread* thread_;
    boost::asio::io_service service_;
    boost::asio::signal_set sigset_;
    channel chanset_[MAX_SIGNAL];
};

} // namespace core
} // namespace supernova

#endif
