#include "signal_notifier.hpp"
#include <boost/asio/placeholders.hpp>

namespace bas = boost::asio;
namespace bsi = boost::signals2;
namespace bsy = boost::system;

namespace simulation_grid {
namespace core {

signal_notifier::signal_notifier() :
    thread_(0), service_(), sigset_(service_)
{ }

signal_notifier::~signal_notifier()
{
    try
    {
        for (std::size_t iter = 0; iter < MAX_SIGNAL; ++iter)
        {
                chanset_[iter].disconnect_all_slots();
        }
        if (!service_.stopped())
        {
            service_.stop();
            if (running())
            {
                thread_->join();
                delete thread_;
            }
        }
    }
    catch (...)
    {
        // do nothing
    }
}

void signal_notifier::add(int signal, const receiver& slot)
{
    std::size_t usignal = static_cast<std::size_t>(signal);
    if (!running() && usignal < MAX_SIGNAL)
    {
        chanset_[usignal].connect(slot);
        sigset_.add(signal);
    }
}

void signal_notifier::reset()
{
    boost::function2<void, const bsy::error_code&, int> handler(
            boost::bind(&signal_notifier::handle, this,
            bas::placeholders::error, bas::placeholders::signal_number));
    sigset_.async_wait(handler);
}

void signal_notifier::start()
{
    if (!running())
    {
        boost::function<void ()> entry(boost::bind(&signal_notifier::run, this));
        thread_ = new boost::thread(entry);
    }
}

void signal_notifier::stop()
{
    service_.stop();
}

void signal_notifier::run()
{
    reset();
    service_.run();
}

void signal_notifier::handle(const bsy::error_code& error, int signal)
{
    std::size_t usignal = static_cast<std::size_t>(signal);
    if (!error && usignal < MAX_SIGNAL)
    {
        chanset_[usignal]();
    }
    reset();
}

} // namespace core
} // namespace simulation_grid
