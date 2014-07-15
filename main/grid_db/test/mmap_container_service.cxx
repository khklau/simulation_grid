#include <istream>
#include <ostream>
#include <fstream>
#include <string>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <boost/cstdint.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/noncopyable.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <boost/signals2.hpp>
#include <boost/thread/thread.hpp>
#include <google/protobuf/message.h>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <zmq.hpp>
#include <signal.h>
#include "exception.hpp"
#include "mmap_container.hpp"
#include "mmap_container.hxx"
#include "container_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpo = boost::program_options;
namespace bsi = boost::signals2;
namespace bsy = boost::system;
namespace sgd = simulation_grid::grid_db;

namespace {

typedef boost::uint16_t port_t;

static const port_t DEFAULT_PORT = 22220U;
static const size_t DEFAULT_SIZE = 1 << 24;

namespace ipc {

enum type
{
    shm = 0,
    mmap
};

inline std::ostream& operator<<(std::ostream& out, const ipc::type& source)
{
    switch (source)
    {
	case ipc::shm:
	{
	    out << "shm";
	    break;
	}
	case ipc::mmap:
	{
	    out << "mmap";
	    break;
	}
	default: { }
    }
    return out;
}

inline std::istream& operator>>(std::istream& in, ipc::type& target)
{
    if (!in.good())
    {
	return in;
    }
    std::string tmp;
    in >> tmp;
    boost::algorithm::to_lower(tmp);
    std::string shm("shm");
    std::string mmap("mmap");
    if (tmp == shm)
    {
	target = ipc::shm;
    }
    else if (tmp == mmap)
    {
	target = ipc::mmap;
    }
    else
    {
	in.setstate(std::ios::failbit);
    }
    return in;
}

} // namespace ipc

struct config
{
    config() : ipc(ipc::shm), port(DEFAULT_PORT), size(DEFAULT_SIZE) { }
    config(ipc::type ipc_, const std::string& name_, port_t port_ = DEFAULT_PORT,
	    size_t size_ = DEFAULT_SIZE) :
    	ipc(ipc_), name(name_), port(port_), size(size_)
    { }
    ipc::type ipc;
    std::string name;
    port_t port;
    size_t size;
};

typedef boost::optional<config> parse_result;

parse_result parse_cmd_line(const int argc, char* const argv[], std::ostringstream& err_msg)
{
    parse_result result;
    config tmp;
    bpo::options_description descr("Usage: mmap_container_service [options] ipc name");
    descr.add_options()
            ("help,h", "This help text")
            ("port,p", bpo::value<port_t>(&tmp.port)->default_value(DEFAULT_PORT),
                    "Port number to listen on")
            ("size,s", bpo::value<size_t>(&tmp.size)->default_value(DEFAULT_SIZE),
                    "Size of ring buffer in bytes")
            ("ipc,i", bpo::value<ipc::type>(&tmp.ipc)->required(),
                    "IPC method: (shm|mmap)")
            ("name,n", bpo::value<std::string>(&tmp.name)->required(),
                    "Name of the shared memory/mapped file");
    bpo::positional_options_description pos;
    pos.add("ipc", 1);
    pos.add("name", 1);
    bpo::variables_map vm;
    try
    {
        bpo::store(bpo::command_line_parser(argc, argv).options(descr).positional(pos).run(), vm);
        bpo::notify(vm);
    }
    catch (bpo::error& ex)
    {
        err_msg << "ERROR: " << ex.what() << "\n";
        err_msg << descr;
        return result;
    }
    if (vm.count("help"))
    {
        err_msg << descr;
        return result;
    }
    result = tmp;
    return result;
}

class signal_notifier : private boost::noncopyable
{
public:
    typedef bsi::signal<void ()> channel;
    typedef boost::function<void ()> receiver;
    signal_notifier();
    ~signal_notifier();
    bool running() const { return thread_ != 0; }
    void add(int signal, const receiver& slot);
    void reset();
    void start();
    void stop();
private:
    static const size_t MAX_SIGNAL = 64U; // TODO : work out how to base this on SIGRTMAX
    void run();
    void handle(const bsy::error_code& error, int signal);
    boost::thread* thread_;
    bas::io_service service_;
    bas::signal_set sigset_;
    channel chanset_[MAX_SIGNAL];
};

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

template <class element_t>
class container_service
{
public:
    container_service(const config& config);
    ~container_service();
    void start();
    void stop();
    bool terminated() const { return service_.stopped(); }
private:
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    void run();
    void receive_instruction(const bsy::error_code& error, size_t);
    void exec_terminate(const sgd::terminate_instr& input, sgd::result_msg& output);
    void exec_write(const sgd::write_instr& input, sgd::result_msg& output);
    sgd::mvcc_mmap_owner owner_;
    bas::io_service service_;
    signal_notifier notifier_;
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::posix::stream_descriptor stream_;
    sgd::instruction_msg instr_;
    sgd::result_msg result_;
};

template <class element_t>
container_service<element_t>::container_service(const config& config) :
    owner_(bfs::path(config.name.c_str()), config.size),
    service_(),
    notifier_(),
    context_(1),
    socket_(context_, ZMQ_REP),
    stream_(service_, init_zmq_socket(socket_, config)),
    instr_(),
    result_()
{
    notifier_.add(SIGTERM, boost::bind(&container_service::stop, this));
    notifier_.add(SIGINT, boost::bind(&container_service::stop, this));
    notifier_.start();
}

template <class element_t>
container_service<element_t>::~container_service()
{
    stream_.release();
    socket_.close();
    context_.close();
    notifier_.stop();
    stop();
}

template <class element_t>
void container_service<element_t>::start()
{
    if (service_.stopped())
    {
	service_.reset();
    }
    run();
}

template <class element_t>
void container_service<element_t>::stop()
{
    service_.stop();
}

template <class element_t>
int container_service<element_t>::init_zmq_socket(zmq::socket_t& socket, const config& config)
{
    std::string address(str(boost::format("tcp://127.0.0.1:%d") % config.port));
    socket.bind(address.c_str());

    // TODO this is currently POSIX specific, add a Windows version
    int fd = 0;
    size_t size = sizeof(fd);
    socket.getsockopt(ZMQ_FD, &fd, &size);
    if (UNLIKELY_EXT(size != sizeof(fd)))
    {
	throw std::runtime_error("Can't find ZeroMQ socket file descriptor");
    }
    return fd;
}

template <class element_t>
void container_service<element_t>::run()
{
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&container_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run();
    }
}

template <class element_t>
void container_service<element_t>::receive_instruction(const bsy::error_code& error, size_t)
{
    int event = 0;
    size_t size = sizeof(event);
    socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    // More than 1 message may be available, so we need to consume all of them
    while (LIKELY_EXT(!error && size == sizeof(event)) && (event & ZMQ_POLLIN))
    {
	sgd::instruction_msg::msg_status status = instr_.deserialize(socket_);
	if (UNLIKELY_EXT(status == sgd::instruction_msg::MALFORMED))
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	else if (instr_.is_terminate())
	{
	    exec_terminate(instr_.get_terminate(), result_);
	}
	else if (instr_.is_write())
	{
	    exec_write(instr_.get_write(), result_);
	}
	else
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	result_.serialize(socket_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&container_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run_one();
    }
}

template <class element_t>
void container_service<element_t>::exec_terminate(const sgd::terminate_instr& input, sgd::result_msg& output)
{
    stop();
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation(tmp);
}

template <class element_t>
void container_service<element_t>::exec_write(const sgd::write_instr& input, sgd::result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    owner_.write(input.key().c_str(), input.value());
    output.set_confirmation(tmp);
}

typedef container_service<boost::int32_t> shm_container_service;
typedef container_service<boost::int32_t> mmap_container_service;

} // anonymous namespace

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::ostringstream err;
    parse_result config = parse_cmd_line(argc, argv, err);
    if (!config)
    {
	std::cerr << err.str() << std::endl;
	return 1;
    }
    switch (config.get().ipc)
    {
	case ipc::shm:
	{
	    {
		shm_container_service service(config.get());
		service.start();
	    }
	    bip::shared_memory_object::remove(config.get().name.c_str());
	    break;
	}
	case ipc::mmap:
	{
	    {
		mmap_container_service service(config.get());
		service.start();
	    }
	    bfs::remove(config.get().name);
	    break;
	}
    }
    return 0;
}
