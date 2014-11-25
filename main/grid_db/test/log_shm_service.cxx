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
#include <boost/interprocess/shared_memory_object.hpp>
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
#include <simulation_grid/core/signal_notifier.hpp>
#include <zmq.hpp>
#include <signal.h>
#include "exception.hpp"
#include "log_shm.hpp"
#include "log_shm.hxx"
#include "log_service_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpo = boost::program_options;
namespace bsi = boost::signals2;
namespace bsy = boost::system;
namespace sgd = simulation_grid::grid_db;
namespace sco = simulation_grid::core;

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
    bpo::options_description descr("Usage: log_shm_service [options] ipc name");
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

class log_service
{
public:
    log_service(const config& config);
    ~log_service();
    void start();
    void stop();
    bool terminated() const { return service_.stopped(); }
private:
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    void run();
    void receive_instruction(const bsy::error_code& error, size_t);
    void exec_terminate(const sgd::terminate_msg& input, sgd::result& output);
    void exec_append(const sgd::append_msg& input, sgd::result& output);
    sgd::log_shm_owner<sgd::union_AB> owner_;
    bas::io_service service_;
    sco::signal_notifier notifier_;
    sgd::instruction instr_;
    sgd::result result_;
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::posix::stream_descriptor stream_;
};

log_service::log_service(const config& config) :
    owner_(config.name, config.size),
    service_(),
    notifier_(),
    instr_(),
    result_(),
    context_(1),
    socket_(context_, ZMQ_REP),
    stream_(service_, init_zmq_socket(socket_, config))
{
    notifier_.add(SIGTERM, boost::bind(&log_service::stop, this));
    notifier_.add(SIGINT, boost::bind(&log_service::stop, this));
    notifier_.start();
}

log_service::~log_service()
{
    stream_.release();
    socket_.close();
    context_.close();
    notifier_.stop();
    stop();
}

void log_service::start()
{
    if (service_.stopped())
    {
	service_.reset();
    }
    run();
}

void log_service::stop()
{
    service_.stop();
}

int log_service::init_zmq_socket(zmq::socket_t& socket, const config& config)
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

void log_service::run()
{
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&log_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run();
    }
}

void log_service::receive_instruction(const bsy::error_code& error, size_t)
{
    int event = 0;
    size_t size = sizeof(event);
    socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    // More than 1 message may be available, so we need to consume all of them
    while (LIKELY_EXT(!error && size == sizeof(event)) && (event & ZMQ_POLLIN))
    {
	sgd::instruction::msg_status status = instr_.deserialize(socket_);
	if (UNLIKELY_EXT(status == sgd::instruction::MALFORMED))
	{
	    sgd::malformed_message_msg tmp;
	    result_.set_malformed_message_msg(tmp);
	}
	else if (instr_.is_terminate_msg())
	{
	    exec_terminate(instr_.get_terminate_msg(), result_);
	}
	else if (instr_.is_append_msg())
	{
	    exec_append(instr_.get_append_msg(), result_);
	}
	else
	{
	    sgd::malformed_message_msg tmp;
	    result_.set_malformed_message_msg(tmp);
	}
	result_.serialize(socket_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&log_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run_one();
    }
}

void log_service::exec_terminate(const sgd::terminate_msg& input, sgd::result& output)
{
    stop();
    sgd::confirmation_msg tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation_msg(tmp);
}

void log_service::exec_append(const sgd::append_msg& input, sgd::result& output)
{
    sgd::union_AB entry(input.entry());
    boost::optional<sgd::log_index> result = owner_.append(entry);
    if (result)
    {
	sgd::index_msg success;
	success.set_sequence(input.sequence());
	success.set_index(result.get());
	output.set_index_msg(success);
    }
    else
    {
	sgd::failed_op_msg failure;
	failure.set_sequence(input.sequence());
	output.set_failed_op_msg(failure);
    }
}

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
		log_service service(config.get());
		service.start();
	    }
	    bip::shared_memory_object::remove(config.get().name.c_str());
	    break;
	}
	case ipc::mmap:
	{
	    {
		log_service service(config.get());
		service.start();
	    }
	    bfs::remove(config.get().name);
	    break;
	}
    }
    return 0;
}
