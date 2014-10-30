#include <iostream>
#include <exception>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/array.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/atomic.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/thread_time.hpp>
#include <gtest/gtest.h>
#include <zmq.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/core/process_utility.hpp>
#include <simulation_grid/core/tcpip_utility.hpp>
#include "exception.hpp"
#include "log_mmap.hpp"
#include "log_mmap.hxx"
#include "log_service_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bpt = boost::posix_time;
namespace scp = simulation_grid::core::process_utility;
namespace sct = simulation_grid::core::tcpip_utility;
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

class service_client
{
public:
    service_client(const config& config);
    ~service_client();
    sgd::result send(sgd::instruction& instr);
    void send_terminate_msg(boost::uint32_t sequence);
    sgd::log_index send_append_msg(boost::uint32_t sequence, const sgd::union_AB& entry);
private:
    bool terminate_sent_;
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::io_service service_;
    bas::posix::stream_descriptor stream_;
};

service_client::service_client(const config& config) :
    terminate_sent_(false),
    context_(1),
    socket_(context_, ZMQ_REQ),
    service_(),
    stream_(service_, init_zmq_socket(socket_, config))
{ }

service_client::~service_client()
{
    if (!terminate_sent_)
    {
	send_terminate_msg(99U);
    }
    stream_.release();
    service_.stop();
    socket_.close();
    context_.close();
}

int service_client::init_zmq_socket(zmq::socket_t& socket, const config& config)
{
    std::string address(str(boost::format("tcp://127.0.0.1:%d") % config.port));
    socket.connect(address.c_str());

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

sgd::result service_client::send(sgd::instruction& instr)
{
    instr.serialize(socket_);
    int event = 0;
    size_t size = sizeof(event);
    boost::system::error_code error;
    sgd::result result;
    do
    {
	//stream_.read_some(boost::asio::null_buffers(), error);
	if (UNLIKELY_EXT(error))
	{
	    throw std::runtime_error("Unexpected connection close");
	}
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
	if (UNLIKELY_EXT(size != sizeof(event)))
	{
	    throw std::runtime_error("Unable to read socket options");
	}
    } while (!(event & ZMQ_POLLIN));
    // Finally received a whole message
    sgd::result::msg_status status = result.deserialize(socket_);
    if (UNLIKELY_EXT(status == sgd::result::MALFORMED))
    {
	throw std::runtime_error("Received malformed message");
    }
    return result;
}

void service_client::send_terminate_msg(boost::uint32_t sequence)
{
    sgd::instruction instr;
    sgd::terminate_msg msg;
    msg.set_sequence(sequence);
    instr.set_terminate_msg(msg);
    sgd::result outmsg(send(instr));
    EXPECT_TRUE(outmsg.is_confirmation_msg()) << "Unexpected terminate result";
    EXPECT_EQ(instr.get_terminate_msg().sequence(), outmsg.get_confirmation_msg().sequence()) << "Sequence number mismatch";
    terminate_sent_ = true;
}

sgd::log_index service_client::send_append_msg(boost::uint32_t sequence, const sgd::union_AB& entry)
{
    sgd::instruction instr;
    sgd::append_msg append;
    sgd::union_AB_msg union_msg;
    entry.export_to(union_msg);
    append.set_sequence(sequence);
    entry.export_to(*append.mutable_entry());
    instr.set_append_msg(append);
    sgd::result outmsg(send(instr));
    EXPECT_TRUE(outmsg.is_index_msg()) << "unexpected write result";
    EXPECT_EQ(instr.get_append_msg().sequence(), outmsg.get_index_msg().sequence()) << "sequence number mismatch";
    return outmsg.get_index_msg().index();
}

class service_launcher
{
public:
    service_launcher(const config& config);
    ~service_launcher();
    int wait();
private:
    bool has_terminated;
    pid_t pid_;
};

service_launcher::service_launcher(const config& config) :
    has_terminated(false)
{
    static const char SLAVE_NAME[] = "log_mmap_service";
    boost::array <char, sizeof(SLAVE_NAME)> launcher_name;
    strncpy(launcher_name.c_array(), SLAVE_NAME, launcher_name.max_size());

    static const char PORT_OPT[] = "--port";
    boost::array<char, sizeof(PORT_OPT)> port_opt;
    strncpy(port_opt.c_array(), PORT_OPT, port_opt.max_size());

    boost::array<char, std::numeric_limits<port_t>::digits> port_arg;
    std::ostringstream port_buf;
    port_buf << config.port;
    strncpy(port_arg.c_array(), port_buf.str().c_str(), port_arg.max_size());

    static const char SIZE_OPT[] = "--size";
    boost::array<char, sizeof(SIZE_OPT)> size_opt;
    strncpy(size_opt.c_array(), SIZE_OPT, size_opt.max_size());

    boost::array<char, std::numeric_limits<size_t>::digits> size_arg;
    std::ostringstream size_buf;
    size_buf << config.size;
    strncpy(size_arg.c_array(), size_buf.str().c_str(), size_arg.max_size());

    // TODO: calculate the array size properly with constexpr after moving to C++11
    boost::array<char, 256> ipc_arg;
    std::ostringstream ipc_buf;
    ipc_buf << config.ipc;
    strncpy(ipc_arg.c_array(), ipc_buf.str().c_str(), ipc_arg.max_size());

    std::vector<char> name_arg(config.name.size() + 1, '\0');
    std::copy(config.name.begin(), config.name.end(), name_arg.begin());

    boost::array<char*, 9> arg_list = boost::assign::list_of
    		(launcher_name.c_array())
		(port_opt.c_array())(port_arg.c_array())
		(size_opt.c_array())(size_arg.c_array())
		(ipc_arg.c_array())
		(&name_arg[0])
		(0);
    bfs::path launcher_path = scp::current_exe_path().parent_path() / bfs::path(SLAVE_NAME);
    // stupid arg_list argument has to be an array of mutable C strings
    if (posix_spawn(&pid_, launcher_path.string().c_str(), 0, 0, arg_list.c_array(), environ))
    {
	throw std::runtime_error("Could not spawn service_launcher");
    }
    bfs::path ipcpath(config.name);
    if (config.ipc == ipc::shm)
    {
	ipcpath = bfs::path("/dev/shm") / bfs::path(config.name);
    }
    while (!sct::is_tcp_port_open("127.0.0.1", config.port))
    {
	boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
    }
}

service_launcher::~service_launcher()
{
    if (!has_terminated)
    {
	wait();
    }
}

int service_launcher::wait()
{
    int exit_code = -1;
    int status = 0;
    if (waitpid(pid_, &status, 0) == pid_)
    {
	if (WIFEXITED(status))
	{
	    exit_code = WEXITSTATUS(status);
	}
	has_terminated = true;
    }
    return exit_code;
}

} // anonymous namespace

TEST(log_mmap_test, startup_and_shutdown_benchmark)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    client.send_terminate_msg(1U);
}
