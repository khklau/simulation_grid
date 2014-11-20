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
#include <boost/optional.hpp>
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
static const size_t DEFAULT_SIZE = 1 << 10;

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
    boost::optional<sgd::log_index> send_append_msg(boost::uint32_t sequence, const sgd::union_AB& entry);
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

boost::optional<sgd::log_index> service_client::send_append_msg(boost::uint32_t sequence, const sgd::union_AB& entry)
{
    sgd::instruction instr;
    sgd::append_msg append;
    sgd::union_AB_msg union_msg;
    entry.export_to(union_msg);
    append.set_sequence(sequence);
    entry.export_to(*append.mutable_entry());
    instr.set_append_msg(append);
    sgd::result outmsg(send(instr));
    boost::optional<sgd::log_index> output;
    if (outmsg.is_index_msg())
    {
	EXPECT_EQ(instr.get_append_msg().sequence(), outmsg.get_index_msg().sequence()) << "sequence number mismatch";
	output = outmsg.get_index_msg().index();
    }
    else if (outmsg.is_failed_op_msg())
    {
	EXPECT_EQ(instr.get_append_msg().sequence(), outmsg.get_failed_op_msg().sequence()) << "sequence number mismatch";
    }
    return output;
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

TEST(log_mmap_test, atomic_tail_index)
{
    boost::atomic<sgd::log_index> tmp;
    ASSERT_TRUE(tmp.is_lock_free()) << "log_index is not atomic";
}

TEST(log_mmap_test, empty_log)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::log_mmap_reader<sgd::union_AB> reader(bfs::path(conf.name.c_str()));
    EXPECT_FALSE(reader.get_front_index()) << "front index is defined for an empty log";
    EXPECT_FALSE(reader.get_back_index()) << "back index is defined for an empty log";
    client.send_terminate_msg(1U);
}

TEST(log_mmap_test, fill_log)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::log_mmap_reader<sgd::union_AB> reader1(bfs::path(conf.name.c_str()));
    sgd::log_mmap_reader<sgd::union_AB> reader2(bfs::path(conf.name.c_str()));

    sgd::struct_A A1("foo", "bar");
    sgd::union_AB U1(A1);
    boost::optional<sgd::log_index> index1 = client.send_append_msg(10U, U1);
    ASSERT_TRUE(index1) << "append failed";
    EXPECT_NE(index1.get(), reader1.get_max_index()) << "log is prematurely full";
    EXPECT_TRUE(reader1.get_front_index()) << "front index is not defined";
    EXPECT_TRUE(reader2.get_back_index()) << "back index is not defined";
    EXPECT_TRUE(reader1.read(index1.get())) << "cannot read entry";
    EXPECT_EQ(U1, reader2.read(index1.get()).get()) << "entry read does not match entry just appended";

    sgd::struct_B B2("blah", true, 52, 3.8);
    sgd::union_AB U2(B2);
    boost::optional<sgd::log_index> index2 = client.send_append_msg(20U, U2);
    EXPECT_TRUE(reader1.get_front_index()) << "front index is not defined";
    EXPECT_TRUE(reader2.get_back_index()) << "back index is not defined";
    ASSERT_TRUE(index2) << "append failed";
    EXPECT_NE(index2.get(), reader2.get_max_index()) << "log is prematurely full";
    EXPECT_NE(index1.get(), index2.get()) << "index from appending is not unique";
    EXPECT_TRUE(index1.get() < index2.get()) << "index of first append is not less than second append";
    EXPECT_TRUE(reader1.read(index2.get())) << "cannot read entry";
    EXPECT_EQ(U2, reader2.read(index2.get()).get()) << "entry read does not match entry just appended";

    for (boost::optional<sgd::log_index> index = index2.get() + 1; index && index.get() <= reader1.get_max_index(); ++(index.get()))
    {
	sgd::struct_B fillerB("blah", true, index.get(), 1.0 * index.get());
	sgd::union_AB fillerU(fillerB);
	index = client.send_append_msg(30U + index.get(), fillerU);
	EXPECT_TRUE(reader1.read(index.get())) << "cannot read entry";
	EXPECT_EQ(fillerU, reader2.read(index.get()).get()) << "entry read does not match entry just appended";
    }
    EXPECT_EQ(reader1.get_back_index(), reader2.get_max_index()) << "in a full log the back index != max index";

    client.send_terminate_msg(40U);
}

TEST(log_mmap_test, forward_iterate)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::log_mmap_reader<sgd::union_AB> reader(bfs::path(conf.name.c_str()));

    sgd::struct_A A1("foo", "bar");
    sgd::union_AB U1(A1);
    client.send_append_msg(10U, U1);
    sgd::struct_B B2("wah", true, 52, 3.8);
    sgd::union_AB U2(B2);
    client.send_append_msg(11U, U2);
    sgd::struct_A A3("blah", "blob");
    sgd::union_AB U3(A3);
    client.send_append_msg(12U, U3);
    sgd::struct_B B4("woot", false, 106, 21.7);
    sgd::union_AB U4(B4);
    client.send_append_msg(13U, U4);

    boost::optional<sgd::log_index> iter = reader.get_front_index();
    ASSERT_TRUE(iter) << "front index is not defined";
    EXPECT_EQ(U1, reader.read(iter.get()).get()) << "read failed";
    ++(iter.get());
    EXPECT_EQ(U2, reader.read(iter.get()).get()) << "read failed";
    ++(iter.get());
    EXPECT_EQ(U3, reader.read(iter.get()).get()) << "read failed";
    ++(iter.get());
    EXPECT_EQ(U4, reader.read(iter.get()).get()) << "read failed";
    EXPECT_EQ(reader.get_back_index().get(), iter.get()) << "back of log was not reached";
    ++(iter.get());
    EXPECT_FALSE(reader.read(iter.get())) << "read beyond back of log was allowed";

    client.send_terminate_msg(20U);
}

TEST(log_mmap_test, backward_iterate)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::log_mmap_reader<sgd::union_AB> reader(bfs::path(conf.name.c_str()));

    sgd::struct_A A1("foo", "bar");
    sgd::union_AB U1(A1);
    client.send_append_msg(10U, U1);
    sgd::struct_B B2("wah", true, 52, 3.8);
    sgd::union_AB U2(B2);
    client.send_append_msg(11U, U2);
    sgd::struct_A A3("blah", "blob");
    sgd::union_AB U3(A3);
    client.send_append_msg(12U, U3);
    sgd::struct_B B4("woot", false, 106, 21.7);
    sgd::union_AB U4(B4);
    client.send_append_msg(13U, U4);

    boost::optional<sgd::log_index> iter = reader.get_back_index();
    ASSERT_TRUE(iter) << "back index is not defined";
    EXPECT_EQ(U4, reader.read(iter.get()).get()) << "read failed";
    --(iter.get());
    EXPECT_EQ(U3, reader.read(iter.get()).get()) << "read failed";
    --(iter.get());
    EXPECT_EQ(U2, reader.read(iter.get()).get()) << "read failed";
    --(iter.get());
    EXPECT_EQ(U1, reader.read(iter.get()).get()) << "read failed";
    EXPECT_EQ(reader.get_front_index().get(), iter.get()) << "front of log was not reached";
    --(iter.get());
    EXPECT_FALSE(reader.read(iter.get())) << "read beyond front of log was allowed";

    client.send_terminate_msg(20U);
}
