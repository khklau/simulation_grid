#include <iostream>
#include <exception>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string>
#include <vector>
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
#include "mvcc_mmap.hpp"
#include "mvcc_mmap.hxx"
#include "mvcc_service_msg.hpp"

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
    sgd::result_msg send(sgd::instruction_msg& msg);
    void send_terminate(boost::uint32_t sequence);
    void send_write_string(boost::uint32_t sequence, const char* key, const sgd::string_value& value);
    void send_write_struct(boost::uint32_t sequence, const char* key, const sgd::struct_value& value);
    void send_remove_string(boost::uint32_t sequence, const char* key);
    void send_remove_struct(boost::uint32_t sequence, const char* key);
    void send_process_read_metadata(boost::uint32_t sequence, sgd::reader_token_id from = 0, sgd::reader_token_id to = sgd::MVCC_READER_LIMIT);
    void send_process_write_metadata(boost::uint32_t sequence, std::size_t max_attempts = 0);
    std::string send_collect_garbage(boost::uint32_t sequence, std::size_t max_attempts = 0);
    std::string send_collect_garbage(boost::uint32_t sequence, const std::string& from, std::size_t max_attempts = 0);
    boost::uint64_t send_get_global_oldest_revision_read(boost::uint32_t sequence);
    std::vector<std::string> send_get_registered_keys(boost::uint32_t sequence);
    std::size_t send_get_string_history_depth(boost::uint32_t sequence, const char* key);
    std::size_t send_get_struct_history_depth(boost::uint32_t sequence, const char* key);
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
	send_terminate(99U);
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

sgd::result_msg service_client::send(sgd::instruction_msg& msg)
{
    msg.serialize(socket_);
    int event = 0;
    size_t size = sizeof(event);
    boost::system::error_code error;
    sgd::result_msg result;
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
    sgd::result_msg::msg_status status = result.deserialize(socket_);
    if (UNLIKELY_EXT(status == sgd::result_msg::MALFORMED))
    {
	throw std::runtime_error("Received malformed message");
    }
    return result;
}

void service_client::send_terminate(boost::uint32_t sequence)
{
    sgd::instruction_msg inmsg;
    sgd::terminate_instr instr;
    instr.set_sequence(sequence);
    inmsg.set_terminate(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "Unexpected terminate result";
    EXPECT_EQ(inmsg.get_terminate().sequence(), outmsg.get_confirmation().sequence()) << "Sequence number mismatch";
    terminate_sent_ = true;
}

void service_client::send_write_string(boost::uint32_t sequence, const char* key, const sgd::string_value& value)
{
    sgd::instruction_msg inmsg;
    sgd::write_string_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    instr.set_value(value.c_str);
    inmsg.set_write_string(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected write result";
    EXPECT_EQ(inmsg.get_write_string().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

void service_client::send_write_struct(boost::uint32_t sequence, const char* key, const sgd::struct_value& value)
{
    sgd::instruction_msg inmsg;
    sgd::write_struct_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    instr.set_value1(value.value1);
    instr.set_value2(value.value2);
    instr.set_value3(value.value3);
    inmsg.set_write_struct(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected write result";
    EXPECT_EQ(inmsg.get_write_struct().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

void service_client::send_remove_string(boost::uint32_t sequence, const char* key)
{
    sgd::instruction_msg inmsg;
    sgd::remove_string_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    inmsg.set_remove_string(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected remove result";
    EXPECT_EQ(inmsg.get_remove_string().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

void service_client::send_remove_struct(boost::uint32_t sequence, const char* key)
{
    sgd::instruction_msg inmsg;
    sgd::remove_struct_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    inmsg.set_remove_struct(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected remove result";
    EXPECT_EQ(inmsg.get_remove_struct().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

void service_client::send_process_read_metadata(boost::uint32_t sequence, sgd::reader_token_id from, sgd::reader_token_id to)
{
    sgd::instruction_msg inmsg;
    sgd::process_read_metadata_instr instr;
    instr.set_sequence(sequence);
    instr.set_from(from);
    instr.set_to(to);
    inmsg.set_process_read_metadata(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected process_read_metadata result";
    EXPECT_EQ(inmsg.get_process_read_metadata().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

void service_client::send_process_write_metadata(boost::uint32_t sequence, std::size_t max_attempts)
{
    sgd::instruction_msg inmsg;
    sgd::process_write_metadata_instr instr;
    instr.set_sequence(sequence);
    instr.set_max_attempts(max_attempts);
    inmsg.set_process_write_metadata(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "unexpected process_write_metadata result";
    EXPECT_EQ(inmsg.get_process_write_metadata().sequence(), outmsg.get_confirmation().sequence()) << "sequence number mismatch";
}

std::string service_client::send_collect_garbage(boost::uint32_t sequence, std::size_t max_attempts)
{
    sgd::instruction_msg inmsg;
    sgd::collect_garbage_1_instr instr;
    instr.set_sequence(sequence);
    instr.set_max_attempts(max_attempts);
    inmsg.set_collect_garbage_1(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_key()) << "unexpected collect_garbage_1 result";
    EXPECT_EQ(inmsg.get_collect_garbage_1().sequence(), outmsg.get_key().sequence()) << "sequence number mismatch";
    return outmsg.get_key().key();
}

std::string service_client::send_collect_garbage(boost::uint32_t sequence, const std::string& from, std::size_t max_attempts)
{
    sgd::instruction_msg inmsg;
    sgd::collect_garbage_2_instr instr;
    instr.set_sequence(sequence);
    instr.set_from(from);
    instr.set_max_attempts(max_attempts);
    inmsg.set_collect_garbage_2(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_key()) << "unexpected collect_garbage_1 result";
    EXPECT_EQ(inmsg.get_collect_garbage_2().sequence(), outmsg.get_key().sequence()) << "sequence number mismatch";
    return outmsg.get_key().key();
}

boost::uint64_t service_client::send_get_global_oldest_revision_read(boost::uint32_t sequence)
{
    sgd::instruction_msg inmsg;
    sgd::get_global_oldest_revision_read_instr instr;
    instr.set_sequence(sequence);
    inmsg.set_get_global_oldest_revision_read(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_revision()) << "unexpected get_global_oldest_revision_read result";
    EXPECT_EQ(inmsg.get_get_global_oldest_revision_read().sequence(), outmsg.get_revision().sequence()) << "sequence number mismatch";
    return outmsg.get_revision().revision();
}

std::vector<std::string> service_client::send_get_registered_keys(boost::uint32_t sequence)
{
    sgd::instruction_msg inmsg;
    sgd::get_registered_keys_instr instr;
    instr.set_sequence(sequence);
    inmsg.set_get_registered_keys(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_key_list()) << "unexpected get_registered_keys result";
    EXPECT_EQ(inmsg.get_get_registered_keys().sequence(), outmsg.get_key_list().sequence()) << "sequence number mismatch";
    std::vector<std::string> result;
    for (int iter = 0; iter < outmsg.get_key_list().key_list().size(); ++iter)
    {
	result.push_back(outmsg.get_key_list().key_list().Get(iter));
    }
    return result;
}

std::size_t service_client::send_get_string_history_depth(boost::uint32_t sequence, const char* key)
{
    sgd::instruction_msg inmsg;
    sgd::get_string_history_depth_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    inmsg.set_get_string_history_depth(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_size()) << "unexpected get_string_history_depth result";
    EXPECT_EQ(inmsg.get_get_string_history_depth().sequence(), outmsg.get_size().sequence()) << "sequence number mismatch";
    return outmsg.get_size().size();
}

std::size_t service_client::send_get_struct_history_depth(boost::uint32_t sequence, const char* key)
{
    sgd::instruction_msg inmsg;
    sgd::get_struct_history_depth_instr instr;
    instr.set_sequence(sequence);
    instr.set_key(key);
    inmsg.set_get_struct_history_depth(instr);
    sgd::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_size()) << "unexpected get_struct_history_depth result";
    EXPECT_EQ(inmsg.get_get_struct_history_depth().sequence(), outmsg.get_size().sequence()) << "sequence number mismatch";
    return outmsg.get_size().size();
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
    static const char SLAVE_NAME[] = "mvcc_mmap_service";
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

TEST(mvcc_mmap_test, startup_and_shutdown_benchmark)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    client.send_terminate(1U);
}

TEST(mvcc_mmap_test, access_historical)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    const char* key = "access_historical";

    sgd::string_value expected1("11");
    client.send_write_string(1U, key, expected1);
    ASSERT_TRUE(readerA.exists<sgd::string_value>(key)) << "write failed";
    const boost::optional<const sgd::string_value&> actual1 = readerA.read<sgd::string_value>(key);
    EXPECT_TRUE(actual1) << "read failed";
    EXPECT_EQ(expected1, actual1.get()) << "read value is not the value just written";

    sgd::string_value expected2("22");
    client.send_write_string(2U, key, expected2);
    ASSERT_TRUE(readerA.exists<sgd::string_value>(key)) << "write failed";
    const boost::optional<const sgd::string_value&> actual2 = readerA.read<sgd::string_value>(key);
    EXPECT_TRUE(actual2) << "read failed";
    EXPECT_EQ(expected2, actual2.get()) << "read value is not the value just written";
    EXPECT_EQ(expected1, actual1.get()) << "incorrect historical value";

    client.send_terminate(3U);
}

TEST(mvcc_mmap_test, atomic_global_revision)
{
    boost::atomic<sgd::mvcc_revision> tmp;
    ASSERT_TRUE(tmp.is_lock_free()) << "mvcc_revision is not atomic";
}

TEST(mvcc_mmap_test, process_read_metadata_single_key)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    const char* key = "process_read_metadata_single";

    sgd::string_value expected1("abc1");
    client.send_write_string(10U, key, expected1);
    const boost::optional<const sgd::string_value&> readerA_actual1 = readerA.read<sgd::string_value>(key);
    const boost::optional<const sgd::string_value&> readerB_actual1 = readerB.read<sgd::string_value>(key);
    EXPECT_TRUE(readerA_actual1) << "read failed";
    EXPECT_TRUE(readerB_actual1) << "read failed";
    EXPECT_EQ(readerA_actual1.get(), expected1) << "value read is not the value just written";
    EXPECT_EQ(readerB_actual1.get(), expected1) << "value read is not the value just written";
    client.send_process_read_metadata(11U);
    boost::uint64_t readerA_rev1 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev1 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev1 = client.send_get_global_oldest_revision_read(12U);
    EXPECT_NE(oldest_rev1, 0U) << "process_read_metadata failed";
    EXPECT_EQ(readerA_rev1, oldest_rev1) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev1, oldest_rev1) << "oldest global read revision is not correct";

    sgd::string_value expected2("abc2");
    client.send_write_string(20U, key, expected2);
    const boost::optional<const sgd::string_value&> readerA_actual2 = readerA.read<sgd::string_value>(key);
    const boost::optional<const sgd::string_value&> readerB_actual2 = readerB.read<sgd::string_value>(key);
    EXPECT_TRUE(readerA_actual2) << "read failed";
    EXPECT_TRUE(readerB_actual2) << "read failed";
    EXPECT_EQ(readerA_actual2.get(), expected2) << "value read is not the value just written";
    EXPECT_EQ(readerB_actual2.get(), expected2) << "value read is not the value just written";
    client.send_process_read_metadata(21U);
    boost::uint64_t readerA_rev2 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev2 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev2 = client.send_get_global_oldest_revision_read(22U);
    EXPECT_NE(oldest_rev2, 0U) << "process_read_metadata failed";
    EXPECT_EQ(readerA_rev2, oldest_rev2) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev2, oldest_rev2) << "oldest global read revision is not correct";

    sgd::string_value expected3("abc3");
    client.send_write_string(30U, key, expected3);
    const boost::optional<const sgd::string_value&> readerA_actual3 = readerA.read<sgd::string_value>(key);
    EXPECT_TRUE(readerA_actual3) << "read failed";
    EXPECT_EQ(readerA_actual3.get(), expected3) << "value read is not the value just written";
    client.send_process_read_metadata(31U);
    boost::uint64_t readerA_rev3 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev3 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev3 = client.send_get_global_oldest_revision_read(32U);
    EXPECT_NE(readerA_rev3, oldest_rev3) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev3, oldest_rev3) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev2, readerB_rev3) << "last read revision is not correct";

    const boost::optional<const sgd::string_value&> readerB_actual3 = readerA.read<sgd::string_value>("foobar");
    EXPECT_FALSE(readerB_actual3) << "read succeeded";

    client.send_terminate(40U);
}

TEST(mvcc_mmap_test, process_read_metadata_multi_key)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    const char* keyA = "process_read_metadata_multi_A";
    const char* keyB = "process_read_metadata_multi_B";
    const char* keyC = "process_read_metadata_multi_C";

    sgd::string_value expectedA1("abc1");
    client.send_write_string(10U, keyA, expectedA1);
    const boost::optional<const sgd::string_value&> readerA_actual1 = readerA.read<sgd::string_value>(keyA);
    const boost::optional<const sgd::string_value&> readerB_actual1 = readerB.read<sgd::string_value>(keyA);
    EXPECT_TRUE(readerA_actual1) << "read failed";
    EXPECT_TRUE(readerB_actual1) << "read failed";
    EXPECT_EQ(readerA_actual1.get(), expectedA1) << "value read is not the value just written";
    EXPECT_EQ(readerB_actual1.get(), expectedA1) << "value read is not the value just written";
    client.send_process_read_metadata(11U);
    boost::uint64_t readerA_rev1 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev1 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev1 = client.send_get_global_oldest_revision_read(12U);
    EXPECT_NE(oldest_rev1, 0U) << "process_read_metadata failed";
    EXPECT_EQ(readerA_rev1, oldest_rev1) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev1, oldest_rev1) << "oldest global read revision is not correct";

    sgd::string_value expectedB1("xyz1");
    client.send_write_string(20U, keyB, expectedB1);
    const boost::optional<const sgd::string_value&> readerA_actual2 = readerA.read<sgd::string_value>(keyB);
    const boost::optional<const sgd::string_value&> readerB_actual2 = readerB.read<sgd::string_value>(keyB);
    EXPECT_TRUE(readerA_actual2) << "read failed";
    EXPECT_TRUE(readerB_actual2) << "read failed";
    EXPECT_EQ(readerA_actual2.get(), expectedB1) << "value read is not the value just written";
    EXPECT_EQ(readerB_actual2.get(), expectedB1) << "value read is not the value just written";
    client.send_process_read_metadata(21U);
    boost::uint64_t readerA_rev2 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev2 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev2 = client.send_get_global_oldest_revision_read(22U);
    EXPECT_NE(oldest_rev2, 0U) << "process_read_metadata failed";
    EXPECT_TRUE(oldest_rev1 < oldest_rev2) << "process_read_metadata did not detect read change";
    EXPECT_EQ(readerA_rev2, oldest_rev2) << "oldest global read revision is not correct";
    EXPECT_EQ(readerB_rev2, oldest_rev2) << "oldest global read revision is not correct";

    sgd::string_value expectedC1("!@#1");
    client.send_write_string(30U, keyC, expectedC1);
    const boost::optional<const sgd::string_value&> readerB_actual3 = readerB.read<sgd::string_value>(keyC);
    EXPECT_TRUE(readerB_actual3) << "read failed";
    EXPECT_EQ(readerB_actual3.get(), expectedC1) << "value read is not the value just written";
    client.send_process_read_metadata(31U);
    boost::uint64_t readerA_rev3 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev3 = readerB.get_last_read_revision();
    boost::uint64_t oldest_rev3 = client.send_get_global_oldest_revision_read(32U);
    EXPECT_NE(oldest_rev3, 0U) << "process_read_metadata failed";
    EXPECT_EQ(oldest_rev2, oldest_rev3) << "process_read_metadata incorrectly changed the oldest found";
    EXPECT_TRUE(oldest_rev3 < readerB_rev3) << "oldest global read revision is not correct";
    EXPECT_EQ(readerA_rev3,  oldest_rev3) << "oldest global read revision is not correct";

    const boost::optional<const sgd::string_value&> readerA_actual3 = readerA.read<sgd::string_value>("foobar");
    EXPECT_FALSE(readerA_actual3) << "read succeeded";

    client.send_terminate(40U);
}

TEST(mvcc_mmap_test, process_read_metadata_subset)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerC(bfs::path(conf.name.c_str()));
    const char* keyA = "process_read_metadata_multi_A";
    const char* keyB = "process_read_metadata_multi_B";

    sgd::string_value expectedA1("abc123");
    client.send_write_string(10U, keyA, expectedA1);
    readerA.read<sgd::string_value>(keyA);
    readerB.read<sgd::string_value>(keyA);
    readerC.read<sgd::string_value>(keyA);

    sgd::string_value expectedB1("def456");
    client.send_write_string(20U, keyB, expectedB1);
    readerA.read<sgd::string_value>(keyB);
    readerB.read<sgd::string_value>(keyB);

    sgd::string_value expectedA2("xyz123");
    client.send_write_string(30U, keyA, expectedA2);
    readerA.read<sgd::string_value>(keyA);

    client.send_process_read_metadata(40U, readerA.get_reader_token_id(), readerC.get_reader_token_id());
    boost::uint64_t readerA_rev = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev = readerB.get_last_read_revision();
    boost::uint64_t readerC_rev = readerC.get_last_read_revision();
    boost::uint64_t oldest_rev = client.send_get_global_oldest_revision_read(41U);
    EXPECT_TRUE(oldest_rev < readerA_rev) << "process_read_metadata did not detected global oldest";
    EXPECT_EQ(oldest_rev, readerB_rev) << "process_read_metadata did not detected global oldest";
    EXPECT_TRUE(readerC_rev < oldest_rev) << "process_read_metadata did not detected global oldest";

    client.send_terminate(50U);
}

TEST(mvcc_mmap_test, process_write_metadata_single_key)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    std::string key("process_write_metadata_single");

    sgd::string_value value1("abc123");
    client.send_write_string(10U, key.c_str(), value1);
    client.send_process_write_metadata(11U);
    std::vector<std::string> registered1(client.send_get_registered_keys(12U));
    EXPECT_EQ(registered1.size(), 1U) << "incorrect number of registered values";
    EXPECT_EQ(registered1.at(0), key) << "unexpected registered key";

    sgd::string_value value2("abc456");
    client.send_write_string(20U, key.c_str(), value2);
    client.send_process_write_metadata(21U);
    std::vector<std::string> registered2(client.send_get_registered_keys(22U));
    EXPECT_EQ(registered1.size(), 1U) << "incorrect number of registered values";
    EXPECT_EQ(registered1.at(0), key) << "unexpected registered key";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, process_write_metadata_multi_key)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    std::string keyA("process_write_metadata_A");
    std::string keyB("process_write_metadata_B");
    std::string keyC("process_write_metadata_C");

    sgd::string_value valueC1("abc123");
    client.send_write_string(10U, keyC.c_str(), valueC1);
    sgd::string_value valueB1("def123");
    client.send_write_string(11U, keyB.c_str(), valueB1);
    client.send_process_write_metadata(12U);
    std::vector<std::string> registered1(client.send_get_registered_keys(13U));
    EXPECT_EQ(registered1.size(), 2U) << "incorrect number of registered values";
    EXPECT_EQ(registered1.at(0), keyB) << "unexpected registered key";
    EXPECT_EQ(registered1.at(1), keyC) << "unexpected registered key";

    sgd::string_value valueC2("abc456");
    client.send_write_string(20U, keyC.c_str(), valueC2);
    sgd::string_value valueA1("ghi123");
    client.send_write_string(21U, keyA.c_str(), valueA1);
    client.send_process_write_metadata(22U);
    std::vector<std::string> registered2(client.send_get_registered_keys(23U));
    EXPECT_EQ(registered2.size(), 3U) << "incorrect number of registered values";
    EXPECT_EQ(registered2.at(0), keyA) << "unexpected registered key";
    EXPECT_EQ(registered2.at(1), keyB) << "unexpected registered key";
    EXPECT_EQ(registered2.at(2), keyC) << "unexpected registered key";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, process_write_metadata_subset)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    std::string keyA("process_write_metadata_A");
    std::string keyB("process_write_metadata_B");
    std::string keyC("process_write_metadata_C");

    sgd::string_value valueC1("abc123");
    client.send_write_string(10U, keyC.c_str(), valueC1);
    sgd::string_value valueB1("def123");
    client.send_write_string(11U, keyB.c_str(), valueB1);
    client.send_process_write_metadata(12U, 1U);
    std::vector<std::string> registered1(client.send_get_registered_keys(13U));
    EXPECT_EQ(registered1.size(), 1U) << "incorrect number of registered values";
    EXPECT_EQ(registered1.at(0), keyC) << "unexpected registered key";

    sgd::string_value valueC2("abc456");
    client.send_write_string(20U, keyC.c_str(), valueC2);
    sgd::string_value valueA1("ghi123");
    client.send_write_string(21U, keyA.c_str(), valueA1);
    client.send_process_write_metadata(22U, 1U);
    std::vector<std::string> registered2(client.send_get_registered_keys(23U));
    EXPECT_EQ(registered2.size(), 2U) << "incorrect number of registered values";
    EXPECT_EQ(registered2.at(0), keyB) << "unexpected registered key";
    EXPECT_EQ(registered2.at(1), keyC) << "unexpected registered key";

    client.send_process_write_metadata(40U, 5U);
    std::vector<std::string> registered3(client.send_get_registered_keys(41U));
    EXPECT_EQ(registered3.size(), 3U) << "incorrect number of registered values";
    EXPECT_EQ(registered3.at(0), keyA) << "unexpected registered key";
    EXPECT_EQ(registered3.at(1), keyB) << "unexpected registered key";
    EXPECT_EQ(registered3.at(2), keyC) << "unexpected registered key";

    client.send_terminate(50U);
}

TEST(mvcc_mmap_test, collect_garbage_single_key_single_type)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    std::string keyA("collect_garbage_A");

    std::size_t depth1 = 0;
    sgd::string_value valueA1("abc123");
    client.send_write_string(10U, keyA.c_str(), valueA1);
    ++depth1;
    readerA.read<sgd::string_value>(keyA.c_str());
    readerB.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevA1 = readerA.get_oldest_revision<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevB1 = readerB.get_oldest_revision<sgd::string_value>(keyA.c_str());
    EXPECT_EQ(oldestrevA1, oldestrevB1) << "oldest revision of same value is inconsistent";
    sgd::string_value valueA2("def123");
    client.send_write_string(11U, keyA.c_str(), valueA2);
    ++depth1;
    EXPECT_EQ(depth1, client.send_get_string_history_depth(12U, keyA.c_str())) << "unexpected history depth";
    readerA.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevA2 = readerA.get_oldest_revision<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevB2 = readerB.get_oldest_revision<sgd::string_value>(keyA.c_str());
    EXPECT_EQ(oldestrevA2, oldestrevB2) << "oldest revision of same value is inconsistent";
    client.send_process_read_metadata(13U);
    client.send_process_write_metadata(14U);
    boost::uint64_t oldestrev1 = client.send_get_global_oldest_revision_read(12U);
    EXPECT_EQ(oldestrev1, oldestrevA1) << "global oldest revision read is wrong";
    client.send_collect_garbage(15U);
    readerB.read<sgd::string_value>(keyA.c_str());
    EXPECT_EQ(depth1, client.send_get_string_history_depth(16U, keyA.c_str())) << "read value was collected";

    std::size_t depth2 = depth1;
    sgd::string_value valueA3("ghi23");
    client.send_write_string(20U, keyA.c_str(), valueA3);
    ++depth2;
    readerA.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevA3 = readerA.get_oldest_revision<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevB3 = readerB.get_oldest_revision<sgd::string_value>(keyA.c_str());
    EXPECT_EQ(oldestrevA3, oldestrevB3) << "oldest revision of same value is inconsistent";
    client.send_process_read_metadata(21U);
    client.send_process_write_metadata(22U);
    boost::uint64_t oldestrev2 = client.send_get_global_oldest_revision_read(12U);
    EXPECT_NE(oldestrev2, oldestrevA1) << "global oldest revision read not updated after newer reads";
    client.send_collect_garbage(23U);
    --depth2;
    boost::uint64_t oldestrevA4 = readerA.get_oldest_revision<sgd::string_value>(keyA.c_str());
    boost::uint64_t oldestrevB4 = readerB.get_oldest_revision<sgd::string_value>(keyA.c_str());
    EXPECT_EQ(oldestrevA4, oldestrevB4) << "oldest revision of same value is inconsistent";
    EXPECT_NE(oldestrevA3, oldestrevA4) << "oldest revision not updated after garbage collection";
    EXPECT_EQ(depth2, client.send_get_string_history_depth(24U, keyA.c_str())) << "unused value was not collected";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, collect_garbage_single_key_multi_type)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    std::string keyA("collect_garbage_A");
    std::string keyB("collect_garbage_B");
    std::size_t depthA1 = 0;
    std::size_t depthB1 = 0;

    sgd::string_value valueA1("abc123");
    sgd::struct_value valueB1(true, 5, 12.5);
    client.send_write_string(10U, keyA.c_str(), valueA1);
    client.send_write_struct(11U, keyB.c_str(), valueB1);
    ++depthA1;
    ++depthB1;
    readerA.read<sgd::string_value>(keyA.c_str());
    readerB.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t readerArev1 = readerA.get_last_read_revision();
    boost::uint64_t readerBrev1 = readerB.get_last_read_revision();
    readerA.read<sgd::struct_value>(keyB.c_str());
    readerB.read<sgd::struct_value>(keyB.c_str());
    boost::uint64_t readerArev2 = readerA.get_last_read_revision();
    boost::uint64_t readerBrev2 = readerB.get_last_read_revision();
    client.send_process_read_metadata(12U);
    client.send_process_write_metadata(13U);
    boost::uint64_t oldestrev1 = client.send_get_global_oldest_revision_read(14U);
    EXPECT_NE(readerArev1, readerArev2) << "revision of newer read is not greater than old read";
    EXPECT_NE(readerBrev1, readerBrev2) << "revision of newer read is not greater than old read";
    EXPECT_EQ(oldestrev1, readerArev2) << "global oldest revision read not updated after newer reads";
    EXPECT_EQ(oldestrev1, readerBrev2) << "global oldest revision read not updated after newer reads";
    client.send_collect_garbage(15U);
    EXPECT_EQ(depthA1, client.send_get_string_history_depth(16U, keyA.c_str())) << "read value was not collected";
    EXPECT_EQ(depthB1, client.send_get_struct_history_depth(17U, keyB.c_str())) << "value in use was collected";

    sgd::string_value valueA2("def456");
    client.send_write_string(20U, keyA.c_str(), valueA2);
    ++depthA1;
    readerA.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t readerArev3 = readerA.get_last_read_revision();
    EXPECT_NE(readerArev2, readerArev3) << "revision of newer read is not greater than old read";
    client.send_process_read_metadata(21U);
    client.send_process_write_metadata(22U);
    boost::uint64_t oldestrev2 = client.send_get_global_oldest_revision_read(23U);
    EXPECT_EQ(oldestrev1, oldestrev2) << "global oldest revision should not have changed";
    client.send_collect_garbage(24U);
    --depthA1;
    EXPECT_EQ(depthA1, client.send_get_string_history_depth(25U, keyA.c_str())) << "value in use was collected";
    EXPECT_EQ(depthB1, client.send_get_struct_history_depth(26U, keyB.c_str())) << "value in use was collected";

    readerB.read<sgd::string_value>(keyA.c_str());
    boost::uint64_t readerBrev3 = readerB.get_last_read_revision();
    EXPECT_NE(readerBrev2, readerBrev3) << "revision of newer read is not greater than old read";
    client.send_process_read_metadata(30U);
    client.send_process_write_metadata(31U);
    boost::uint64_t oldestrev3 = client.send_get_global_oldest_revision_read(23U);
    EXPECT_NE(oldestrev2, oldestrev3) << "global oldest revision not updated";
    client.send_collect_garbage(32U);
    EXPECT_EQ(depthA1, client.send_get_string_history_depth(33U, keyA.c_str())) << "value in use was collected";
    EXPECT_EQ(depthB1, client.send_get_struct_history_depth(334, keyB.c_str())) << "last remaining value was collected";

    client.send_terminate(40U);
}

TEST(mvcc_mmap_test, collect_garbage_multi_key_multi_type)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader reader1(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader reader2(bfs::path(conf.name.c_str()));
    std::string stringKeyA("collect_string_A");
    std::string stringKeyB("collect_string_B");
    std::string structKeyA("collect_struct_A");
    std::string structKeyB("collect_struct_B");
    std::size_t stringAdepth = 0;
    std::size_t stringBdepth = 0;
    std::size_t structAdepth = 0;
    std::size_t structBdepth = 0;

    sgd::string_value stringA1("abc123");
    client.send_write_string(10U, stringKeyA.c_str(), stringA1);
    ++stringAdepth;
    boost::uint64_t stringA1Rev = reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str());
    reader1.read<sgd::string_value>(stringKeyA.c_str());
    sgd::string_value stringB1("def123");
    client.send_write_string(11U, stringKeyB.c_str(), stringB1);
    ++stringBdepth;
    boost::uint64_t stringB1Rev = reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str());
    reader2.read<sgd::string_value>(stringKeyB.c_str());
    sgd::string_value stringB2("def456");
    client.send_write_string(12U, stringKeyB.c_str(), stringB2);
    ++stringBdepth;
    reader1.read<sgd::string_value>(stringKeyB.c_str());
    boost::uint64_t stringB2Rev = reader1.get_last_read_revision();
    client.send_process_read_metadata(13U);
    client.send_process_write_metadata(14U);
    client.send_collect_garbage(15U);
    EXPECT_EQ(stringAdepth, client.send_get_string_history_depth(16U, stringKeyA.c_str())) << "read value was collected";
    EXPECT_EQ(stringA1Rev, reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str())) << "revision 1 of stringKeyA was collected";

    sgd::struct_value structA1(true, 5, 12.5);
    client.send_write_struct(20U, structKeyA.c_str(), structA1);
    ++structAdepth;
    boost::uint64_t structA1Rev = reader2.get_oldest_revision<sgd::struct_value>(structKeyA.c_str());
    reader2.read<sgd::struct_value>(structKeyA.c_str());
    client.send_process_read_metadata(21U);
    client.send_process_write_metadata(22U);
    client.send_collect_garbage(23U);
    --stringBdepth;
    EXPECT_EQ(stringBdepth, client.send_get_string_history_depth(24U, stringKeyB.c_str())) << "read value was not collected";
    EXPECT_NE(stringB1Rev, reader2.get_oldest_revision<sgd::struct_value>(stringKeyB.c_str())) << "revision 1 of stringKeyA was not collected";

    sgd::struct_value structB1(false, 9, 23.8);
    client.send_write_struct(30U, structKeyB.c_str(), structB1);
    ++structBdepth;
    boost::uint64_t structB1Rev = reader1.get_oldest_revision<sgd::struct_value>(structKeyB.c_str());
    reader1.read<sgd::struct_value>(structKeyB.c_str());
    client.send_process_read_metadata(31U);
    client.send_process_write_metadata(32U);
    client.send_collect_garbage(33U);
    EXPECT_EQ(stringBdepth, client.send_get_string_history_depth(34U, stringKeyB.c_str())) << "read value was collected";
    EXPECT_EQ(stringB2Rev, reader1.get_oldest_revision<sgd::struct_value>(stringKeyB.c_str())) << "revision 2 of stringKeyA was collected";

    sgd::struct_value structB2(false, 40, 378.99);
    client.send_write_struct(40U, structKeyB.c_str(), structB2);
    ++structBdepth;
    reader2.read<sgd::struct_value>(structKeyB.c_str());
    boost::uint64_t structB2Rev = reader2.get_last_read_revision();
    client.send_process_read_metadata(41U);
    client.send_process_write_metadata(42U);
    client.send_collect_garbage(43U);
    EXPECT_EQ(structAdepth, client.send_get_struct_history_depth(44U, structKeyA.c_str())) << "read value was collected";
    EXPECT_EQ(structA1Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyA.c_str())) << "revision 1 of structKeyA was collected";

    sgd::string_value stringA2("abc456");
    client.send_write_string(50U, stringKeyA.c_str(), stringA2);
    ++stringAdepth;
    reader1.read<sgd::string_value>(stringKeyA.c_str());
    reader2.read<sgd::string_value>(stringKeyA.c_str());
    client.send_process_read_metadata(51U);
    client.send_process_write_metadata(52U);
    client.send_collect_garbage(53U);
    --structBdepth;
    EXPECT_EQ(structBdepth, client.send_get_struct_history_depth(54U, structKeyB.c_str())) << "revision 1 of structKeyB was not collected";
    EXPECT_NE(structB1Rev, reader1.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "revision 1 of structKeyB was not collected";
    client.send_collect_garbage(55U);
    EXPECT_EQ(structBdepth, client.send_get_struct_history_depth(56U, structKeyB.c_str())) << "last remaining value of structKeyB was collected";
    EXPECT_EQ(structB2Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "last remaining value of structKeyB was collected";

    client.send_terminate(60U);
}

TEST(mvcc_mmap_test, collect_garbage_subset)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader reader1(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader reader2(bfs::path(conf.name.c_str()));
    std::string stringKeyA("collect_string_A");
    std::string stringKeyB("collect_string_B");
    std::string structKeyA("collect_struct_A");
    std::string structKeyB("collect_struct_B");
    std::size_t stringAdepth = 0;
    std::size_t stringBdepth = 0;
    std::size_t structAdepth = 0;
    std::size_t structBdepth = 0;

    sgd::string_value stringA1("abc123");
    client.send_write_string(10U, stringKeyA.c_str(), stringA1);
    ++stringAdepth;
    boost::uint64_t stringA1Rev = reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str());
    sgd::string_value stringA2("abc456");
    client.send_write_string(11U, stringKeyA.c_str(), stringA2);
    ++stringAdepth;
    boost::uint64_t stringA2Rev = reader1.get_newest_revision<sgd::string_value>(stringKeyA.c_str());
    sgd::string_value stringB1("def123");
    client.send_write_string(12U, stringKeyB.c_str(), stringB1);
    ++stringBdepth;
    boost::uint64_t stringB1Rev = reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str());
    sgd::string_value stringB2("def456");
    client.send_write_string(13U, stringKeyB.c_str(), stringB1);
    ++stringBdepth;
    boost::uint64_t stringB2Rev = reader2.get_newest_revision<sgd::string_value>(stringKeyB.c_str());
    sgd::struct_value structA1(true, 5, 12.5);
    client.send_write_struct(14U, structKeyA.c_str(), structA1);
    ++structAdepth;
    boost::uint64_t structA1Rev = reader1.get_oldest_revision<sgd::struct_value>(structKeyA.c_str());
    sgd::struct_value structA2(true, 7, 41.3);
    client.send_write_struct(15U, structKeyA.c_str(), structA1);
    ++structAdepth;
    boost::uint64_t structA2Rev = reader1.get_newest_revision<sgd::struct_value>(structKeyA.c_str());
    sgd::struct_value structB1(false, 9, 23.8);
    client.send_write_struct(16U, structKeyB.c_str(), structB1);
    ++structBdepth;
    boost::uint64_t structB1Rev = reader1.get_oldest_revision<sgd::struct_value>(structKeyB.c_str());
    reader1.read<sgd::struct_value>(structKeyB.c_str());
    reader2.read<sgd::struct_value>(structKeyB.c_str());
    client.send_process_read_metadata(17U);
    client.send_process_write_metadata(18U);
    std::string nextKey1(client.send_collect_garbage(19U, 2U));
    --stringAdepth;
    --stringBdepth;
    EXPECT_EQ(stringAdepth, client.send_get_string_history_depth(20U, stringKeyA.c_str())) << "unused value was not collected";
    EXPECT_NE(stringA1Rev, reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str())) << "unused value was not collected";
    EXPECT_EQ(stringA2Rev, reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str())) << "last remaining value was collected";
    EXPECT_EQ(stringBdepth, client.send_get_string_history_depth(21U, stringKeyB.c_str())) << "unused value was not collected";
    EXPECT_NE(stringB1Rev, reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str())) << "unused value was not collected";
    EXPECT_EQ(stringB2Rev, reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str())) << "last remaining value was collected";
    EXPECT_EQ(structA1Rev, reader1.get_oldest_revision<sgd::struct_value>(structKeyA.c_str())) << "collection exceeded maximum attempts";
    EXPECT_EQ(structB1Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "collection exceeded maximum attempts";
    EXPECT_NE(stringKeyA, nextKey1) << "next key to collect is a key just collected";
    EXPECT_NE(stringKeyB, nextKey1) << "next key to collect is a key just collected";
    EXPECT_TRUE(nextKey1 == structKeyA || nextKey1 == structKeyB) << "next key to collect is not one of the keys available for collection";

    sgd::struct_value structB2(false, 11, 56.23);
    client.send_write_struct(30U, structKeyB.c_str(), structB1);
    ++structBdepth;
    boost::uint64_t structB2Rev = reader1.get_newest_revision<sgd::struct_value>(structKeyB.c_str());
    reader1.read<sgd::struct_value>(structKeyB.c_str());
    reader2.read<sgd::struct_value>(structKeyB.c_str());
    client.send_process_read_metadata(32U);
    client.send_process_write_metadata(33U);
    std::string nextKey2(client.send_collect_garbage(34U, nextKey1, 2U));
    --structAdepth;
    --structBdepth;
    EXPECT_EQ(structAdepth, client.send_get_struct_history_depth(35U, structKeyA.c_str())) << "unused value was not collected";
    EXPECT_NE(structA1Rev, reader1.get_oldest_revision<sgd::struct_value>(structKeyA.c_str())) << "unused value was not collected";
    EXPECT_EQ(structA2Rev, reader1.get_oldest_revision<sgd::struct_value>(structKeyA.c_str())) << "last remaining value was collected";
    EXPECT_EQ(structBdepth, client.send_get_struct_history_depth(36U, structKeyB.c_str())) << "unused value was not collected";
    EXPECT_NE(structB1Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "unused value was not collected";
    EXPECT_EQ(structB2Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "last remaining value was collected";
    EXPECT_EQ(stringA2Rev, reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str())) << "collection exceeded maximum attempts";
    EXPECT_EQ(stringB2Rev, reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str())) << "collection exceeded maximum attempts";
    EXPECT_NE(structKeyA, nextKey2) << "next key to collect is a key just collected";
    EXPECT_NE(structKeyB, nextKey2) << "next key to collect is a key just collected";
    EXPECT_TRUE(nextKey2 == stringKeyA || nextKey2 == stringKeyB) << "next key to collect is not one of the keys available for collection";

    sgd::string_value stringA3("abc789");
    client.send_write_string(40U, stringKeyA.c_str(), stringA3);
    ++stringAdepth;
    boost::uint64_t stringA3Rev = reader1.get_newest_revision<sgd::string_value>(stringKeyA.c_str());
    reader1.read<sgd::string_value>(stringKeyA.c_str());
    reader2.read<sgd::string_value>(stringKeyA.c_str());
    client.send_process_read_metadata(41U);
    client.send_process_write_metadata(42U);
    std::string nextKey3(client.send_collect_garbage(43U, nextKey2, 2U));
    --stringAdepth;
    EXPECT_EQ(stringAdepth, client.send_get_string_history_depth(44U, stringKeyA.c_str())) << "unused value was not collected";
    EXPECT_EQ(stringA3Rev, reader1.get_oldest_revision<sgd::string_value>(stringKeyA.c_str())) << "unused value was not collected";
    EXPECT_EQ(stringBdepth, client.send_get_string_history_depth(45U, stringKeyB.c_str())) << "last remaining value was collected";
    EXPECT_EQ(stringB2Rev, reader2.get_oldest_revision<sgd::string_value>(stringKeyB.c_str())) << "last remaining value was collected";
    EXPECT_NE(stringKeyA, nextKey3) << "next key to collect is a key just collected";
    EXPECT_NE(stringKeyB, nextKey3) << "next key to collect is a key just collected";
    EXPECT_EQ(structA2Rev, reader1.get_oldest_revision<sgd::struct_value>(structKeyA.c_str())) << "collection exceeded maximum attempts";
    EXPECT_EQ(structB2Rev, reader2.get_oldest_revision<sgd::struct_value>(structKeyB.c_str())) << "collection exceeded maximum attempts";
    EXPECT_TRUE(nextKey3 == structKeyA || nextKey3 == structKeyB) << "next key to collect is not one of the keys available for collection";

    client.send_terminate(60U);
}

TEST(mvcc_mmap_test, exists_after_removed)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    std::string stringKey("string_@@@");
    std::string structKey("struct_@@@");
    std::size_t stringDepth = 0;
    std::size_t structDepth = 0;

    sgd::string_value stringValue1("abc123");
    client.send_write_string(10U, stringKey.c_str(), stringValue1);
    ++stringDepth;
    EXPECT_TRUE(readerA.exists<sgd::string_value>(stringKey.c_str())) << "write failed";
    client.send_remove_string(11U, stringKey.c_str());
    EXPECT_FALSE(readerB.exists<sgd::string_value>(stringKey.c_str())) << "remove failed";

    sgd::struct_value structValue1(true, 5, 12.5);
    client.send_write_struct(20U, structKey.c_str(), structValue1);
    ++structDepth;
    EXPECT_TRUE(readerB.exists<sgd::struct_value>(structKey.c_str())) << "write failed";
    client.send_remove_struct(21U, structKey.c_str());
    EXPECT_FALSE(readerA.exists<sgd::struct_value>(structKey.c_str())) << "remove failed";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, write_after_remove)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    std::string stringKey("string_@@@");
    std::string structKey("struct_@@@");
    std::size_t stringDepth = 0;
    std::size_t structDepth = 0;

    sgd::string_value stringValue1("abc123");
    client.send_write_string(10U, stringKey.c_str(), stringValue1);
    ++stringDepth;
    client.send_remove_string(11U, stringKey.c_str());
    EXPECT_FALSE(readerB.exists<sgd::string_value>(stringKey.c_str())) << "remove failed";
    sgd::string_value stringValue2("abc456");
    client.send_write_string(12U, stringKey.c_str(), stringValue2);
    ++stringDepth;
    EXPECT_TRUE(readerA.exists<sgd::string_value>(stringKey.c_str())) << "write failed";

    sgd::struct_value structValue1(true, 5, 12.5);
    client.send_write_struct(20U, structKey.c_str(), structValue1);
    ++structDepth;
    client.send_remove_struct(21U, structKey.c_str());
    EXPECT_FALSE(readerA.exists<sgd::struct_value>(structKey.c_str())) << "remove failed";
    sgd::struct_value structValue2(true, 90, 37.2);
    client.send_write_struct(22U, structKey.c_str(), structValue2);
    ++structDepth;
    EXPECT_TRUE(readerB.exists<sgd::struct_value>(structKey.c_str())) << "write failed";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, collect_garbage_after_remove)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    std::string stringKey("string_@@@");
    std::string structKey("struct_@@@");
    std::size_t stringDepth = 0;
    std::size_t structDepth = 0;

    sgd::string_value stringValue1("abc123");
    client.send_write_string(10U, stringKey.c_str(), stringValue1);
    ++stringDepth;
    readerA.read<sgd::string_value>(stringKey.c_str());
    readerB.read<sgd::string_value>(stringKey.c_str());
    client.send_process_read_metadata(11U);
    client.send_process_write_metadata(12U);
    client.send_collect_garbage(13U);
    EXPECT_EQ(stringDepth, client.send_get_string_history_depth(14U, stringKey.c_str())) << "last remaining value was collected";
    client.send_remove_string(15U, stringKey.c_str());
    client.send_collect_garbage(16U);
    --stringDepth;
    EXPECT_EQ(stringDepth, client.send_get_string_history_depth(17U, stringKey.c_str())) << "value we want removed was not collected";

    sgd::struct_value structValue1(true, 5, 12.5);
    client.send_write_struct(20U, structKey.c_str(), structValue1);
    ++structDepth;
    readerA.read<sgd::struct_value>(structKey.c_str());
    readerB.read<sgd::struct_value>(structKey.c_str());
    client.send_process_read_metadata(21U);
    client.send_process_write_metadata(22U);
    client.send_collect_garbage(23U);
    EXPECT_EQ(structDepth, client.send_get_struct_history_depth(24U, structKey.c_str())) << "last remaining value was collected";
    client.send_remove_struct(25U, structKey.c_str());
    client.send_collect_garbage(26U);
    --structDepth;
    EXPECT_EQ(structDepth, client.send_get_struct_history_depth(27U, structKey.c_str())) << "value we want removed was not collected";

    client.send_terminate(30U);
}

TEST(mvcc_mmap_test, read_after_remove)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    service_client client(conf);
    sgd::mvcc_mmap_reader readerA(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerB(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerC(bfs::path(conf.name.c_str()));
    sgd::mvcc_mmap_reader readerD(bfs::path(conf.name.c_str()));
    std::string stringKey("string_@@@");
    std::string structKey("struct_@@@");

    sgd::string_value stringValue1("abc123");
    client.send_write_string(10U, stringKey.c_str(), stringValue1);
    const boost::optional<const sgd::string_value&> readString1 = readerA.read<sgd::string_value>(stringKey.c_str());
    EXPECT_TRUE(readString1) << "read failed";
    client.send_remove_string(11U, stringKey.c_str());
    const boost::optional<const sgd::string_value&> readString2 = readerB.read<sgd::string_value>(stringKey.c_str());
    EXPECT_FALSE(readString2) << "remove failed";
    boost::uint64_t readerA_rev1 = readerA.get_last_read_revision();
    boost::uint64_t readerB_rev1 = readerB.get_last_read_revision();
    EXPECT_NE(readerA_rev1, 0U) << "last read revision was not updated for successful read";
    EXPECT_EQ(readerB_rev1, 0U) << "last read revision was updated for unsuccessful read";

    sgd::struct_value structValue1(true, 5, 12.5);
    client.send_write_struct(20U, structKey.c_str(), structValue1);
    const boost::optional<const sgd::struct_value&> readStruct1 = readerC.read<sgd::struct_value>(structKey.c_str());
    EXPECT_TRUE(readStruct1) << "read failed";
    client.send_remove_struct(21U, structKey.c_str());
    const boost::optional<const sgd::struct_value&> readStruct2 = readerD.read<sgd::struct_value>(structKey.c_str());
    EXPECT_FALSE(readStruct2) << "remove failed";
    boost::uint64_t readerC_rev1 = readerC.get_last_read_revision();
    boost::uint64_t readerD_rev1 = readerD.get_last_read_revision();
    EXPECT_NE(readerC_rev1, 0U) << "last read revision was not updated for successful read";
    EXPECT_EQ(readerD_rev1, 0U) << "last read revision was updated for unsuccessful read";

    client.send_terminate(30U);
}
