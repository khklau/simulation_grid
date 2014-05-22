#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <exception>
#include <memory>
#include <limits>
#include <sstream>
#include <string>
#include <vector>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/array.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/cstdint.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/thread_time.hpp>
#include <gtest/gtest.h>
#include <zmq.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/core/process_utility.hpp>
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"
#include "ringbuf_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace scp = simulation_grid::core::process_utility;
namespace sgd = simulation_grid::grid_db;

namespace {

typedef boost::uint16_t port_t;

static const port_t DEFAULT_PORT = 22222U;
static const size_t DEFAULT_SIZE = 1 << 16;
static const size_t DEFAULT_CAPACITY = 4;

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
    config() : ipc(ipc::shm), port(DEFAULT_PORT), size(DEFAULT_SIZE), capacity(DEFAULT_CAPACITY) { }
    config(ipc::type ipc_, const std::string& name_, port_t port_ = DEFAULT_PORT,
	    size_t size_ = DEFAULT_SIZE, size_t capacity_ = DEFAULT_CAPACITY) :
    	ipc(ipc_), name(name_), port(port_), size(size_), capacity(capacity_)
    { }
    ipc::type ipc;
    std::string name;
    port_t port;
    size_t size;
    size_t capacity;
};

template <class element_t, class memory_t>
class ringbuf_master
{
public:
    typedef sgd::multi_reader_ring_buffer<element_t, memory_t> ringbuf;
    ringbuf_master(const config& config);
    ~ringbuf_master();
    const ringbuf& get_ringbuf() const { return *ringbuf_; }
    sgd::result_msg send(sgd::instruction_msg& msg);
private:
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::io_service service_;
    bas::posix::stream_descriptor stream_;
    memory_t memory_;
    ringbuf* ringbuf_;
};

template <class element_t, class memory_t>
ringbuf_master<element_t, memory_t>::ringbuf_master(const config& config) :
    context_(1),
    socket_(context_, ZMQ_REQ),
    service_(),
    stream_(service_, init_zmq_socket(socket_, config)),
    memory_(bip::open_only, config.name.c_str()),
    ringbuf_(memory_.template find< sgd::multi_reader_ring_buffer<element_t, memory_t> >(config.name.c_str()).first)
{
    if (UNLIKELY_EXT(!ringbuf_))
    {
	throw std::runtime_error("ring buffer not found");
    }
}

template <class element_t, class memory_t>
ringbuf_master<element_t, memory_t>::~ringbuf_master()
{
    stream_.release();
    service_.stop();
    socket_.close();
    context_.close();
}


template <class element_t, class memory_t>
int ringbuf_master<element_t, memory_t>::init_zmq_socket(zmq::socket_t& socket, const config& config)
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

template <class element_t, class memory_t>
sgd::result_msg ringbuf_master<element_t, memory_t>::send(sgd::instruction_msg& msg)
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

typedef ringbuf_master<boost::int32_t, bip::managed_mapped_file> mmap_ringbuf_master;
typedef ringbuf_master<boost::int32_t, bip::managed_shared_memory> shm_ringbuf_master;

class ringbuf_slave
{
public:
    ringbuf_slave(const config& config);
    ~ringbuf_slave();
    int wait();
private:
    bool has_terminated;
    pid_t pid_;
};

ringbuf_slave::ringbuf_slave(const config& config) :
    has_terminated(false)
{
    static const char SLAVE_NAME[] = "multi_reader_ring_buffer_slave";
    boost::array <char, sizeof(SLAVE_NAME)> slave_name;
    strncpy(slave_name.c_array(), SLAVE_NAME, slave_name.max_size());

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

    static const char CAPACITY_OPT[] = "--capacity";
    boost::array<char, sizeof(CAPACITY_OPT)> capacity_opt;
    strncpy(capacity_opt.c_array(), CAPACITY_OPT, capacity_opt.max_size());

    boost::array<char, std::numeric_limits<size_t>::digits> capacity_arg;
    std::ostringstream capacity_buf;
    capacity_buf << config.capacity;
    strncpy(capacity_arg.c_array(), capacity_buf.str().c_str(), capacity_arg.max_size());

    // TODO: calculate the array size properly with constexpr after moving to C++11
    boost::array<char, 256> ipc_arg;
    std::ostringstream ipc_buf;
    ipc_buf << config.ipc;
    strncpy(ipc_arg.c_array(), ipc_buf.str().c_str(), ipc_arg.max_size());

    std::vector<char> name_arg(config.name.size() + 1, '\0');
    std::copy(config.name.begin(), config.name.end(), name_arg.begin());

    boost::array<char*, 10> arg_list = boost::assign::list_of
    		(slave_name.c_array())
		(port_opt.c_array())(port_arg.c_array())
		(size_opt.c_array())(size_arg.c_array())
		(capacity_opt.c_array())(capacity_arg.c_array())
		(ipc_arg.c_array())
		(&name_arg[0])
		(0);
    bfs::path slave_path = scp::current_exe_path().parent_path() / bfs::path(SLAVE_NAME);
    // stupid arg_list argument has to be an array of mutable C strings
    if (posix_spawn(&pid_, slave_path.string().c_str(), 0, 0, arg_list.c_array(), environ))
    {
	throw std::runtime_error("Could not spawn ringbuf_slave");
    }
    bfs::path ipcpath(config.name);
    if (config.ipc == ipc::shm)
    {
	ipcpath = bfs::path("/dev/shm") / bfs::path(config.name);
    }
    while (!bfs::exists(ipcpath))
    {
	boost::this_thread::sleep_for(boost::chrono::milliseconds(50));
    }
}

ringbuf_slave::~ringbuf_slave()
{
    if (!has_terminated)
    {
	wait();
    }
}

int ringbuf_slave::wait()
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

TEST(multi_reader_ring_buffer_test, mmap_front_reference_lifetime)
{
    bfs::path tmp_path = bfs::absolute(bfs::unique_path());
    config conf(ipc::mmap, tmp_path.string());
    ringbuf_slave slave(conf);
    mmap_ringbuf_master master(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(master.get_ringbuf().empty()) << "ringbuf is not empty";
    sgd::instruction_msg inmsg1;
    sgd::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sgd::result_msg outmsg1(master.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = master.get_ringbuf().front();
    EXPECT_EQ(expected1, actual1) << "front element is not just pushed element";
    ASSERT_EQ(1U, master.get_ringbuf().element_count()) << "element_count is wrong";

    const boost::int32_t expected2 = 2;
    sgd::instruction_msg inmsg2;
    sgd::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(expected2);
    inmsg2.set_push_front(instr2);
    sgd::result_msg outmsg2(master.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = master.get_ringbuf().front();
    EXPECT_EQ(expected2, actual2) << "front element is not just pushed element";
    EXPECT_EQ(2U, master.get_ringbuf().element_count()) << "element count is wrong";
    ASSERT_EQ(expected1, actual1) << "reference to original front element is lost after subsequent push";

    sgd::instruction_msg inmsg3;
    sgd::terminate_instr instr3;
    instr3.set_sequence(3U);
    inmsg3.set_terminate(instr3);
    sgd::result_msg outmsg3(master.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "Unexpected terminate result";
    ASSERT_EQ(inmsg3.get_terminate().sequence(), outmsg3.get_confirmation().sequence()) << "Sequence number mismatch";
}

TEST(multi_reader_ring_buffer_test, mmap_already_popped_back_element)
{
    bfs::path tmp_path = bfs::absolute(bfs::unique_path());
    config conf(ipc::mmap, tmp_path.string(), DEFAULT_PORT, DEFAULT_SIZE, 2);
    ringbuf_slave slave(conf);
    mmap_ringbuf_master master(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(master.get_ringbuf().empty()) << "ringbuf is not empty";
    sgd::instruction_msg inmsg1;
    sgd::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sgd::result_msg outmsg1(master.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = master.get_ringbuf().back();
    EXPECT_EQ(expected1, actual1) << "back element is not first element pushed";
    EXPECT_EQ(1U, master.get_ringbuf().element_count()) << "element count is wrong";

    const boost::int32_t expected2 = master.get_ringbuf().back();
    sgd::instruction_msg inmsg2;
    sgd::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(2);
    inmsg2.set_push_front(instr2);
    sgd::result_msg outmsg2(master.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = master.get_ringbuf().back();
    EXPECT_EQ(expected2, actual2) << "back element is not the same after push";
    EXPECT_EQ(2U, master.get_ringbuf().element_count()) << "element count is wrong";

    sgd::instruction_msg inmsg3;
    sgd::query_back_instr instr3;
    instr3.set_sequence(3U);
    instr3.set_out_register(0U);
    inmsg3.set_query_back(instr3);
    sgd::result_msg outmsg3(master.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg3.get_query_back().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

    sgd::instruction_msg inmsg4;
    sgd::export_element_instr instr4;
    instr4.set_sequence(4U);
    instr4.set_in_register(0U);
    inmsg4.set_export_element(instr4);
    sgd::result_msg outmsg4(master.send(inmsg4));
    ASSERT_TRUE(outmsg4.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg4.get_export_element().sequence(), outmsg4.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual4 = outmsg4.get_element().element();
    EXPECT_EQ(expected1, actual4) << "back element is not the original pushed element";

    sgd::instruction_msg inmsg5;
    sgd::pop_back_instr instr5;
    instr5.set_sequence(5U);
    instr5.set_in_register(0U);
    inmsg5.set_pop_back(instr5);
    sgd::result_msg outmsg5(master.send(inmsg5));
    ASSERT_TRUE(outmsg5.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg5.get_pop_back().sequence(), outmsg5.get_confirmation().sequence()) << "sequence number mismatch";

    sgd::instruction_msg inmsg6;
    sgd::query_back_instr instr6;
    instr6.set_sequence(6U);
    instr6.set_out_register(1U);
    inmsg6.set_query_back(instr6);
    sgd::result_msg outmsg6(master.send(inmsg6));
    ASSERT_TRUE(outmsg6.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg6.get_query_back().sequence(), outmsg6.get_confirmation().sequence()) << "sequence number mismatch";

    sgd::instruction_msg inmsg7;
    sgd::export_element_instr instr7;
    instr7.set_sequence(7U);
    instr7.set_in_register(1U);
    inmsg7.set_export_element(instr7);
    sgd::result_msg outmsg7(master.send(inmsg7));
    ASSERT_TRUE(outmsg7.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg7.get_export_element().sequence(), outmsg7.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual7 = outmsg7.get_element().element();
    EXPECT_NE(expected1, actual7) << "back element was not popped";
    EXPECT_EQ(1U, master.get_ringbuf().element_count()) << "element count is wrong";

    sgd::instruction_msg inmsg8;
    sgd::pop_back_instr instr8;
    instr8.set_sequence(8U);
    instr8.set_in_register(0U);
    inmsg8.set_pop_back(instr8);
    sgd::result_msg outmsg8(master.send(inmsg8));
    ASSERT_TRUE(outmsg8.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg8.get_pop_back().sequence(), outmsg8.get_confirmation().sequence()) << "sequence number mismatch";
    EXPECT_EQ(1U, master.get_ringbuf().element_count()) << "attempting to pop an element that was already popped should have failed";

    sgd::instruction_msg inmsg9;
    sgd::terminate_instr instr9;
    instr9.set_sequence(9U);
    inmsg9.set_terminate(instr9);
    sgd::result_msg outmsg9(master.send(inmsg9));
    ASSERT_TRUE(outmsg9.is_confirmation()) << "Unexpected terminate result";
    ASSERT_EQ(inmsg9.get_terminate().sequence(), outmsg9.get_confirmation().sequence()) << "Sequence number mismatch";
}

TEST(multi_reader_ring_buffer_test, mmap_overfill)
{
    bfs::path tmp_path = bfs::unique_path();
    bip::managed_mapped_file file(bip::create_only, tmp_path.string().c_str(), 4096);
    sgd::mmap_multi_reader_ring_buffer<boost::int32_t>::type* buf = file.construct<sgd::mmap_multi_reader_ring_buffer<boost::int32_t>::type>("RB")(4, &file);
    EXPECT_TRUE(buf->empty()) << "initial state of ring buffer is not empty";
    buf->push_front(1);
    buf->push_front(2);
    buf->push_front(3);
    buf->push_front(4);
    EXPECT_TRUE(buf->full()) << "ring buffer not filled to capacity";
    const boost::int32_t& expected1 = buf->back();
    buf->push_front(5);
    const boost::int32_t& actual1 = buf->back();
    ASSERT_NE(expected1, actual1) << "back element was not popped";
    bfs::remove(tmp_path);
}

TEST(multi_reader_ring_buffer_test, shmem_front_reference_lifetime)
{
    bip::managed_shared_memory shmem(bip::create_only, "shmem_front_reference_lifetime", 4096);
    sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type* buf = shmem.construct<sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type>("RB")(4, &shmem);
    const boost::int32_t expected1 = 1;
    buf->push_front(expected1);
    const boost::int32_t& actual1 = buf->front();
    EXPECT_EQ(expected1, actual1) << "front element is not just pushed element";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    const boost::int32_t expected2 = 2;
    buf->push_front(expected2);
    const boost::int32_t& actual2 = buf->front();
    EXPECT_EQ(expected2, actual2) << "front element is not just pushed element";
    EXPECT_EQ(2U, buf->element_count()) << "element count is wrong";
    ASSERT_EQ(expected1, actual1) << "reference to original front element is lost after subsequent push";
    bip::shared_memory_object::remove("shmem_front_reference_lifetime");
}

TEST(multi_reader_ring_buffer_test, shmem_already_popped_back_element)
{
    bip::managed_shared_memory shmem(bip::create_only, "shmem_already_popped_back_element", 4096);
    sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type* buf = shmem.construct<sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type>("RB")(4, &shmem);
    const boost::int32_t expected1 = 1;
    buf->push_front(expected1);
    const boost::int32_t& actual1 = buf->back();
    EXPECT_EQ(expected1, actual1) << "back element is not first element pushed";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    const boost::int32_t expected2 = buf->back();
    buf->push_front(2);
    const boost::int32_t& actual2 = buf->back();
    EXPECT_EQ(expected2, actual2) << "back element is not the same after push";
    EXPECT_EQ(2U, buf->element_count()) << "element count is wrong";
    const boost::int32_t& expected3 = buf->back();
    buf->pop_back(expected3);
    const boost::int32_t& actual3 = buf->back();
    EXPECT_NE(expected3, actual3) << "back element was not popped";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    buf->pop_back(expected3);
    ASSERT_EQ(1U, buf->element_count()) << "attempting to pop an element that was already popped should have failed";
    bip::shared_memory_object::remove("shmem_already_popped_back_element");
}

TEST(multi_reader_ring_buffer_test, shmem_overfill)
{
    bip::managed_shared_memory shmem(bip::create_only, "shmem_overfill", 4096);
    sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type* buf = shmem.construct<sgd::shmem_multi_reader_ring_buffer<boost::int32_t>::type>("RB")(4, &shmem);
    EXPECT_TRUE(buf->empty()) << "initial state of ring buffer is not empty";
    buf->push_front(1);
    buf->push_front(2);
    buf->push_front(3);
    buf->push_front(4);
    EXPECT_TRUE(buf->full()) << "ring buffer not filled to capacity";
    const boost::int32_t& expected1 = buf->back();
    buf->push_front(5);
    const boost::int32_t& actual1 = buf->back();
    ASSERT_NE(expected1, actual1) << "back element was not popped";
    bip::shared_memory_object::remove("shmem_overfill");
}
