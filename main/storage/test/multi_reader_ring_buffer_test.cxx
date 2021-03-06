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
#include <supernova/core/compiler_extensions.hpp>
#include <supernova/core/process_utility.hpp>
#include <supernova/communication/request_reply_client.hpp>
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"
#include "ringbuf_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace scp = supernova::core::process_utility;
namespace scm = supernova::communication;
namespace sst = supernova::storage;

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
class service_client
{
public:
    typedef bip::allocator<element_t, typename memory_t::segment_manager> allocator_t;
    typedef sst::multi_reader_ring_buffer<element_t, allocator_t> ringbuf;
    service_client(const config& config);
    ~service_client();
    const ringbuf& get_ringbuf() const { return *ringbuf_; }
    sst::result_msg send(sst::instruction_msg& msg);
    void send_terminate(boost::uint32_t sequence);
private:
    memory_t memory_;
    ringbuf* ringbuf_;
    scm::request_reply_client client_;
};

template <class element_t, class memory_t>
service_client<element_t, memory_t>::service_client(const config& config) :
    memory_(bip::open_only, config.name.c_str()),
    ringbuf_(memory_.template find< sst::multi_reader_ring_buffer<element_t, allocator_t> >(config.name.c_str()).first),
    client_("127.0.0.1", config.port)
{
    if (UNLIKELY_EXT(!ringbuf_))
    {
	throw std::runtime_error("ring buffer not found");
    }
}

template <class element_t, class memory_t>
service_client<element_t, memory_t>::~service_client()
{ }

template <class element_t, class memory_t>
sst::result_msg service_client<element_t, memory_t>::send(sst::instruction_msg& instr)
{
    sst::result_msg result;
    scm::request_reply_client::source source(sizeof(result));
    scm::request_reply_client::sink sink(sizeof(instr));
    instr.serialize(sink);
    client_.send(sink, source);
    sst::result_msg::msg_status status = result.deserialize(source);
    if (UNLIKELY_EXT(status == sst::result_msg::MALFORMED))
    {
	throw std::runtime_error("Received malformed message");
    }
    return result;
}

template <class element_t, class memory_t>
void service_client<element_t, memory_t>::send_terminate(boost::uint32_t sequence)
{
    sst::instruction_msg inmsg;
    sst::terminate_instr instr;
    instr.set_sequence(sequence);
    inmsg.set_terminate(instr);
    sst::result_msg outmsg(send(inmsg));
    EXPECT_TRUE(outmsg.is_confirmation()) << "Unexpected terminate result";
    EXPECT_EQ(inmsg.get_terminate().sequence(), outmsg.get_confirmation().sequence()) << "Sequence number mismatch";
}

typedef service_client<boost::int32_t, bip::managed_mapped_file> mmap_service_client;
typedef service_client<boost::int32_t, bip::managed_shared_memory> shm_service_client;

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
    static const char SLAVE_NAME[] = "multi_reader_ring_buffer_service";
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
    		(launcher_name.c_array())
		(port_opt.c_array())(port_arg.c_array())
		(size_opt.c_array())(size_arg.c_array())
		(capacity_opt.c_array())(capacity_arg.c_array())
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
    while (!bfs::exists(ipcpath))
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

TEST(multi_reader_ring_buffer_test, mmap_front_reference_lifetime)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string());
    service_launcher launcher(conf);
    mmap_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = client.get_ringbuf().front();
    EXPECT_EQ(expected1, actual1) << "front element is not just pushed element";
    ASSERT_EQ(1U, client.get_ringbuf().element_count()) << "element_count is wrong";

    const boost::int32_t expected2 = 2;
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(expected2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = client.get_ringbuf().front();
    EXPECT_EQ(expected2, actual2) << "front element is not just pushed element";
    EXPECT_EQ(2U, client.get_ringbuf().element_count()) << "element count is wrong";
    ASSERT_EQ(expected1, actual1) << "reference to original front element is lost after subsequent push";

    client.send_terminate(3U);
}

TEST(multi_reader_ring_buffer_test, mmap_already_popped_back_element)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string(), DEFAULT_PORT, DEFAULT_SIZE, 2U);
    service_launcher launcher(conf);
    mmap_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = client.get_ringbuf().back();
    EXPECT_EQ(expected1, actual1) << "back element is not first element pushed";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "element count is wrong";

    const boost::int32_t expected2 = client.get_ringbuf().back();
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = client.get_ringbuf().back();
    EXPECT_EQ(expected2, actual2) << "back element is not the same after push";
    EXPECT_EQ(2U, client.get_ringbuf().element_count()) << "element count is wrong";

    sst::instruction_msg inmsg3;
    sst::query_back_instr instr3;
    instr3.set_sequence(3U);
    instr3.set_out_register(0U);
    inmsg3.set_query_back(instr3);
    sst::result_msg outmsg3(client.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg3.get_query_back().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg4;
    sst::export_element_instr instr4;
    instr4.set_sequence(4U);
    instr4.set_in_register(0U);
    inmsg4.set_export_element(instr4);
    sst::result_msg outmsg4(client.send(inmsg4));
    ASSERT_TRUE(outmsg4.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg4.get_export_element().sequence(), outmsg4.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual4 = outmsg4.get_element().element();
    EXPECT_EQ(expected1, actual4) << "back element is not the original pushed element";

    sst::instruction_msg inmsg5;
    sst::pop_back_instr instr5;
    instr5.set_sequence(5U);
    instr5.set_in_register(0U);
    inmsg5.set_pop_back(instr5);
    sst::result_msg outmsg5(client.send(inmsg5));
    ASSERT_TRUE(outmsg5.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg5.get_pop_back().sequence(), outmsg5.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg6;
    sst::query_back_instr instr6;
    instr6.set_sequence(6U);
    instr6.set_out_register(1U);
    inmsg6.set_query_back(instr6);
    sst::result_msg outmsg6(client.send(inmsg6));
    ASSERT_TRUE(outmsg6.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg6.get_query_back().sequence(), outmsg6.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg7;
    sst::export_element_instr instr7;
    instr7.set_sequence(7U);
    instr7.set_in_register(1U);
    inmsg7.set_export_element(instr7);
    sst::result_msg outmsg7(client.send(inmsg7));
    ASSERT_TRUE(outmsg7.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg7.get_export_element().sequence(), outmsg7.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual7 = outmsg7.get_element().element();
    EXPECT_NE(expected1, actual7) << "back element was not popped";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "element count is wrong";

    sst::instruction_msg inmsg8;
    sst::pop_back_instr instr8;
    instr8.set_sequence(8U);
    instr8.set_in_register(0U);
    inmsg8.set_pop_back(instr8);
    sst::result_msg outmsg8(client.send(inmsg8));
    ASSERT_TRUE(outmsg8.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg8.get_pop_back().sequence(), outmsg8.get_confirmation().sequence()) << "sequence number mismatch";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "attempting to pop an element that was already popped should have failed";

    client.send_terminate(9U);
}

TEST(multi_reader_ring_buffer_test, mmap_overfill)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string(), DEFAULT_PORT, DEFAULT_SIZE, 2U);
    service_launcher launcher(conf);
    mmap_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";

    const boost::int32_t expected2 = 2;
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(expected2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    ASSERT_TRUE(client.get_ringbuf().full()) << "ring buffer not filled to capacity";

    sst::instruction_msg inmsg3;
    sst::query_back_instr instr3;
    instr3.set_sequence(3U);
    instr3.set_out_register(0U);
    inmsg3.set_query_back(instr3);
    sst::result_msg outmsg3(client.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg3.get_query_back().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg4;
    sst::export_element_instr instr4;
    instr4.set_sequence(4U);
    instr4.set_in_register(0U);
    inmsg4.set_export_element(instr4);
    sst::result_msg outmsg4(client.send(inmsg4));
    ASSERT_TRUE(outmsg4.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg4.get_export_element().sequence(), outmsg4.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual4 = outmsg4.get_element().element();

    const boost::int32_t expected5 = 5;
    sst::instruction_msg inmsg5;
    sst::push_front_instr instr5;
    instr5.set_sequence(5U);
    instr5.set_element(expected5);
    inmsg5.set_push_front(instr5);
    sst::result_msg outmsg5(client.send(inmsg5));
    ASSERT_TRUE(outmsg5.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg5.get_push_front().sequence(), outmsg5.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg6;
    sst::query_back_instr instr6;
    instr6.set_sequence(6U);
    instr6.set_out_register(1U);
    inmsg6.set_query_back(instr6);
    sst::result_msg outmsg6(client.send(inmsg6));
    ASSERT_TRUE(outmsg6.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg6.get_query_back().sequence(), outmsg6.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg7;
    sst::export_element_instr instr7;
    instr7.set_sequence(7U);
    instr7.set_in_register(1U);
    inmsg7.set_export_element(instr7);
    sst::result_msg outmsg7(client.send(inmsg7));
    ASSERT_TRUE(outmsg7.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg7.get_export_element().sequence(), outmsg7.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual7 = outmsg7.get_element().element();
    ASSERT_NE(actual4, actual7) << "back element was not popped";

    client.send_terminate(9U);
}

TEST(multi_reader_ring_buffer_test, mmap_multi_read_sequence)
{
    config conf(ipc::mmap, bfs::absolute(bfs::unique_path()).string(), DEFAULT_PORT, DEFAULT_SIZE, 4U);
    service_launcher launcher(conf);

    {
	mmap_service_client client1(conf);

	const boost::int32_t expected1 = 1;
	ASSERT_TRUE(client1.get_ringbuf().empty()) << "ringbuf is not empty";
	sst::instruction_msg inmsg1;
	sst::push_front_instr instr1;
	instr1.set_sequence(1U);
	instr1.set_element(expected1);
	inmsg1.set_push_front(instr1);
	sst::result_msg outmsg1(client1.send(inmsg1));
	ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";

	const boost::int32_t expected2 = 2;
	sst::instruction_msg inmsg2;
	sst::push_front_instr instr2;
	instr2.set_sequence(2U);
	instr2.set_element(expected2);
	inmsg2.set_push_front(instr2);
	sst::result_msg outmsg2(client1.send(inmsg2));
	ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";

	ASSERT_EQ(2U, client1.get_ringbuf().element_count()) << "element_count is wrong";
    }

    {
	mmap_service_client client2(conf);

	const boost::int32_t expected3 = 3;
	ASSERT_FALSE(client2.get_ringbuf().empty()) << "ringbuf is empty";
	sst::instruction_msg inmsg3;
	sst::push_front_instr instr3;
	instr3.set_sequence(3U);
	instr3.set_element(expected3);
	inmsg3.set_push_front(instr3);
	sst::result_msg outmsg3(client2.send(inmsg3));
	ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg3.get_push_front().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

	const boost::int32_t expected4 = 2;
	sst::instruction_msg inmsg4;
	sst::push_front_instr instr4;
	instr4.set_sequence(2U);
	instr4.set_element(expected4);
	inmsg4.set_push_front(instr4);
	sst::result_msg outmsg4(client2.send(inmsg4));
	ASSERT_TRUE(outmsg4.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg4.get_push_front().sequence(), outmsg4.get_confirmation().sequence()) << "sequence number mismatch";

	ASSERT_EQ(4U, client2.get_ringbuf().element_count()) << "element_count is wrong";

	client2.send_terminate(5U);
    }
}

TEST(multi_reader_ring_buffer_test, shm_front_reference_lifetime)
{
    config conf(ipc::shm, bfs::unique_path().string());
    service_launcher launcher(conf);
    shm_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = client.get_ringbuf().front();
    EXPECT_EQ(expected1, actual1) << "front element is not just pushed element";
    ASSERT_EQ(1U, client.get_ringbuf().element_count()) << "element_count is wrong";

    const boost::int32_t expected2 = 2;
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(expected2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = client.get_ringbuf().front();
    EXPECT_EQ(expected2, actual2) << "front element is not just pushed element";
    EXPECT_EQ(2U, client.get_ringbuf().element_count()) << "element count is wrong";
    ASSERT_EQ(expected1, actual1) << "reference to original front element is lost after subsequent push";

    client.send_terminate(3U);
}

TEST(multi_reader_ring_buffer_test, shm_already_popped_back_element)
{
    config conf(ipc::shm, bfs::unique_path().string(), DEFAULT_PORT, DEFAULT_SIZE, 2U);
    service_launcher launcher(conf);
    shm_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual1 = client.get_ringbuf().back();
    EXPECT_EQ(expected1, actual1) << "back element is not first element pushed";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "element count is wrong";

    const boost::int32_t expected2 = client.get_ringbuf().back();
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    const boost::int32_t& actual2 = client.get_ringbuf().back();
    EXPECT_EQ(expected2, actual2) << "back element is not the same after push";
    EXPECT_EQ(2U, client.get_ringbuf().element_count()) << "element count is wrong";

    sst::instruction_msg inmsg3;
    sst::query_back_instr instr3;
    instr3.set_sequence(3U);
    instr3.set_out_register(0U);
    inmsg3.set_query_back(instr3);
    sst::result_msg outmsg3(client.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg3.get_query_back().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg4;
    sst::export_element_instr instr4;
    instr4.set_sequence(4U);
    instr4.set_in_register(0U);
    inmsg4.set_export_element(instr4);
    sst::result_msg outmsg4(client.send(inmsg4));
    ASSERT_TRUE(outmsg4.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg4.get_export_element().sequence(), outmsg4.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual4 = outmsg4.get_element().element();
    EXPECT_EQ(expected1, actual4) << "back element is not the original pushed element";

    sst::instruction_msg inmsg5;
    sst::pop_back_instr instr5;
    instr5.set_sequence(5U);
    instr5.set_in_register(0U);
    inmsg5.set_pop_back(instr5);
    sst::result_msg outmsg5(client.send(inmsg5));
    ASSERT_TRUE(outmsg5.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg5.get_pop_back().sequence(), outmsg5.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg6;
    sst::query_back_instr instr6;
    instr6.set_sequence(6U);
    instr6.set_out_register(1U);
    inmsg6.set_query_back(instr6);
    sst::result_msg outmsg6(client.send(inmsg6));
    ASSERT_TRUE(outmsg6.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg6.get_query_back().sequence(), outmsg6.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg7;
    sst::export_element_instr instr7;
    instr7.set_sequence(7U);
    instr7.set_in_register(1U);
    inmsg7.set_export_element(instr7);
    sst::result_msg outmsg7(client.send(inmsg7));
    ASSERT_TRUE(outmsg7.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg7.get_export_element().sequence(), outmsg7.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual7 = outmsg7.get_element().element();
    EXPECT_NE(expected1, actual7) << "back element was not popped";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "element count is wrong";

    sst::instruction_msg inmsg8;
    sst::pop_back_instr instr8;
    instr8.set_sequence(8U);
    instr8.set_in_register(0U);
    inmsg8.set_pop_back(instr8);
    sst::result_msg outmsg8(client.send(inmsg8));
    ASSERT_TRUE(outmsg8.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg8.get_pop_back().sequence(), outmsg8.get_confirmation().sequence()) << "sequence number mismatch";
    EXPECT_EQ(1U, client.get_ringbuf().element_count()) << "attempting to pop an element that was already popped should have failed";

    client.send_terminate(9U);
}

TEST(multi_reader_ring_buffer_test, shm_overfill)
{
    config conf(ipc::shm, bfs::unique_path().string(), DEFAULT_PORT, DEFAULT_SIZE, 2U);
    service_launcher launcher(conf);
    shm_service_client client(conf);

    const boost::int32_t expected1 = 1;
    ASSERT_TRUE(client.get_ringbuf().empty()) << "ringbuf is not empty";
    sst::instruction_msg inmsg1;
    sst::push_front_instr instr1;
    instr1.set_sequence(1U);
    instr1.set_element(expected1);
    inmsg1.set_push_front(instr1);
    sst::result_msg outmsg1(client.send(inmsg1));
    ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";

    const boost::int32_t expected2 = 2;
    sst::instruction_msg inmsg2;
    sst::push_front_instr instr2;
    instr2.set_sequence(2U);
    instr2.set_element(expected2);
    inmsg2.set_push_front(instr2);
    sst::result_msg outmsg2(client.send(inmsg2));
    ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";
    ASSERT_TRUE(client.get_ringbuf().full()) << "ring buffer not filled to capacity";

    sst::instruction_msg inmsg3;
    sst::query_back_instr instr3;
    instr3.set_sequence(3U);
    instr3.set_out_register(0U);
    inmsg3.set_query_back(instr3);
    sst::result_msg outmsg3(client.send(inmsg3));
    ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg3.get_query_back().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg4;
    sst::export_element_instr instr4;
    instr4.set_sequence(4U);
    instr4.set_in_register(0U);
    inmsg4.set_export_element(instr4);
    sst::result_msg outmsg4(client.send(inmsg4));
    ASSERT_TRUE(outmsg4.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg4.get_export_element().sequence(), outmsg4.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual4 = outmsg4.get_element().element();

    const boost::int32_t expected5 = 5;
    sst::instruction_msg inmsg5;
    sst::push_front_instr instr5;
    instr5.set_sequence(5U);
    instr5.set_element(expected5);
    inmsg5.set_push_front(instr5);
    sst::result_msg outmsg5(client.send(inmsg5));
    ASSERT_TRUE(outmsg5.is_confirmation()) << "unexpected push_front result";
    ASSERT_EQ(inmsg5.get_push_front().sequence(), outmsg5.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg6;
    sst::query_back_instr instr6;
    instr6.set_sequence(6U);
    instr6.set_out_register(1U);
    inmsg6.set_query_back(instr6);
    sst::result_msg outmsg6(client.send(inmsg6));
    ASSERT_TRUE(outmsg6.is_confirmation()) << "unexpected query_back result";
    ASSERT_EQ(inmsg6.get_query_back().sequence(), outmsg6.get_confirmation().sequence()) << "sequence number mismatch";

    sst::instruction_msg inmsg7;
    sst::export_element_instr instr7;
    instr7.set_sequence(7U);
    instr7.set_in_register(1U);
    inmsg7.set_export_element(instr7);
    sst::result_msg outmsg7(client.send(inmsg7));
    ASSERT_TRUE(outmsg7.is_element()) << "unexpected export_element result";
    ASSERT_EQ(inmsg7.get_export_element().sequence(), outmsg7.get_element().sequence()) << "sequence number mismatch";
    const boost::int32_t actual7 = outmsg7.get_element().element();
    ASSERT_NE(actual4, actual7) << "back element was not popped";

    client.send_terminate(9U);
}

TEST(multi_reader_ring_buffer_test, shm_multi_read_sequence)
{
    config conf(ipc::shm, bfs::unique_path().string(), DEFAULT_PORT, DEFAULT_SIZE, 4U);
    service_launcher launcher(conf);

    {
	shm_service_client client1(conf);

	const boost::int32_t expected1 = 1;
	ASSERT_TRUE(client1.get_ringbuf().empty()) << "ringbuf is not empty";
	sst::instruction_msg inmsg1;
	sst::push_front_instr instr1;
	instr1.set_sequence(1U);
	instr1.set_element(expected1);
	inmsg1.set_push_front(instr1);
	sst::result_msg outmsg1(client1.send(inmsg1));
	ASSERT_TRUE(outmsg1.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg1.get_push_front().sequence(), outmsg1.get_confirmation().sequence()) << "sequence number mismatch";

	const boost::int32_t expected2 = 2;
	sst::instruction_msg inmsg2;
	sst::push_front_instr instr2;
	instr2.set_sequence(2U);
	instr2.set_element(expected2);
	inmsg2.set_push_front(instr2);
	sst::result_msg outmsg2(client1.send(inmsg2));
	ASSERT_TRUE(outmsg2.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg2.get_push_front().sequence(), outmsg2.get_confirmation().sequence()) << "sequence number mismatch";

	ASSERT_EQ(2U, client1.get_ringbuf().element_count()) << "element_count is wrong";
    }

    {
	shm_service_client client2(conf);

	const boost::int32_t expected3 = 3;
	ASSERT_FALSE(client2.get_ringbuf().empty()) << "ringbuf is empty";
	sst::instruction_msg inmsg3;
	sst::push_front_instr instr3;
	instr3.set_sequence(3U);
	instr3.set_element(expected3);
	inmsg3.set_push_front(instr3);
	sst::result_msg outmsg3(client2.send(inmsg3));
	ASSERT_TRUE(outmsg3.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg3.get_push_front().sequence(), outmsg3.get_confirmation().sequence()) << "sequence number mismatch";

	const boost::int32_t expected4 = 2;
	sst::instruction_msg inmsg4;
	sst::push_front_instr instr4;
	instr4.set_sequence(2U);
	instr4.set_element(expected4);
	inmsg4.set_push_front(instr4);
	sst::result_msg outmsg4(client2.send(inmsg4));
	ASSERT_TRUE(outmsg4.is_confirmation()) << "unexpected push_front result";
	ASSERT_EQ(inmsg4.get_push_front().sequence(), outmsg4.get_confirmation().sequence()) << "sequence number mismatch";

	ASSERT_EQ(4U, client2.get_ringbuf().element_count()) << "element_count is wrong";

	client2.send_terminate(5U);
    }
}
