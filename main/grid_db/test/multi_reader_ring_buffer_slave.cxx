#include <istream>
#include <ostream>
#include <string>
#include <sstream>
#include <stdexcept>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/bind.hpp>
#include <boost/cstdint.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/optional.hpp>
#include <google/protobuf/message.h>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <zmq.hpp>
#include "role.hpp"
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"
#include "multi_reader_ring_buffer_slave.pb.h"
#include "ringbuf_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpo = boost::program_options;
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
    config() : ipc(ipc::shm), role(sgd::reader), port(DEFAULT_PORT), size(DEFAULT_SIZE) { }
    ipc::type ipc;
    sgd::role role;
    std::string name;
    port_t port;
    size_t size;
    size_t capacity;
};

typedef boost::optional<config> parse_result;

parse_result parse_cmd_line(const int argc, char* const argv[], std::ostringstream& err_msg)
{
    parse_result result;
    config tmp;
    bpo::options_description descr("Usage: multi_reader_ring_buffer_slave [options] ipc role name");
    descr.add_options()
            ("help,h", "This help text")
            ("port,p", bpo::value<port_t>(&tmp.port)->default_value(DEFAULT_PORT),
                    "Port number to listen on")
            ("size,s", bpo::value<size_t>(&tmp.size)->default_value(DEFAULT_SIZE),
                    "Size of ring buffer in bytes")
            ("capacity,c", bpo::value<size_t>(&tmp.capacity)->default_value(DEFAULT_CAPACITY),
                    "Capacity of ring buffer by number of elements")
            ("ipc,i", bpo::value<ipc::type>(&tmp.ipc)->required(),
                    "IPC method: (shm|mmap)")
            ("role,r", bpo::value<sgd::role>(&tmp.role)->required(),
                    "Role of the slave: (owner|reader)")
            ("name,n", bpo::value<std::string>(&tmp.name)->required(),
                    "Name of the shared memory/mapped file");
    bpo::positional_options_description pos;
    pos.add("ipc", 1);
    pos.add("role", 1);
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

template <class element_t, class memory_t>
class ringbuf_slave
{
public:
    ringbuf_slave(const sgd::owner_t&, const config& config);
    ringbuf_slave(const sgd::reader_t&, const config& config);
    void run();
    void receive(const boost::system::error_code& error, size_t);
    void exec_terminate(const sgd::terminate_instr& input, result_msg& output);
    void exec_query_front(const sgd::query_front_instr& input, result_msg& output);
    void exec_query_back(const sgd::query_back_instr& input, result_msg& output);
    void exec_query_capacity(const sgd::query_capacity_instr& input, result_msg& output);
    void exec_query_count(const sgd::query_count_instr& input, result_msg& output);
    void exec_query_empty(const sgd::query_empty_instr& input, result_msg& output);
    void exec_query_full(const sgd::query_full_instr& input, result_msg& output);
    void exec_push_front(const sgd::push_front_instr& input, result_msg& output);
    void exec_pop_back(const sgd::pop_back_instr& input, result_msg& output);
    void exec_export_element(const sgd::export_element_instr& input, result_msg& output);
private:
    enum state
    {
	READY = 0,
	FINISHED = 1
    };
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::io_service service_;
    bas::posix::stream_descriptor stream_;
    memory_t memory_;
    sgd::multi_reader_ring_buffer<element_t, memory_t>* ringbuf_;
    std::vector<const element_t*> register_set_;
    instruction_msg instr_;
    result_msg result_;
    state state_;
};

template <class element_t, class memory_t>
ringbuf_slave<element_t, memory_t>::ringbuf_slave(const sgd::owner_t&, const config& config) :
    context_(),
    socket_(context_, ZMQ_REP),
    service_(),
    stream_(service_, init_zmq_socket(socket_, config)),
    memory_(bip::create_only, config.name.c_str(), config.size),
    ringbuf_(memory_.template construct< sgd::multi_reader_ring_buffer<element_t, memory_t> >(config.name.c_str())(config.capacity, &memory_)),
    register_set_(config.capacity, 0),
    instr_(0, register_set_.size() - 1),
    result_(),
    state_(READY)
{ }

template <class element_t, class memory_t>
ringbuf_slave<element_t, memory_t>::ringbuf_slave(const sgd::reader_t&, const config& config) :
    context_(),
    socket_(context_, ZMQ_REP),
    service_(),
    stream_(service_, init_zmq_socket(socket_, config)),
    memory_(bip::open_only, config.name.c_str()),
    ringbuf_(memory_.template find< sgd::multi_reader_ring_buffer<element_t, memory_t> >(config.name.c_str()).first),
    register_set_(config.capacity, 0),
    instr_(0, register_set_.size() - 1),
    result_(),
    state_(READY)
{
    if (UNLIKELY_EXT(!ringbuf_))
    {
	throw std::runtime_error("ring buffer not found");
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::run()
{
    boost::function2<void, const boost::system::error_code&, size_t> func(boost::bind(&ringbuf_slave::receive, this, _1, _2));
    while (state_ != FINISHED)
    {
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run();
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::receive(const boost::system::error_code& error, size_t)
{
    int event = 0;
    size_t size = 0;
    socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    // More than 1 message may be available, so we need to consume all of them
    while (LIKELY_EXT(!error && size == sizeof(event)) && (event & ZMQ_POLLIN))
    {
	instruction_msg::msg_status status = instr_.deserialize(socket_);
	if (UNLIKELY_EXT(status == instruction_msg::MALFORMED))
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	else if (instr_.is_terminate())
	{
	    exec_terminate(instr_.get_terminate(), result_);
	}
	else if (instr_.is_query_front())
	{
	    exec_query_front(instr_.get_query_front(), result_);
	}
	else if (instr_.is_query_back())
	{
	    exec_query_back(instr_.get_query_back(), result_);
	}
	else if (instr_.is_query_capacity())
	{
	    exec_query_capacity(instr_.get_query_capacity(), result_);
	}
	else if (instr_.is_query_count())
	{
	    exec_query_count(instr_.get_query_count(), result_);
	}
	else if (instr_.is_query_empty())
	{
	    exec_query_empty(instr_.get_query_empty(), result_);
	}
	else if (instr_.is_query_full())
	{
	    exec_query_full(instr_.get_query_full(), result_);
	}
	else if (instr_.is_push_front())
	{
	    exec_push_front(instr_.get_push_front(), result_);
	}
	else if (instr_.is_pop_back())
	{
	    exec_pop_back(instr_.get_pop_back(), result_);
	}
	else if (instr_.is_export_element())
	{
	    exec_export_element(instr_.get_export_element(), result_);
	}
	else
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	result_.serialize(socket_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_terminate(const sgd::terminate_instr& input, result_msg& output)
{
    state_ = FINISHED;
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_front(const sgd::query_front_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.out_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	sgd::confirmation_result tmp;
	tmp.set_sequence(input.sequence());
	output.set_confirmation(tmp);
	register_set_.at(static_cast<size_t>(input.out_register())) = &(ringbuf_->front());
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_back(const sgd::query_back_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.out_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	sgd::confirmation_result tmp;
	tmp.set_sequence(input.sequence());
	output.set_confirmation(tmp);
	register_set_.at(static_cast<size_t>(input.out_register())) = &(ringbuf_->back());
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_capacity(const sgd::query_capacity_instr& input, result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_size(ringbuf_->capacity());
    output.set_size(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_count(const sgd::query_count_instr& input, result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_size(ringbuf_->element_count());
    output.set_size(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_empty(const sgd::query_empty_instr& input, result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_predicate(ringbuf_->empty());
    output.set_predicate(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_query_full(const sgd::query_full_instr& input, result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_predicate(ringbuf_->full());
    output.set_predicate(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_push_front(const sgd::push_front_instr& input, result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    element_t value = input.element();
    ringbuf_->push_front(value);
    output.set_confirmation(tmp);
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_pop_back(const sgd::pop_back_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.in_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	const element_t* addr = register_set_.at(static_cast<size_t>(input.in_register()));
	if (LIKELY_EXT(addr))
	{
	    ringbuf_->pop_back(*addr);
	    register_set_.at(static_cast<size_t>(input.in_register())) = 0;
	}
	sgd::confirmation_result tmp;
	tmp.set_sequence(input.sequence());
	output.set_confirmation(tmp);
    }
}

template <class element_t, class memory_t>
void ringbuf_slave<element_t, memory_t>::exec_export_element(const sgd::export_element_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.in_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	const element_t* addr = register_set_.at(static_cast<size_t>(input.in_register()));
	if (UNLIKELY_EXT(!addr))
	{
	    sgd::invalid_argument_result tmp;
	    output.set_invalid_argument(tmp);
	}
	else
	{
	    sgd::element_result tmp;
	    tmp.set_sequence(input.sequence());
	    tmp.set_element(*addr);
	    output.set_element(tmp);
	}
    }
}

template <class element_t, class memory_t>
int ringbuf_slave<element_t, memory_t>::init_zmq_socket(zmq::socket_t& socket, const config& config)
{
    std::string address(str(boost::format("ipc://*:%d") % config.port));
    socket.bind(address.c_str());

    // TODO this is currently POSIX specific, add a Windows version
    int fd = 0;
    size_t size = 0;
    socket.getsockopt(ZMQ_FD, &fd, &size);
    if (UNLIKELY_EXT(size != sizeof(fd)))
    {
	throw std::runtime_error("Can't find ZeroMQ socket file descriptor");
    }
    return fd;
}

typedef ringbuf_slave<boost::int32_t, bip::managed_mapped_file> mmap_ringbuf_slave;
typedef ringbuf_slave<boost::int32_t, bip::managed_shared_memory> shm_ringbuf_slave;

class shm_launcher : public boost::static_visitor<>
{
public:
    shm_launcher(const config& config) : config_(config) { }

    void operator()(const sgd::owner_t&) const
    {
	shm_ringbuf_slave service(sgd::owner, config_);
	service.run();
	bip::shared_memory_object::remove(config_.name.c_str());
    }

    void operator()(const sgd::reader_t&) const
    {
	shm_ringbuf_slave service(sgd::reader, config_);
	service.run();
    }

private:
    const config& config_;
};

class mmap_launcher : public boost::static_visitor<>
{
public:
    mmap_launcher(const config& config) : config_(config) { }

    void operator()(const sgd::owner_t&) const
    {
	mmap_ringbuf_slave service(sgd::owner, config_);
	service.run();
	bfs::remove(config_.name);
    }

    void operator()(const sgd::reader_t&) const
    {
	mmap_ringbuf_slave service(sgd::reader, config_);
	service.run();
    }

private:
    const config& config_;
};

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
    if (config.get().ipc == ipc::shm)
    {
	boost::apply_visitor(shm_launcher(config.get()), config.get().role);
    }
    else if (config.get().ipc == ipc::mmap)
    {
	boost::apply_visitor(mmap_launcher(config.get()), config.get().role);
    }
    return 0;
}
