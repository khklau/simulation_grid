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
#include <boost/format.hpp>
#include <boost/function.hpp>
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

namespace bas = boost::asio;
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

class instruction_msg
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    instruction_msg(size_t first, size_t last);
    msg_status deserialize(zmq::socket_t& socket);
    const sgd::instruction& get() const { return instr_; }
private:
    sgd::instruction instr_;
    zmq::message_t buf_;
    size_t first_register_;
    size_t last_register_;
};

instruction_msg::instruction_msg(size_t first, size_t last) :
    instr_(),
    buf_(static_cast<size_t>(instr_.SpaceUsed())),
    first_register_(first),
    last_register_(last)
{ }

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction_msg::msg_status instruction_msg::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (instr_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((instr_.opcode() == sgd::instruction::TERMINATE && instr_.has_terminate()) ||
	    (instr_.opcode() == sgd::instruction::QUERY_FRONT && instr_.has_query_front()) ||
	    (instr_.opcode() == sgd::instruction::QUERY_BACK && instr_.has_query_back()) ||
	    (instr_.opcode() == sgd::instruction::QUERY_CAPACITY && instr_.has_query_capacity()) ||
	    (instr_.opcode() == sgd::instruction::QUERY_BACK && instr_.has_query_empty()) ||
	    (instr_.opcode() == sgd::instruction::QUERY_BACK && instr_.has_query_full()) ||
	    (instr_.opcode() == sgd::instruction::PUSH_FRONT && instr_.has_push_front()) || 
	    (instr_.opcode() == sgd::instruction::POP_BACK && instr_.has_pop_back()) ||
	    (instr_.opcode() == sgd::instruction::EXPORT_ELEMENT && instr_.has_export_element()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

class result_msg
{
public:
    result_msg();
    const sgd::result& get() const { return result_; }
    void serialize(zmq::socket_t& socket);
    void set_malformed_message(sgd::malformed_message_result& result);
    void set_invalid_argument(sgd::invalid_argument_result& result);
    void set_confirmation(sgd::confirmation_result& result);
    void set_element(sgd::element_result& result);
    void set_size(sgd::size_result& result);
    void set_predicate(sgd::predicate_result& result);
private:
    sgd::result result_;
    zmq::message_t buf_;
};

result_msg::result_msg() :
     result_(), buf_(static_cast<size_t>(result_.SpaceUsed()))
{
    result_.set_opcode(sgd::result::CONFIRMATION);
}

void result_msg::serialize(zmq::socket_t& socket)
{
}

void result_msg::set_malformed_message(sgd::malformed_message_result& result)
{
    result_.set_opcode(sgd::result::MALFORMED_MESSAGE);
    *result_.mutable_malformed_message() = result;
}

void result_msg::set_invalid_argument(sgd::invalid_argument_result& result)
{
    result_.set_opcode(sgd::result::INVALID_ARGUMENT);
    *result_.mutable_invalid_argument() = result;
}

void result_msg::set_confirmation(sgd::confirmation_result& result)
{
    result_.set_opcode(sgd::result::CONFIRMATION);
    *result_.mutable_confirmation() = result;
}

void result_msg::set_element(sgd::element_result& result)
{
    result_.set_opcode(sgd::result::ELEMENT);
    *result_.mutable_element() = result;
}

void result_msg::set_size(sgd::size_result& result)
{
    result_.set_opcode(sgd::result::SIZE);
    *result_.mutable_size() = result;
}

void result_msg::set_predicate(sgd::predicate_result& result)
{
    result_.set_opcode(sgd::result::PREDICATE);
    *result_.mutable_predicate() = result;
}

template <class element_t, class memory_t>
class ringbuf_service
{
public:
    ringbuf_service(const sgd::owner_t&, const config& config);
    ringbuf_service(const sgd::reader_t&, const config& config);
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
ringbuf_service<element_t, memory_t>::ringbuf_service(const sgd::owner_t&, const config& config) :
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
ringbuf_service<element_t, memory_t>::ringbuf_service(const sgd::reader_t&, const config& config) :
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
void ringbuf_service<element_t, memory_t>::run()
{
    boost::function2<void, const boost::system::error_code&, size_t> func(boost::bind(&ringbuf_service::receive, this, _1, _2));
    while (state_ != FINISHED)
    {
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run();
    }
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::receive(const boost::system::error_code& error, size_t)
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
	switch (instr_.get().opcode())
	{
	    case sgd::instruction::TERMINATE:
	    {
		exec_terminate(instr_.get().terminate(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_FRONT:
	    {
		exec_query_front(instr_.get().query_front(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_BACK:
	    {
		exec_query_back(instr_.get().query_back(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_CAPACITY:
	    {
		exec_query_capacity(instr_.get().query_capacity(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_COUNT:
	    {
		exec_query_count(instr_.get().query_count(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_EMPTY:
	    {
		exec_query_empty(instr_.get().query_empty(), result_);
		break;
	    }
	    case sgd::instruction::QUERY_FULL:
	    {
		exec_query_full(instr_.get().query_full(), result_);
		break;
	    }
	    case sgd::instruction::PUSH_FRONT:
	    {
		exec_push_front(instr_.get().push_front(), result_);
		break;
	    }
	    case sgd::instruction::POP_BACK:
	    {
		exec_pop_back(instr_.get().pop_back(), result_);
		break;
	    }
	    case sgd::instruction::EXPORT_ELEMENT:
	    {
		exec_export_element(instr_.get().export_element(), result_);
		break;
	    }
	}
	result_.serialize(socket_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_terminate(const sgd::terminate_instr& input, result_msg& output)
{
    state_ = FINISHED;
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_query_front(const sgd::query_front_instr& input, result_msg& output)
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
void ringbuf_service<element_t, memory_t>::exec_query_back(const sgd::query_back_instr& input, result_msg& output)
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
void ringbuf_service<element_t, memory_t>::exec_query_capacity(const sgd::query_capacity_instr& input, result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_size(ringbuf_->capacity());
    output.set_size(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_query_count(const sgd::query_count_instr& input, result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_size(ringbuf_->element_count());
    output.set_size(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_query_empty(const sgd::query_empty_instr& input, result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_predicate(ringbuf_->empty());
    output.set_predicate(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_query_full(const sgd::query_full_instr& input, result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    tmp.set_predicate(ringbuf_->full());
    output.set_predicate(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_push_front(const sgd::push_front_instr& input, result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    boost::int32_t value = input.element();
    ringbuf_->push_front(value);
    output.set_confirmation(tmp);
}

template <class element_t, class memory_t>
void ringbuf_service<element_t, memory_t>::exec_pop_back(const sgd::pop_back_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.in_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	const boost::int32_t* addr = register_set_.at(static_cast<size_t>(input.in_register()));
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
void ringbuf_service<element_t, memory_t>::exec_export_element(const sgd::export_element_instr& input, result_msg& output)
{
    if (UNLIKELY_EXT(input.in_register() >= register_set_.size()))
    {
	sgd::invalid_argument_result tmp;
	output.set_invalid_argument(tmp);
    }
    else
    {
	const boost::int32_t* addr = register_set_.at(static_cast<size_t>(input.in_register()));
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
int ringbuf_service<element_t, memory_t>::init_zmq_socket(zmq::socket_t& socket, const config& config)
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

typedef ringbuf_service<boost::int32_t, bip::managed_mapped_file> mmap_ringbuf_service;
typedef ringbuf_service<boost::int32_t, bip::managed_shared_memory> shm_ringbuf_service;

class shm_launcher : public boost::static_visitor<>
{
public:
    shm_launcher(const config& config) : config_(config) { }

    void operator()(const sgd::owner_t&) const
    {
	shm_ringbuf_service service(sgd::owner, config_);
	service.run();
    }

    void operator()(const sgd::reader_t&) const
    {
	shm_ringbuf_service service(sgd::reader, config_);
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
	mmap_ringbuf_service service(sgd::owner, config_);
	service.run();
    }

    void operator()(const sgd::reader_t&) const
    {
	mmap_ringbuf_service service(sgd::reader, config_);
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
    //ringbuf_service service(config.get().mode);
    return 0;
}
