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
    void serialize(zmq::socket_t& socket);
    msg_status deserialize(zmq::socket_t& socket);
    inline bool is_terminate() { return msg_.opcode() == sgd::instruction::TERMINATE; }
    inline bool is_query_front() { return msg_.opcode() == sgd::instruction::QUERY_FRONT; }
    inline bool is_query_back() { return msg_.opcode() == sgd::instruction::QUERY_BACK; }
    inline bool is_query_capacity() { return msg_.opcode() == sgd::instruction::QUERY_CAPACITY; }
    inline bool is_query_count() { return msg_.opcode() == sgd::instruction::QUERY_COUNT; }
    inline bool is_query_empty() { return msg_.opcode() == sgd::instruction::QUERY_EMPTY; }
    inline bool is_query_full() { return msg_.opcode() == sgd::instruction::QUERY_FULL; }
    inline bool is_push_front() { return msg_.opcode() == sgd::instruction::PUSH_FRONT; }
    inline bool is_pop_back() { return msg_.opcode() == sgd::instruction::POP_BACK; }
    inline bool is_export_element() { return msg_.opcode() == sgd::instruction::EXPORT_ELEMENT; }
    inline const sgd::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const sgd::query_front_instr& get_query_front() { return msg_.query_front(); }
    inline const sgd::query_back_instr& get_query_back() { return msg_.query_back(); }
    inline const sgd::query_capacity_instr& get_query_capacity() { return msg_.query_capacity(); }
    inline const sgd::query_count_instr& get_query_count() { return msg_.query_count(); }
    inline const sgd::query_empty_instr& get_query_empty() { return msg_.query_empty(); }
    inline const sgd::query_full_instr& get_query_full() { return msg_.query_full(); }
    inline const sgd::push_front_instr& get_push_front() { return msg_.push_front(); }
    inline const sgd::pop_back_instr& get_pop_back() { return msg_.pop_back(); }
    inline const sgd::export_element_instr& get_export_element() { return msg_.export_element(); }
    void set_terminate(const sgd::terminate_instr& instr);
    void set_query_front(const sgd::query_front_instr& instr);
    void set_query_back(const sgd::query_back_instr& instr);
    void set_query_capacity(const sgd::query_capacity_instr& instr);
    void set_query_count(const sgd::query_count_instr& instr);
    void set_query_empty(const sgd::query_empty_instr& instr);
    void set_query_full(const sgd::query_full_instr& instr);
    void set_push_front(const sgd::push_front_instr& instr);
    void set_pop_back(const sgd::pop_back_instr& instr);
    void set_export_element(const sgd::export_element_instr& instr);
private:
    sgd::instruction msg_;
    zmq::message_t buf_;
    size_t first_register_;
    size_t last_register_;
};

instruction_msg::instruction_msg(size_t first, size_t last) :
    msg_(),
    buf_(static_cast<size_t>(msg_.SpaceUsed())),
    first_register_(first),
    last_register_(last)
{ }

void instruction_msg::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction_msg::msg_status instruction_msg::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((is_terminate() && msg_.has_terminate()) ||
	    (is_query_front() && msg_.has_query_front()) ||
	    (is_query_back() && msg_.has_query_back()) ||
	    (is_query_capacity() && msg_.has_query_capacity()) ||
	    (is_query_count() && msg_.has_query_count()) ||
	    (is_query_empty() && msg_.has_query_empty()) ||
	    (is_query_full() && msg_.has_query_full()) ||
	    (is_push_front() && msg_.has_push_front()) || 
	    (is_pop_back() && msg_.has_pop_back()) ||
	    (is_export_element() && msg_.has_export_element()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void instruction_msg::set_terminate(const sgd::terminate_instr& instr)
{
    msg_.set_opcode(sgd::instruction::TERMINATE);
    *msg_.mutable_terminate() = instr;
}

void instruction_msg::set_query_front(const sgd::query_front_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_FRONT);
    *msg_.mutable_query_front() = instr;
}

void instruction_msg::set_query_back(const sgd::query_back_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_BACK);
    *msg_.mutable_query_back() = instr;
}

void instruction_msg::set_query_capacity(const sgd::query_capacity_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_CAPACITY);
    *msg_.mutable_query_capacity() = instr;
}

void instruction_msg::set_query_count(const sgd::query_count_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_COUNT);
    *msg_.mutable_query_count() = instr;
}

void instruction_msg::set_query_empty(const sgd::query_empty_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_EMPTY);
    *msg_.mutable_query_empty() = instr;
}

void instruction_msg::set_query_full(const sgd::query_full_instr& instr)
{
    msg_.set_opcode(sgd::instruction::QUERY_FULL);
    *msg_.mutable_query_full() = instr;
}

void instruction_msg::set_push_front(const sgd::push_front_instr& instr)
{
    msg_.set_opcode(sgd::instruction::PUSH_FRONT);
    *msg_.mutable_push_front() = instr;
}

void instruction_msg::set_pop_back(const sgd::pop_back_instr& instr)
{
    msg_.set_opcode(sgd::instruction::POP_BACK);
    *msg_.mutable_pop_back() = instr;
}

void instruction_msg::set_export_element(const sgd::export_element_instr& instr)
{
    msg_.set_opcode(sgd::instruction::EXPORT_ELEMENT);
    *msg_.mutable_export_element() = instr;
}


class result_msg
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    result_msg();
    void serialize(zmq::socket_t& socket);
    msg_status deserialize(zmq::socket_t& socket);
    inline bool is_malformed_message() { return msg_.opcode() == sgd::result::MALFORMED_MESSAGE; }
    inline bool is_invalid_argument() { return msg_.opcode() == sgd::result::INVALID_ARGUMENT; }
    inline bool is_confirmation() { return msg_.opcode() == sgd::result::CONFIRMATION; }
    inline bool is_element() { return msg_.opcode() == sgd::result::ELEMENT; }
    inline bool is_size() { return msg_.opcode() == sgd::result::SIZE; }
    inline bool is_predicate() { return msg_.opcode() == sgd::result::PREDICATE; }
    inline const sgd::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const sgd::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const sgd::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const sgd::element_result& get_element() { return msg_.element(); }
    inline const sgd::size_result& get_size() { return msg_.size(); }
    inline const sgd::predicate_result& get_predicate() { return msg_.predicate(); }
    void set_malformed_message(const sgd::malformed_message_result& result);
    void set_invalid_argument(const sgd::invalid_argument_result& result);
    void set_confirmation(const sgd::confirmation_result& result);
    void set_element(const sgd::element_result& result);
    void set_size(const sgd::size_result& result);
    void set_predicate(const sgd::predicate_result& result);
private:
    sgd::result msg_;
    zmq::message_t buf_;
};

result_msg::result_msg() :
     msg_(), buf_(static_cast<size_t>(msg_.SpaceUsed()))
{
    msg_.set_opcode(sgd::result::CONFIRMATION);
}

void result_msg::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
result_msg::msg_status result_msg::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((is_malformed_message() && msg_.has_malformed_message()) ||
	    (is_invalid_argument() && msg_.has_invalid_argument()) ||
	    (is_confirmation() && msg_.has_confirmation()) ||
	    (is_element() && msg_.has_element()) ||
	    (is_size() && msg_.has_size()) ||
	    (is_predicate() && msg_.has_predicate()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void result_msg::set_malformed_message(const sgd::malformed_message_result& result)
{
    msg_.set_opcode(sgd::result::MALFORMED_MESSAGE);
    *msg_.mutable_malformed_message() = result;
}

void result_msg::set_invalid_argument(const sgd::invalid_argument_result& result)
{
    msg_.set_opcode(sgd::result::INVALID_ARGUMENT);
    *msg_.mutable_invalid_argument() = result;
}

void result_msg::set_confirmation(const sgd::confirmation_result& result)
{
    msg_.set_opcode(sgd::result::CONFIRMATION);
    *msg_.mutable_confirmation() = result;
}

void result_msg::set_element(const sgd::element_result& result)
{
    msg_.set_opcode(sgd::result::ELEMENT);
    *msg_.mutable_element() = result;
}

void result_msg::set_size(const sgd::size_result& result)
{
    msg_.set_opcode(sgd::result::SIZE);
    *msg_.mutable_size() = result;
}

void result_msg::set_predicate(const sgd::predicate_result& result)
{
    msg_.set_opcode(sgd::result::PREDICATE);
    *msg_.mutable_predicate() = result;
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
