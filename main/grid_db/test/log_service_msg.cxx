#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "log_service_msg.hpp"

namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

struct_A::struct_A(const char* k, const char* v)
{
    if (UNLIKELY_EXT(strlen(key) > MAX_LENGTH) || UNLIKELY_EXT(strlen(value) > MAX_LENGTH))
    {
        throw grid_db_error("Maximum value length exceeded")
                << info_component_identity("struct_A")
                << info_data_identity(key);
    }
    strncpy(key, k, sizeof(key));
    strncpy(value, v, sizeof(value));
}

struct_A::struct_A(const struct_A& other)
{
    strncpy(key, other.key, sizeof(key));
    strncpy(value, other.value, sizeof(value));
}

struct_A::~struct_A()
{ }

struct_A& struct_A::operator=(const struct_A& other)
{
    if (this != &other)
    {
        strncpy(key, other.key, sizeof(key));
        strncpy(value, other.value, sizeof(value));
    }
    return *this;
}

struct_A& struct_A::operator=(const struct_A_msg& other)
{
    strncpy(key, other.key().c_str(), sizeof(key));
    strncpy(value, other.value().c_str(), sizeof(value));
    return *this;
}

bool struct_A::operator==(const struct_A& other) const
{
    return strncmp(key, other.key, sizeof(key)) == 0 && strncmp(value, other.value, sizeof(value));
}

bool struct_A::operator<(const struct_A& other) const
{
    return strncmp(key, other.key, sizeof(key)) < 0 && strncmp(value, other.value, sizeof(value)) < 0;;
}

struct_B::struct_B(const char* k, bool a, boost::int32_t b, double c) :
	value1(a), value2(b), value3(c)
{
    strncpy(key, k, sizeof(key));
}

struct_B::struct_B(const struct_B& other) :
	value1(other.value1), value2(other.value2), value3(other.value3)
{
    strncpy(key, other.key, sizeof(key));
}

struct_B::struct_B(const struct_B_msg& msg) :
	value1(msg.value1()), value2(msg.value2()), value3(msg.value3())
{
    strncpy(key, msg.key().c_str(), sizeof(key));
}

struct_B::~struct_B()
{ }

struct_B& struct_B::operator=(const struct_B& other)
{
    if (this != &other)
    {
        strncpy(key, other.key, sizeof(key));
	value1 = other.value1;
	value2 = other.value2;
	value3 = other.value3;
    }
    return *this;
}

struct_B& struct_B::operator=(const struct_B_msg& msg)
{
    strncpy(key, msg.key().c_str(), sizeof(key));
    value1 = msg.value1();
    value2 = msg.value2();
    value3 = msg.value3();
    return *this;
}

bool struct_B::operator==(const struct_B& other) const
{
    return (strncmp(key, other.key, sizeof(key)) == 0 &&
	    value1 == other.value1 &&
	    value2 == other.value2 &&
	    value3 == other.value3);
}

bool struct_B::operator<(const struct_B& other) const
{
    return (strncmp(key, other.key, sizeof(key)) < 0 &&
	    value1 < other.value1 &&
	    value2 < other.value2 &&
	    value3 < other.value3);
}

instruction::instruction() :
    msg_(),
    buf_(static_cast<size_t>(msg_.SpaceUsed()))
{ }

instruction::instruction(const instruction& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

instruction& instruction::operator=(const instruction& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
}

void instruction::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction::msg_status instruction::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((is_terminate_msg() && msg_.has_terminate_msg()) ||
	    (is_append_msg() && msg_.has_append_msg()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void instruction::set_terminate_msg(const terminate_msg& msg)
{
    msg_.set_opcode(instruction_msg::TERMINATE);
    *msg_.mutable_terminate_msg() = msg;
}

void instruction::set_append_msg(const append_msg& msg)
{
    msg_.set_opcode(instruction_msg::APPEND);
    *msg_.mutable_append_msg() = msg;
}

result::result() :
     msg_(),
     buf_(static_cast<size_t>(msg_.SpaceUsed()))
{
    msg_.set_opcode(result_msg::CONFIRMATION);
}

result::result(const result& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

result& result::operator=(const result& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
}

void result::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
result::msg_status result::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((is_malformed_message_msg() && msg_.has_malformed_message_msg()) ||
	    (is_invalid_argument_msg() && msg_.has_invalid_argument_msg()) ||
	    (is_confirmation_msg() && msg_.has_confirmation_msg()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void result::set_malformed_message_msg(const malformed_message_msg& msg)
{
    msg_.set_opcode(result_msg::MALFORMED_MESSAGE);
    *msg_.mutable_malformed_message_msg() = msg;
}

void result::set_invalid_argument_msg(const invalid_argument_msg& msg)
{
    msg_.set_opcode(result_msg::INVALID_ARGUMENT);
    *msg_.mutable_invalid_argument_msg() = msg;
}

void result::set_confirmation_msg(const confirmation_msg& msg)
{
    msg_.set_opcode(result_msg::CONFIRMATION);
    *msg_.mutable_confirmation_msg() = msg;
}

} // namespace grid_db
} // namespace simulation_grid
