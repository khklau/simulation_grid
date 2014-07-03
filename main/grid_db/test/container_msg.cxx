#include <simulation_grid/core/compiler_extensions.hpp>
#include "container_msg.hpp"

namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

instruction_msg::instruction_msg() :
    msg_(),
    buf_(static_cast<size_t>(msg_.SpaceUsed()))
{ }

instruction_msg::instruction_msg(const instruction_msg& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

instruction_msg& instruction_msg::operator=(const instruction_msg& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
}

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
	    (is_write() && msg_.has_write()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void instruction_msg::set_terminate(const terminate_instr& instr)
{
    msg_.set_opcode(instruction::TERMINATE);
    *msg_.mutable_terminate() = instr;
}

void instruction_msg::set_write(const write_instr& instr)
{
    msg_.set_opcode(instruction::PUSH_FRONT);
    *msg_.mutable_write() = instr;
}

result_msg::result_msg() :
     msg_(),
     buf_(static_cast<size_t>(msg_.SpaceUsed()))
{
    msg_.set_opcode(result::CONFIRMATION);
}

result_msg::result_msg(const result_msg& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

result_msg& result_msg::operator=(const result_msg& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
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
	    (is_confirmation() && msg_.has_confirmation()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void result_msg::set_malformed_message(const malformed_message_result& result)
{
    msg_.set_opcode(result::MALFORMED_MESSAGE);
    *msg_.mutable_malformed_message() = result;
}

void result_msg::set_invalid_argument(const invalid_argument_result& result)
{
    msg_.set_opcode(result::INVALID_ARGUMENT);
    *msg_.mutable_invalid_argument() = result;
}

void result_msg::set_confirmation(const confirmation_result& result)
{
    msg_.set_opcode(result::CONFIRMATION);
    *msg_.mutable_confirmation() = result;
}

} // namespace grid_db
} // namespace simulation_grid
