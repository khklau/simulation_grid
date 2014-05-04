#include <simulation_grid/core/compiler_extensions.hpp>
#include "ringbuf_msg.hpp"

namespace sgd = simulation_grid::grid_db;

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
