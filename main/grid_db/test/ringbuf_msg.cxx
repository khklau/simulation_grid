#include <simulation_grid/core/compiler_extensions.hpp>
#include "ringbuf_msg.hpp"

namespace scm = simulation_grid::communication;
namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

instruction_msg::instruction_msg() :
    msg_()
{ }

instruction_msg::instruction_msg(const instruction_msg& other) :
    msg_(other.msg_)
{ }

instruction_msg& instruction_msg::operator=(const instruction_msg& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
    }
    return *this;
}

void instruction_msg::serialize(scm::message_sink& sink) const
{
    std::size_t required_size = msg_.ByteSize();
    msg_.SerializeToArray(sink.data(required_size), required_size);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction_msg::msg_status instruction_msg::deserialize(const scm::message_source& source)
{
    msg_status status = MALFORMED;
    if (msg_.ParseFromArray(source.data(), source.size()))
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

void instruction_msg::set_terminate(const terminate_instr& instr)
{
    msg_.set_opcode(instruction::TERMINATE);
    *msg_.mutable_terminate() = instr;
}

void instruction_msg::set_query_front(const query_front_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_FRONT);
    *msg_.mutable_query_front() = instr;
}

void instruction_msg::set_query_back(const query_back_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_BACK);
    *msg_.mutable_query_back() = instr;
}

void instruction_msg::set_query_capacity(const query_capacity_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_CAPACITY);
    *msg_.mutable_query_capacity() = instr;
}

void instruction_msg::set_query_count(const query_count_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_COUNT);
    *msg_.mutable_query_count() = instr;
}

void instruction_msg::set_query_empty(const query_empty_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_EMPTY);
    *msg_.mutable_query_empty() = instr;
}

void instruction_msg::set_query_full(const query_full_instr& instr)
{
    msg_.set_opcode(instruction::QUERY_FULL);
    *msg_.mutable_query_full() = instr;
}

void instruction_msg::set_push_front(const push_front_instr& instr)
{
    msg_.set_opcode(instruction::PUSH_FRONT);
    *msg_.mutable_push_front() = instr;
}

void instruction_msg::set_pop_back(const pop_back_instr& instr)
{
    msg_.set_opcode(instruction::POP_BACK);
    *msg_.mutable_pop_back() = instr;
}

void instruction_msg::set_export_element(const export_element_instr& instr)
{
    msg_.set_opcode(instruction::EXPORT_ELEMENT);
    *msg_.mutable_export_element() = instr;
}

result_msg::result_msg() :
     msg_()
{
    msg_.set_opcode(result::CONFIRMATION);
}

result_msg::result_msg(const result_msg& other) :
    msg_(other.msg_)
{ }

result_msg& result_msg::operator=(const result_msg& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
    }
    return *this;
}

void result_msg::serialize(scm::message_sink& sink) const
{
    std::size_t required_size = msg_.ByteSize();
    msg_.SerializeToArray(sink.data(required_size), required_size);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
result_msg::msg_status result_msg::deserialize(const scm::message_source& source)
{
    msg_status status = MALFORMED;
    if (msg_.ParseFromArray(source.data(), source.size()))
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

void result_msg::set_element(const element_result& result)
{
    msg_.set_opcode(result::ELEMENT);
    *msg_.mutable_element() = result;
}

void result_msg::set_size(const size_result& result)
{
    msg_.set_opcode(result::SIZE);
    *msg_.mutable_size() = result;
}

void result_msg::set_predicate(const predicate_result& result)
{
    msg_.set_opcode(result::PREDICATE);
    *msg_.mutable_predicate() = result;
}

} // namespace grid_db
} // namespace simulation_grid
