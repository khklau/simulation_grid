#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "container_msg.hpp"

namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

container_value::container_value()
{
    c_str[0] = '\0';
}

container_value::container_value(const char* key)
{
    if (UNLIKELY_EXT(strlen(key) > MAX_LENGTH))
    {
        throw grid_db_error("Maximum value length exceeded")
                << info_component_identity("container_value")
                << info_data_identity(key);
    }
    strncpy(c_str, key, sizeof(c_str));
}

container_value::container_value(const container_value& other)
{
    strncpy(c_str, other.c_str, sizeof(c_str));
}

container_value::~container_value()
{ }

container_value& container_value::operator=(const container_value& other)
{
    if (this != &other)
    {
        strncpy(c_str, other.c_str, sizeof(c_str));
    }
    return *this;
}

bool container_value::operator==(const container_value& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) == 0;
}

bool container_value::operator<(const container_value& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) < 0;
}

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
	    (is_exists() && msg_.has_exists()) ||
	    (is_read() && msg_.has_read()) ||
	    (is_write() && msg_.has_write()) ||
	    (is_process_read_metadata() && msg_.has_process_read_metadata()) ||
	    (is_process_write_metadata() && msg_.has_process_write_metadata()) ||
	    (is_collect_garbage_1() && msg_.has_collect_garbage_1()) ||
	    (is_collect_garbage_2() && msg_.has_collect_garbage_2()) ||
	    (is_get_reader_token_id() && msg_.has_get_reader_token_id()) ||
	    (is_get_oldest_revision() && msg_.has_get_oldest_revision()) ||
	    (is_get_global_oldest_revision_read() && msg_.has_get_global_oldest_revision_read()) ||
	    (is_get_registered_keys() && msg_.has_get_registered_keys()) ||
	    (is_get_history_depth() && msg_.has_get_history_depth()))
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

void instruction_msg::set_exists(const exists_instr& instr)
{
    msg_.set_opcode(instruction::EXISTS);
    *msg_.mutable_exists() = instr;
}

void instruction_msg::set_read(const read_instr& instr)
{
    msg_.set_opcode(instruction::READ);
    *msg_.mutable_read() = instr;
}

void instruction_msg::set_write(const write_instr& instr)
{
    msg_.set_opcode(instruction::WRITE);
    *msg_.mutable_write() = instr;
}

void instruction_msg::set_process_read_metadata(const process_read_metadata_instr& instr)
{
    msg_.set_opcode(instruction::PROCESS_READ_METADATA);
    *msg_.mutable_process_read_metadata() = instr;
}

void instruction_msg::set_process_write_metadata(const process_write_metadata_instr& instr)
{
    msg_.set_opcode(instruction::PROCESS_WRITE_METADATA);
    *msg_.mutable_process_write_metadata() = instr;
}

void instruction_msg::set_collect_garbage_1(const collect_garbage_1_instr& instr)
{
    msg_.set_opcode(instruction::COLLECT_GARBAGE_1);
    *msg_.mutable_collect_garbage_1() = instr;
}

void instruction_msg::set_collect_garbage_2(const collect_garbage_2_instr& instr)
{
    msg_.set_opcode(instruction::COLLECT_GARBAGE_2);
    *msg_.mutable_collect_garbage_2() = instr;
}

void instruction_msg::set_get_reader_token_id(const get_reader_token_id_instr& instr)
{
    msg_.set_opcode(instruction::GET_READER_TOKEN_ID);
    *msg_.mutable_get_reader_token_id() = instr;
}

void instruction_msg::set_get_oldest_revision(const get_oldest_revision_instr& instr)
{
    msg_.set_opcode(instruction::GET_OLDEST_REVISION);
    *msg_.mutable_get_oldest_revision() = instr;
}

void instruction_msg::set_get_global_oldest_revision_read(const get_global_oldest_revision_read_instr& instr)
{
    msg_.set_opcode(instruction::GET_GLOBAL_OLDEST_REVISION_READ);
    *msg_.mutable_get_global_oldest_revision_read() = instr;
}

void instruction_msg::set_get_registered_keys(const get_registered_keys_instr& instr)
{
    msg_.set_opcode(instruction::GET_REGISTERED_KEYS);
    *msg_.mutable_get_registered_keys() = instr;
}

void instruction_msg::set_get_history_depth(const get_history_depth_instr& instr)
{
    msg_.set_opcode(instruction::GET_HISTORY_DEPTH);
    *msg_.mutable_get_history_depth() = instr;
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
	    (is_confirmation() && msg_.has_confirmation()) ||
	    (is_predicate() && msg_.has_predicate()) ||
	    (is_value() && msg_.has_value()) ||
	    (is_key() && msg_.has_key()) ||
	    (is_token_id() && msg_.has_token_id()) ||
	    (is_revision() && msg_.has_revision()) ||
	    (is_key_list() && msg_.has_key_list()) ||
	    (is_size() && msg_.has_size()))
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

void result_msg::set_predicate(const predicate_result& result)
{
    msg_.set_opcode(result::PREDICATE);
    *msg_.mutable_predicate() = result;
}

void result_msg::set_value(const value_result& result)
{
    msg_.set_opcode(result::VALUE);
    *msg_.mutable_value() = result;
}

void result_msg::set_key(const key_result& result)
{
    msg_.set_opcode(result::KEY);
    *msg_.mutable_key() = result;
}

void result_msg::set_token_id(const token_id_result& result)
{
    msg_.set_opcode(result::TOKEN_ID);
    *msg_.mutable_token_id() = result;
}

void result_msg::set_revision(const revision_result& result)
{
    msg_.set_opcode(result::REVISION);
    *msg_.mutable_revision() = result;
}

void result_msg::set_key_list(const key_list_result& result)
{
    msg_.set_opcode(result::KEY_LIST);
    *msg_.mutable_key_list() = result;
}

void result_msg::set_size(const size_result& result)
{
    msg_.set_opcode(result::SIZE);
    *msg_.mutable_size() = result;
}

} // namespace grid_db
} // namespace simulation_grid
