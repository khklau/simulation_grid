#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "mvcc_service_msg.hpp"

namespace scm = simulation_grid::communication;
namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

string_value::string_value()
{
    c_str[0] = '\0';
}

string_value::string_value(const char* key)
{
    if (UNLIKELY_EXT(strlen(key) > MAX_LENGTH))
    {
        throw grid_db_error("Maximum value length exceeded")
                << info_component_identity("string_value")
                << info_data_identity(key);
    }
    strncpy(c_str, key, sizeof(c_str));
}

string_value::string_value(const string_value& other)
{
    strncpy(c_str, other.c_str, sizeof(c_str));
}

string_value::~string_value()
{ }

string_value& string_value::operator=(const string_value& other)
{
    if (this != &other)
    {
        strncpy(c_str, other.c_str, sizeof(c_str));
    }
    return *this;
}

bool string_value::operator==(const string_value& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) == 0;
}

bool string_value::operator<(const string_value& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) < 0;
}

struct_value::struct_value(bool a, boost::int32_t b, double c) :
	value1(a), value2(b), value3(c)
{ }

struct_value::struct_value(const struct_value& other) :
	value1(other.value1), value2(other.value2), value3(other.value3)
{ }

struct_value::struct_value(const write_struct_instr& instr) :
	value1(instr.value1()), value2(instr.value2()), value3(instr.value3())
{ }

struct_value::~struct_value()
{ }

struct_value& struct_value::operator=(const struct_value& other)
{
    if (this != &other)
    {
	value1 = other.value1;
	value2 = other.value2;
	value3 = other.value3;
    }
    return *this;
}

struct_value& struct_value::operator=(const write_struct_instr& instr)
{
    value1 = instr.value1();
    value2 = instr.value2();
    value3 = instr.value3();
    return *this;
}

bool struct_value::operator==(const struct_value& other) const
{
    return (value1 == other.value1 &&
	    value2 == other.value2 &&
	    value3 == other.value3);
}

bool struct_value::operator<(const struct_value& other) const
{
    return (value1 < other.value1 &&
	    value2 < other.value2 &&
	    value3 < other.value3);
}

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
	    (is_exists_string() && msg_.has_exists_string()) ||
	    (is_exists_struct() && msg_.has_exists_struct()) ||
	    (is_read_string() && msg_.has_read_string()) ||
	    (is_read_struct() && msg_.has_read_struct()) ||
	    (is_write_string() && msg_.has_write_string()) ||
	    (is_write_struct() && msg_.has_write_struct()) ||
	    (is_remove_string() && msg_.has_remove_string()) ||
	    (is_remove_struct() && msg_.has_remove_struct()) ||
	    (is_process_read_metadata() && msg_.has_process_read_metadata()) ||
	    (is_process_write_metadata() && msg_.has_process_write_metadata()) ||
	    (is_collect_garbage_1() && msg_.has_collect_garbage_1()) ||
	    (is_collect_garbage_2() && msg_.has_collect_garbage_2()) ||
	    (is_get_reader_token_id() && msg_.has_get_reader_token_id()) ||
	    (is_get_last_read_revision() && msg_.has_get_last_read_revision()) ||
	    (is_get_oldest_string_revision() && msg_.has_get_oldest_string_revision()) ||
	    (is_get_oldest_struct_revision() && msg_.has_get_oldest_struct_revision()) ||
	    (is_get_global_oldest_revision_read() && msg_.has_get_global_oldest_revision_read()) ||
	    (is_get_registered_keys() && msg_.has_get_registered_keys()) ||
	    (is_get_string_history_depth() && msg_.has_get_string_history_depth()) ||
	    (is_get_struct_history_depth() && msg_.has_get_struct_history_depth()))
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

void instruction_msg::set_exists_string(const exists_string_instr& instr)
{
    msg_.set_opcode(instruction::EXISTS_STRING);
    *msg_.mutable_exists_string() = instr;
}

void instruction_msg::set_exists_struct(const exists_struct_instr& instr)
{
    msg_.set_opcode(instruction::EXISTS_STRUCT);
    *msg_.mutable_exists_struct() = instr;
}

void instruction_msg::set_read_string(const read_string_instr& instr)
{
    msg_.set_opcode(instruction::READ_STRING);
    *msg_.mutable_read_string() = instr;
}

void instruction_msg::set_read_struct(const read_struct_instr& instr)
{
    msg_.set_opcode(instruction::READ_STRUCT);
    *msg_.mutable_read_struct() = instr;
}

void instruction_msg::set_write_string(const write_string_instr& instr)
{
    msg_.set_opcode(instruction::WRITE_STRING);
    *msg_.mutable_write_string() = instr;
}

void instruction_msg::set_write_struct(const write_struct_instr& instr)
{
    msg_.set_opcode(instruction::WRITE_STRUCT);
    *msg_.mutable_write_struct() = instr;
}

void instruction_msg::set_remove_string(const remove_string_instr& instr)
{
    msg_.set_opcode(instruction::REMOVE_STRING);
    *msg_.mutable_remove_string() = instr;
}

void instruction_msg::set_remove_struct(const remove_struct_instr& instr)
{
    msg_.set_opcode(instruction::REMOVE_STRUCT);
    *msg_.mutable_remove_struct() = instr;
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

void instruction_msg::set_get_last_read_revision(const get_last_read_revision_instr& instr)
{
    msg_.set_opcode(instruction::GET_LAST_READ_REVISION);
    *msg_.mutable_get_last_read_revision() = instr;
}

void instruction_msg::set_get_oldest_string_revision(const get_oldest_string_revision_instr& instr)
{
    msg_.set_opcode(instruction::GET_OLDEST_STRING_REVISION);
    *msg_.mutable_get_oldest_string_revision() = instr;
}

void instruction_msg::set_get_oldest_struct_revision(const get_oldest_struct_revision_instr& instr)
{
    msg_.set_opcode(instruction::GET_OLDEST_STRUCT_REVISION);
    *msg_.mutable_get_oldest_struct_revision() = instr;
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

void instruction_msg::set_get_string_history_depth(const get_string_history_depth_instr& instr)
{
    msg_.set_opcode(instruction::GET_STRING_HISTORY_DEPTH);
    *msg_.mutable_get_string_history_depth() = instr;
}

void instruction_msg::set_get_struct_history_depth(const get_struct_history_depth_instr& instr)
{
    msg_.set_opcode(instruction::GET_STRUCT_HISTORY_DEPTH);
    *msg_.mutable_get_struct_history_depth() = instr;
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
	    (is_predicate() && msg_.has_predicate()) ||
	    (is_string_value() && msg_.has_string_value()) ||
	    (is_struct_value() && msg_.has_struct_value()) ||
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

void result_msg::set_string_value(const string_value_result& result)
{
    msg_.set_opcode(result::STRING_VALUE);
    *msg_.mutable_string_value() = result;
}

void result_msg::set_struct_value(const struct_value_result& result)
{
    msg_.set_opcode(result::STRUCT_VALUE);
    *msg_.mutable_struct_value() = result;
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
