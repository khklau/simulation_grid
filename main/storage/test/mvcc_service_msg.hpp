#ifndef SUPERNOVA_STORAGE_MVCC_SERVICE_MSG_HPP
#define SUPERNOVA_STORAGE_MVCC_SERVICE_MSG_HPP

#include "mvcc_service_msg.pb.h"
#include <google/protobuf/message.h>
#include <supernova/communication/message_access.hpp>

namespace supernova {
namespace storage {

struct string_value
{
    static const std::size_t MAX_LENGTH = 63;
    string_value();
    string_value(const char* key);
    string_value(const string_value& other);
    ~string_value();
    string_value& operator=(const string_value& other);
    bool operator==(const string_value& other) const;
    bool operator<(const string_value& other) const;
    char c_str[MAX_LENGTH + 1];
};

struct struct_value
{
    struct_value(bool a = true, boost::int32_t b = 0, double c = 0.0F);
    struct_value(const struct_value& other);
    struct_value(const write_struct_instr& instr);
    ~struct_value();
    struct_value& operator=(const struct_value& other);
    struct_value& operator=(const write_struct_instr& instr);
    bool operator==(const struct_value& other) const;
    bool operator<(const struct_value& other) const;
    bool value1;
    boost::int32_t value2;
    double value3;
};

class instruction_msg
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    instruction_msg();
    instruction_msg(const instruction_msg& other);
    instruction_msg& operator=(const instruction_msg& other);
    void serialize(supernova::communication::message_sink& sink) const;
    msg_status deserialize(const supernova::communication::message_source& source);
    inline bool is_terminate() { return msg_.opcode() == supernova::storage::instruction::TERMINATE; }
    inline bool is_exists_string() { return msg_.opcode() == supernova::storage::instruction::EXISTS_STRING; }
    inline bool is_exists_struct() { return msg_.opcode() == supernova::storage::instruction::EXISTS_STRUCT; }
    inline bool is_read_string() { return msg_.opcode() == supernova::storage::instruction::READ_STRING; }
    inline bool is_read_struct() { return msg_.opcode() == supernova::storage::instruction::READ_STRUCT; }
    inline bool is_write_string() { return msg_.opcode() == supernova::storage::instruction::WRITE_STRING; }
    inline bool is_write_struct() { return msg_.opcode() == supernova::storage::instruction::WRITE_STRUCT; }
    inline bool is_remove_string() { return msg_.opcode() == supernova::storage::instruction::REMOVE_STRING; }
    inline bool is_remove_struct() { return msg_.opcode() == supernova::storage::instruction::REMOVE_STRUCT; }
    inline bool is_process_read_metadata() { return msg_.opcode() == supernova::storage::instruction::PROCESS_READ_METADATA; }
    inline bool is_process_write_metadata() { return msg_.opcode() == supernova::storage::instruction::PROCESS_WRITE_METADATA; }
    inline bool is_collect_garbage_1() { return msg_.opcode() == supernova::storage::instruction::COLLECT_GARBAGE_1; }
    inline bool is_collect_garbage_2() { return msg_.opcode() == supernova::storage::instruction::COLLECT_GARBAGE_2; }
    inline bool is_get_reader_token_id() { return msg_.opcode() == supernova::storage::instruction::GET_READER_TOKEN_ID; }
    inline bool is_get_last_read_revision() { return msg_.opcode() == supernova::storage::instruction::GET_LAST_READ_REVISION; }
    inline bool is_get_oldest_string_revision() { return msg_.opcode() == supernova::storage::instruction::GET_OLDEST_STRING_REVISION; }
    inline bool is_get_oldest_struct_revision() { return msg_.opcode() == supernova::storage::instruction::GET_OLDEST_STRUCT_REVISION; }
    inline bool is_get_global_oldest_revision_read() { return msg_.opcode() == supernova::storage::instruction::GET_GLOBAL_OLDEST_REVISION_READ; }
    inline bool is_get_registered_keys() { return msg_.opcode() == supernova::storage::instruction::GET_REGISTERED_KEYS; }
    inline bool is_get_string_history_depth() { return msg_.opcode() == supernova::storage::instruction::GET_STRING_HISTORY_DEPTH; }
    inline bool is_get_struct_history_depth() { return msg_.opcode() == supernova::storage::instruction::GET_STRUCT_HISTORY_DEPTH; }
    inline const supernova::storage::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const supernova::storage::exists_string_instr& get_exists_string() { return msg_.exists_string(); }
    inline const supernova::storage::exists_struct_instr& get_exists_struct() { return msg_.exists_struct(); }
    inline const supernova::storage::read_string_instr& get_read_string() { return msg_.read_string(); }
    inline const supernova::storage::read_struct_instr& get_read_struct() { return msg_.read_struct(); }
    inline const supernova::storage::write_string_instr& get_write_string() { return msg_.write_string(); }
    inline const supernova::storage::write_struct_instr& get_write_struct() { return msg_.write_struct(); }
    inline const supernova::storage::remove_string_instr& get_remove_string() { return msg_.remove_string(); }
    inline const supernova::storage::remove_struct_instr& get_remove_struct() { return msg_.remove_struct(); }
    inline const supernova::storage::process_read_metadata_instr& get_process_read_metadata() { return msg_.process_read_metadata(); }
    inline const supernova::storage::process_write_metadata_instr& get_process_write_metadata() { return msg_.process_write_metadata(); }
    inline const supernova::storage::collect_garbage_1_instr& get_collect_garbage_1() { return msg_.collect_garbage_1(); }
    inline const supernova::storage::collect_garbage_2_instr& get_collect_garbage_2() { return msg_.collect_garbage_2(); }
    inline const supernova::storage::get_reader_token_id_instr& get_get_reader_token_id() { return msg_.get_reader_token_id(); }
    inline const supernova::storage::get_last_read_revision_instr& get_get_last_read_revision() { return msg_.get_last_read_revision(); }
    inline const supernova::storage::get_oldest_string_revision_instr& get_get_oldest_string_revision() { return msg_.get_oldest_string_revision(); }
    inline const supernova::storage::get_oldest_struct_revision_instr& get_get_oldest_struct_revision() { return msg_.get_oldest_struct_revision(); }
    inline const supernova::storage::get_global_oldest_revision_read_instr& get_get_global_oldest_revision_read() { return msg_.get_global_oldest_revision_read(); }
    inline const supernova::storage::get_registered_keys_instr& get_get_registered_keys() { return msg_.get_registered_keys(); }
    inline const supernova::storage::get_string_history_depth_instr& get_get_string_history_depth() { return msg_.get_string_history_depth(); }
    inline const supernova::storage::get_struct_history_depth_instr& get_get_struct_history_depth() { return msg_.get_struct_history_depth(); }
    void set_terminate(const supernova::storage::terminate_instr& instr);
    void set_exists_string(const supernova::storage::exists_string_instr& instr);
    void set_exists_struct(const supernova::storage::exists_struct_instr& instr);
    void set_read_string(const supernova::storage::read_string_instr& instr);
    void set_read_struct(const supernova::storage::read_struct_instr& instr);
    void set_write_string(const supernova::storage::write_string_instr& instr);
    void set_write_struct(const supernova::storage::write_struct_instr& instr);
    void set_remove_string(const supernova::storage::remove_string_instr& instr);
    void set_remove_struct(const supernova::storage::remove_struct_instr& instr);
    void set_process_read_metadata(const supernova::storage::process_read_metadata_instr& instr);
    void set_process_write_metadata(const supernova::storage::process_write_metadata_instr& instr);
    void set_collect_garbage_1(const supernova::storage::collect_garbage_1_instr& instr);
    void set_collect_garbage_2(const supernova::storage::collect_garbage_2_instr& instr);
    void set_get_reader_token_id(const supernova::storage::get_reader_token_id_instr& instr);
    void set_get_last_read_revision(const supernova::storage::get_last_read_revision_instr& instr);
    void set_get_oldest_string_revision(const supernova::storage::get_oldest_string_revision_instr& instr);
    void set_get_oldest_struct_revision(const supernova::storage::get_oldest_struct_revision_instr& instr);
    void set_get_global_oldest_revision_read(const supernova::storage::get_global_oldest_revision_read_instr& instr);
    void set_get_registered_keys(const supernova::storage::get_registered_keys_instr& instr);
    void set_get_string_history_depth(const supernova::storage::get_string_history_depth_instr& instr);
    void set_get_struct_history_depth(const supernova::storage::get_struct_history_depth_instr& instr);
private:
    supernova::storage::instruction msg_;
};

class result_msg
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    result_msg();
    result_msg(const result_msg& other);
    result_msg& operator=(const result_msg& other);
    void serialize(supernova::communication::message_sink& sink) const;
    msg_status deserialize(const supernova::communication::message_source& source);
    inline bool is_malformed_message() { return msg_.opcode() == supernova::storage::result::MALFORMED_MESSAGE; }
    inline bool is_invalid_argument() { return msg_.opcode() == supernova::storage::result::INVALID_ARGUMENT; }
    inline bool is_confirmation() { return msg_.opcode() == supernova::storage::result::CONFIRMATION; }
    inline bool is_predicate() { return msg_.opcode() == supernova::storage::result::PREDICATE; }
    inline bool is_string_value() { return msg_.opcode() == supernova::storage::result::STRING_VALUE; }
    inline bool is_struct_value() { return msg_.opcode() == supernova::storage::result::STRUCT_VALUE; }
    inline bool is_key() { return msg_.opcode() == supernova::storage::result::KEY; }
    inline bool is_token_id() { return msg_.opcode() == supernova::storage::result::TOKEN_ID; }
    inline bool is_revision() { return msg_.opcode() == supernova::storage::result::REVISION; }
    inline bool is_key_list() { return msg_.opcode() == supernova::storage::result::KEY_LIST; }
    inline bool is_size() { return msg_.opcode() == supernova::storage::result::SIZE; }
    inline const supernova::storage::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const supernova::storage::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const supernova::storage::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const supernova::storage::predicate_result& get_predicate() { return msg_.predicate(); }
    inline const supernova::storage::string_value_result& get_string_value() { return msg_.string_value(); }
    inline const supernova::storage::struct_value_result& get_struct_value() { return msg_.struct_value(); }
    inline const supernova::storage::key_result& get_key() { return msg_.key(); }
    inline const supernova::storage::token_id_result& get_token_id() { return msg_.token_id(); }
    inline const supernova::storage::revision_result& get_revision() { return msg_.revision(); }
    inline const supernova::storage::key_list_result& get_key_list() { return msg_.key_list(); }
    inline const supernova::storage::size_result& get_size() { return msg_.size(); }
    void set_malformed_message(const supernova::storage::malformed_message_result& result);
    void set_invalid_argument(const supernova::storage::invalid_argument_result& result);
    void set_confirmation(const supernova::storage::confirmation_result& result);
    void set_predicate(const supernova::storage::predicate_result& result);
    void set_string_value(const supernova::storage::string_value_result& result);
    void set_struct_value(const supernova::storage::struct_value_result& result);
    void set_key(const supernova::storage::key_result& result);
    void set_token_id(const supernova::storage::token_id_result& result);
    void set_revision(const supernova::storage::revision_result& result);
    void set_key_list(const supernova::storage::key_list_result& result);
    void set_size(const supernova::storage::size_result& result);
private:
    supernova::storage::result msg_;
};

} // namespace storage
} // namespace supernova

#endif
