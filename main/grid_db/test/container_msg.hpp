#ifndef SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP
#define SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP

#include <google/protobuf/message.h>
#include <zmq.hpp>
#include "container_msg.pb.h"

namespace simulation_grid {
namespace grid_db {

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
    void serialize(zmq::socket_t& socket);
    msg_status deserialize(zmq::socket_t& socket);
    inline bool is_terminate() { return msg_.opcode() == simulation_grid::grid_db::instruction::TERMINATE; }
    inline bool is_exists_string() { return msg_.opcode() == simulation_grid::grid_db::instruction::EXISTS_STRING; }
    inline bool is_exists_struct() { return msg_.opcode() == simulation_grid::grid_db::instruction::EXISTS_STRUCT; }
    inline bool is_read_string() { return msg_.opcode() == simulation_grid::grid_db::instruction::READ_STRING; }
    inline bool is_read_struct() { return msg_.opcode() == simulation_grid::grid_db::instruction::READ_STRUCT; }
    inline bool is_write_string() { return msg_.opcode() == simulation_grid::grid_db::instruction::WRITE_STRING; }
    inline bool is_write_struct() { return msg_.opcode() == simulation_grid::grid_db::instruction::WRITE_STRUCT; }
    inline bool is_remove_string() { return msg_.opcode() == simulation_grid::grid_db::instruction::REMOVE_STRING; }
    inline bool is_remove_struct() { return msg_.opcode() == simulation_grid::grid_db::instruction::REMOVE_STRUCT; }
    inline bool is_process_read_metadata() { return msg_.opcode() == simulation_grid::grid_db::instruction::PROCESS_READ_METADATA; }
    inline bool is_process_write_metadata() { return msg_.opcode() == simulation_grid::grid_db::instruction::PROCESS_WRITE_METADATA; }
    inline bool is_collect_garbage_1() { return msg_.opcode() == simulation_grid::grid_db::instruction::COLLECT_GARBAGE_1; }
    inline bool is_collect_garbage_2() { return msg_.opcode() == simulation_grid::grid_db::instruction::COLLECT_GARBAGE_2; }
    inline bool is_get_reader_token_id() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_READER_TOKEN_ID; }
    inline bool is_get_last_read_revision() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_LAST_READ_REVISION; }
    inline bool is_get_oldest_string_revision() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_OLDEST_STRING_REVISION; }
    inline bool is_get_oldest_struct_revision() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_OLDEST_STRUCT_REVISION; }
    inline bool is_get_global_oldest_revision_read() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_GLOBAL_OLDEST_REVISION_READ; }
    inline bool is_get_registered_keys() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_REGISTERED_KEYS; }
    inline bool is_get_string_history_depth() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_STRING_HISTORY_DEPTH; }
    inline bool is_get_struct_history_depth() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_STRUCT_HISTORY_DEPTH; }
    inline const simulation_grid::grid_db::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const simulation_grid::grid_db::exists_string_instr& get_exists_string() { return msg_.exists_string(); }
    inline const simulation_grid::grid_db::exists_struct_instr& get_exists_struct() { return msg_.exists_struct(); }
    inline const simulation_grid::grid_db::read_string_instr& get_read_string() { return msg_.read_string(); }
    inline const simulation_grid::grid_db::read_struct_instr& get_read_struct() { return msg_.read_struct(); }
    inline const simulation_grid::grid_db::write_string_instr& get_write_string() { return msg_.write_string(); }
    inline const simulation_grid::grid_db::write_struct_instr& get_write_struct() { return msg_.write_struct(); }
    inline const simulation_grid::grid_db::remove_string_instr& get_remove_string() { return msg_.remove_string(); }
    inline const simulation_grid::grid_db::remove_struct_instr& get_remove_struct() { return msg_.remove_struct(); }
    inline const simulation_grid::grid_db::process_read_metadata_instr& get_process_read_metadata() { return msg_.process_read_metadata(); }
    inline const simulation_grid::grid_db::process_write_metadata_instr& get_process_write_metadata() { return msg_.process_write_metadata(); }
    inline const simulation_grid::grid_db::collect_garbage_1_instr& get_collect_garbage_1() { return msg_.collect_garbage_1(); }
    inline const simulation_grid::grid_db::collect_garbage_2_instr& get_collect_garbage_2() { return msg_.collect_garbage_2(); }
    inline const simulation_grid::grid_db::get_reader_token_id_instr& get_get_reader_token_id() { return msg_.get_reader_token_id(); }
    inline const simulation_grid::grid_db::get_last_read_revision_instr& get_get_last_read_revision() { return msg_.get_last_read_revision(); }
    inline const simulation_grid::grid_db::get_oldest_string_revision_instr& get_get_oldest_string_revision() { return msg_.get_oldest_string_revision(); }
    inline const simulation_grid::grid_db::get_oldest_struct_revision_instr& get_get_oldest_struct_revision() { return msg_.get_oldest_struct_revision(); }
    inline const simulation_grid::grid_db::get_global_oldest_revision_read_instr& get_get_global_oldest_revision_read() { return msg_.get_global_oldest_revision_read(); }
    inline const simulation_grid::grid_db::get_registered_keys_instr& get_get_registered_keys() { return msg_.get_registered_keys(); }
    inline const simulation_grid::grid_db::get_string_history_depth_instr& get_get_string_history_depth() { return msg_.get_string_history_depth(); }
    inline const simulation_grid::grid_db::get_struct_history_depth_instr& get_get_struct_history_depth() { return msg_.get_struct_history_depth(); }
    void set_terminate(const simulation_grid::grid_db::terminate_instr& instr);
    void set_exists_string(const simulation_grid::grid_db::exists_string_instr& instr);
    void set_exists_struct(const simulation_grid::grid_db::exists_struct_instr& instr);
    void set_read_string(const simulation_grid::grid_db::read_string_instr& instr);
    void set_read_struct(const simulation_grid::grid_db::read_struct_instr& instr);
    void set_write_string(const simulation_grid::grid_db::write_string_instr& instr);
    void set_write_struct(const simulation_grid::grid_db::write_struct_instr& instr);
    void set_remove_string(const simulation_grid::grid_db::remove_string_instr& instr);
    void set_remove_struct(const simulation_grid::grid_db::remove_struct_instr& instr);
    void set_process_read_metadata(const simulation_grid::grid_db::process_read_metadata_instr& instr);
    void set_process_write_metadata(const simulation_grid::grid_db::process_write_metadata_instr& instr);
    void set_collect_garbage_1(const simulation_grid::grid_db::collect_garbage_1_instr& instr);
    void set_collect_garbage_2(const simulation_grid::grid_db::collect_garbage_2_instr& instr);
    void set_get_reader_token_id(const simulation_grid::grid_db::get_reader_token_id_instr& instr);
    void set_get_last_read_revision(const simulation_grid::grid_db::get_last_read_revision_instr& instr);
    void set_get_oldest_string_revision(const simulation_grid::grid_db::get_oldest_string_revision_instr& instr);
    void set_get_oldest_struct_revision(const simulation_grid::grid_db::get_oldest_struct_revision_instr& instr);
    void set_get_global_oldest_revision_read(const simulation_grid::grid_db::get_global_oldest_revision_read_instr& instr);
    void set_get_registered_keys(const simulation_grid::grid_db::get_registered_keys_instr& instr);
    void set_get_string_history_depth(const simulation_grid::grid_db::get_string_history_depth_instr& instr);
    void set_get_struct_history_depth(const simulation_grid::grid_db::get_struct_history_depth_instr& instr);
private:
    simulation_grid::grid_db::instruction msg_;
    zmq::message_t buf_;
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
    void serialize(zmq::socket_t& socket);
    msg_status deserialize(zmq::socket_t& socket);
    inline bool is_malformed_message() { return msg_.opcode() == simulation_grid::grid_db::result::MALFORMED_MESSAGE; }
    inline bool is_invalid_argument() { return msg_.opcode() == simulation_grid::grid_db::result::INVALID_ARGUMENT; }
    inline bool is_confirmation() { return msg_.opcode() == simulation_grid::grid_db::result::CONFIRMATION; }
    inline bool is_predicate() { return msg_.opcode() == simulation_grid::grid_db::result::PREDICATE; }
    inline bool is_string_value() { return msg_.opcode() == simulation_grid::grid_db::result::STRING_VALUE; }
    inline bool is_struct_value() { return msg_.opcode() == simulation_grid::grid_db::result::STRUCT_VALUE; }
    inline bool is_key() { return msg_.opcode() == simulation_grid::grid_db::result::KEY; }
    inline bool is_token_id() { return msg_.opcode() == simulation_grid::grid_db::result::TOKEN_ID; }
    inline bool is_revision() { return msg_.opcode() == simulation_grid::grid_db::result::REVISION; }
    inline bool is_key_list() { return msg_.opcode() == simulation_grid::grid_db::result::KEY_LIST; }
    inline bool is_size() { return msg_.opcode() == simulation_grid::grid_db::result::SIZE; }
    inline const simulation_grid::grid_db::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const simulation_grid::grid_db::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const simulation_grid::grid_db::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const simulation_grid::grid_db::predicate_result& get_predicate() { return msg_.predicate(); }
    inline const simulation_grid::grid_db::string_value_result& get_string_value() { return msg_.string_value(); }
    inline const simulation_grid::grid_db::struct_value_result& get_struct_value() { return msg_.struct_value(); }
    inline const simulation_grid::grid_db::key_result& get_key() { return msg_.key(); }
    inline const simulation_grid::grid_db::token_id_result& get_token_id() { return msg_.token_id(); }
    inline const simulation_grid::grid_db::revision_result& get_revision() { return msg_.revision(); }
    inline const simulation_grid::grid_db::key_list_result& get_key_list() { return msg_.key_list(); }
    inline const simulation_grid::grid_db::size_result& get_size() { return msg_.size(); }
    void set_malformed_message(const simulation_grid::grid_db::malformed_message_result& result);
    void set_invalid_argument(const simulation_grid::grid_db::invalid_argument_result& result);
    void set_confirmation(const simulation_grid::grid_db::confirmation_result& result);
    void set_predicate(const simulation_grid::grid_db::predicate_result& result);
    void set_string_value(const simulation_grid::grid_db::string_value_result& result);
    void set_struct_value(const simulation_grid::grid_db::struct_value_result& result);
    void set_key(const simulation_grid::grid_db::key_result& result);
    void set_token_id(const simulation_grid::grid_db::token_id_result& result);
    void set_revision(const simulation_grid::grid_db::revision_result& result);
    void set_key_list(const simulation_grid::grid_db::key_list_result& result);
    void set_size(const simulation_grid::grid_db::size_result& result);
private:
    simulation_grid::grid_db::result msg_;
    zmq::message_t buf_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
