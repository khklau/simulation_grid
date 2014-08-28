#ifndef SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP
#define SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP

#include <google/protobuf/message.h>
#include <zmq.hpp>
#include "container_msg.pb.h"

namespace simulation_grid {
namespace grid_db {

struct container_value
{
    static const std::size_t MAX_LENGTH = 63;
    container_value();
    container_value(const char* key);
    container_value(const container_value& other);
    ~container_value();
    container_value& operator=(const container_value& other);
    bool operator==(const container_value& other) const;
    bool operator<(const container_value& other) const;
    char c_str[MAX_LENGTH + 1];
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
    inline bool is_exists() { return msg_.opcode() == simulation_grid::grid_db::instruction::EXISTS; }
    inline bool is_read() { return msg_.opcode() == simulation_grid::grid_db::instruction::READ; }
    inline bool is_write() { return msg_.opcode() == simulation_grid::grid_db::instruction::WRITE; }
    inline bool is_process_read_metadata() { return msg_.opcode() == simulation_grid::grid_db::instruction::PROCESS_READ_METADATA; }
    inline bool is_process_write_metadata() { return msg_.opcode() == simulation_grid::grid_db::instruction::PROCESS_WRITE_METADATA; }
    inline bool is_collect_garbage_1() { return msg_.opcode() == simulation_grid::grid_db::instruction::COLLECT_GARBAGE_1; }
    inline bool is_collect_garbage_2() { return msg_.opcode() == simulation_grid::grid_db::instruction::COLLECT_GARBAGE_2; }
    inline bool is_get_reader_token_id() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_READER_TOKEN_ID; }
    inline bool is_get_last_read_revision() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_LAST_READ_REVISION; }
    inline bool is_get_global_oldest_revision_read() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_GLOBAL_OLDEST_REVISION_READ; }
    inline bool is_get_registered_keys() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_REGISTERED_KEYS; }
    inline bool is_get_history_depth() { return msg_.opcode() == simulation_grid::grid_db::instruction::GET_HISTORY_DEPTH; }
    inline const simulation_grid::grid_db::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const simulation_grid::grid_db::exists_instr& get_exists() { return msg_.exists(); }
    inline const simulation_grid::grid_db::read_instr& get_read() { return msg_.read(); }
    inline const simulation_grid::grid_db::write_instr& get_write() { return msg_.write(); }
    inline const simulation_grid::grid_db::process_read_metadata_instr& get_process_read_metadata() { return msg_.process_read_metadata(); }
    inline const simulation_grid::grid_db::process_write_metadata_instr& get_process_write_metadata() { return msg_.process_write_metadata(); }
    inline const simulation_grid::grid_db::collect_garbage_1_instr& get_collect_garbage_1() { return msg_.collect_garbage_1(); }
    inline const simulation_grid::grid_db::collect_garbage_2_instr& get_collect_garbage_2() { return msg_.collect_garbage_2(); }
    inline const simulation_grid::grid_db::get_reader_token_id_instr& get_get_reader_token_id() { return msg_.get_reader_token_id(); }
    inline const simulation_grid::grid_db::get_last_read_revision_instr& get_get_last_read_revision() { return msg_.get_last_read_revision(); }
    inline const simulation_grid::grid_db::get_global_oldest_revision_read_instr& get_get_global_oldest_revision_read() { return msg_.get_global_oldest_revision_read(); }
    inline const simulation_grid::grid_db::get_registered_keys_instr& get_get_registered_keys() { return msg_.get_registered_keys(); }
    inline const simulation_grid::grid_db::get_history_depth_instr& get_get_history_depth() { return msg_.get_history_depth(); }
    void set_terminate(const simulation_grid::grid_db::terminate_instr& instr);
    void set_exists(const simulation_grid::grid_db::exists_instr& instr);
    void set_read(const simulation_grid::grid_db::read_instr& instr);
    void set_write(const simulation_grid::grid_db::write_instr& instr);
    void set_process_read_metadata(const simulation_grid::grid_db::process_read_metadata_instr& instr);
    void set_process_write_metadata(const simulation_grid::grid_db::process_write_metadata_instr& instr);
    void set_collect_garbage_1(const simulation_grid::grid_db::collect_garbage_1_instr& instr);
    void set_collect_garbage_2(const simulation_grid::grid_db::collect_garbage_2_instr& instr);
    void set_get_reader_token_id(const simulation_grid::grid_db::get_reader_token_id_instr& instr);
    void set_get_last_read_revision(const simulation_grid::grid_db::get_last_read_revision_instr& instr);
    void set_get_global_oldest_revision_read(const simulation_grid::grid_db::get_global_oldest_revision_read_instr& instr);
    void set_get_registered_keys(const simulation_grid::grid_db::get_registered_keys_instr& instr);
    void set_get_history_depth(const simulation_grid::grid_db::get_history_depth_instr& instr);
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
    inline bool is_value() { return msg_.opcode() == simulation_grid::grid_db::result::VALUE; }
    inline bool is_key() { return msg_.opcode() == simulation_grid::grid_db::result::KEY; }
    inline bool is_token_id() { return msg_.opcode() == simulation_grid::grid_db::result::TOKEN_ID; }
    inline bool is_revision() { return msg_.opcode() == simulation_grid::grid_db::result::REVISION; }
    inline bool is_key_list() { return msg_.opcode() == simulation_grid::grid_db::result::KEY_LIST; }
    inline bool is_size() { return msg_.opcode() == simulation_grid::grid_db::result::SIZE; }
    inline const simulation_grid::grid_db::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const simulation_grid::grid_db::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const simulation_grid::grid_db::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const simulation_grid::grid_db::predicate_result& get_predicate() { return msg_.predicate(); }
    inline const simulation_grid::grid_db::value_result& get_value() { return msg_.value(); }
    inline const simulation_grid::grid_db::key_result& get_key() { return msg_.key(); }
    inline const simulation_grid::grid_db::token_id_result& get_token_id() { return msg_.token_id(); }
    inline const simulation_grid::grid_db::revision_result& get_revision() { return msg_.revision(); }
    inline const simulation_grid::grid_db::key_list_result& get_key_list() { return msg_.key_list(); }
    inline const simulation_grid::grid_db::size_result& get_size() { return msg_.size(); }
    void set_malformed_message(const simulation_grid::grid_db::malformed_message_result& result);
    void set_invalid_argument(const simulation_grid::grid_db::invalid_argument_result& result);
    void set_confirmation(const simulation_grid::grid_db::confirmation_result& result);
    void set_predicate(const simulation_grid::grid_db::predicate_result& result);
    void set_value(const simulation_grid::grid_db::value_result& result);
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
