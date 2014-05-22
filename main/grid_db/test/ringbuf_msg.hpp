#ifndef SIMULATION_GRID_GRID_DB_RINGBUF_MSG_HPP
#define SIMULATION_GRID_GRID_DB_RINGBUF_MSG_HPP

#include <google/protobuf/message.h>
#include <zmq.hpp>
#include "multi_reader_ring_buffer_slave.pb.h"

namespace simulation_grid {
namespace grid_db {

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
    inline bool is_query_front() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_FRONT; }
    inline bool is_query_back() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_BACK; }
    inline bool is_query_capacity() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_CAPACITY; }
    inline bool is_query_count() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_COUNT; }
    inline bool is_query_empty() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_EMPTY; }
    inline bool is_query_full() { return msg_.opcode() == simulation_grid::grid_db::instruction::QUERY_FULL; }
    inline bool is_push_front() { return msg_.opcode() == simulation_grid::grid_db::instruction::PUSH_FRONT; }
    inline bool is_pop_back() { return msg_.opcode() == simulation_grid::grid_db::instruction::POP_BACK; }
    inline bool is_export_element() { return msg_.opcode() == simulation_grid::grid_db::instruction::EXPORT_ELEMENT; }
    inline const simulation_grid::grid_db::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const simulation_grid::grid_db::query_front_instr& get_query_front() { return msg_.query_front(); }
    inline const simulation_grid::grid_db::query_back_instr& get_query_back() { return msg_.query_back(); }
    inline const simulation_grid::grid_db::query_capacity_instr& get_query_capacity() { return msg_.query_capacity(); }
    inline const simulation_grid::grid_db::query_count_instr& get_query_count() { return msg_.query_count(); }
    inline const simulation_grid::grid_db::query_empty_instr& get_query_empty() { return msg_.query_empty(); }
    inline const simulation_grid::grid_db::query_full_instr& get_query_full() { return msg_.query_full(); }
    inline const simulation_grid::grid_db::push_front_instr& get_push_front() { return msg_.push_front(); }
    inline const simulation_grid::grid_db::pop_back_instr& get_pop_back() { return msg_.pop_back(); }
    inline const simulation_grid::grid_db::export_element_instr& get_export_element() { return msg_.export_element(); }
    void set_terminate(const simulation_grid::grid_db::terminate_instr& instr);
    void set_query_front(const simulation_grid::grid_db::query_front_instr& instr);
    void set_query_back(const simulation_grid::grid_db::query_back_instr& instr);
    void set_query_capacity(const simulation_grid::grid_db::query_capacity_instr& instr);
    void set_query_count(const simulation_grid::grid_db::query_count_instr& instr);
    void set_query_empty(const simulation_grid::grid_db::query_empty_instr& instr);
    void set_query_full(const simulation_grid::grid_db::query_full_instr& instr);
    void set_push_front(const simulation_grid::grid_db::push_front_instr& instr);
    void set_pop_back(const simulation_grid::grid_db::pop_back_instr& instr);
    void set_export_element(const simulation_grid::grid_db::export_element_instr& instr);
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
    inline bool is_element() { return msg_.opcode() == simulation_grid::grid_db::result::ELEMENT; }
    inline bool is_size() { return msg_.opcode() == simulation_grid::grid_db::result::SIZE; }
    inline bool is_predicate() { return msg_.opcode() == simulation_grid::grid_db::result::PREDICATE; }
    inline const simulation_grid::grid_db::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const simulation_grid::grid_db::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const simulation_grid::grid_db::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const simulation_grid::grid_db::element_result& get_element() { return msg_.element(); }
    inline const simulation_grid::grid_db::size_result& get_size() { return msg_.size(); }
    inline const simulation_grid::grid_db::predicate_result& get_predicate() { return msg_.predicate(); }
    void set_malformed_message(const simulation_grid::grid_db::malformed_message_result& result);
    void set_invalid_argument(const simulation_grid::grid_db::invalid_argument_result& result);
    void set_confirmation(const simulation_grid::grid_db::confirmation_result& result);
    void set_element(const simulation_grid::grid_db::element_result& result);
    void set_size(const simulation_grid::grid_db::size_result& result);
    void set_predicate(const simulation_grid::grid_db::predicate_result& result);
private:
    simulation_grid::grid_db::result msg_;
    zmq::message_t buf_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
