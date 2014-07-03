#ifndef SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP
#define SIMULATION_GRID_GRID_DB_CONTAINER_MSG_HPP

#include <google/protobuf/message.h>
#include <zmq.hpp>
#include "container_msg.pb.h"

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
    inline bool is_write() { return msg_.opcode() == simulation_grid::grid_db::instruction::PUSH_FRONT; }
    inline const simulation_grid::grid_db::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const simulation_grid::grid_db::write_instr& get_write() { return msg_.write(); }
    void set_terminate(const simulation_grid::grid_db::terminate_instr& instr);
    void set_write(const simulation_grid::grid_db::write_instr& instr);
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
    inline const simulation_grid::grid_db::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const simulation_grid::grid_db::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const simulation_grid::grid_db::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    void set_malformed_message(const simulation_grid::grid_db::malformed_message_result& result);
    void set_invalid_argument(const simulation_grid::grid_db::invalid_argument_result& result);
    void set_confirmation(const simulation_grid::grid_db::confirmation_result& result);
private:
    simulation_grid::grid_db::result msg_;
    zmq::message_t buf_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
