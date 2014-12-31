#ifndef SIMULATION_GRID_GRID_DB_LOG_SERVICE_MSG_HPP
#define SIMULATION_GRID_GRID_DB_LOG_SERVICE_MSG_HPP

#include "log_service_msg.pb.h"
#include <boost/variant.hpp>
#include <google/protobuf/message.h>
#include <simulation_grid/communication/message_access.hpp>

namespace simulation_grid {
namespace grid_db {

struct struct_A
{
    static const std::size_t MAX_LENGTH = 63;
    struct_A(const char* k = "", const char* v = "");
    struct_A(const struct_A& other);
    struct_A(const struct_A_msg& msg);
    ~struct_A();
    struct_A& operator=(const struct_A& other);
    struct_A& operator=(const struct_A_msg& msg);
    bool operator==(const struct_A& other) const;
    void export_to(struct_A_msg& target) const;
    char key[MAX_LENGTH + 1];
    char value[MAX_LENGTH + 1];
};

struct struct_B
{
    static const std::size_t MAX_LENGTH = 63;
    struct_B(const char* k = "", bool v1 = true, boost::int32_t v2 = 0, double v3 = 0.0F);
    struct_B(const struct_B& other);
    struct_B(const struct_B_msg& msg);
    ~struct_B();
    struct_B& operator=(const struct_B& other);
    struct_B& operator=(const struct_B_msg& msg);
    bool operator==(const struct_B& other) const;
    void export_to(struct_B_msg& target) const;
    char key[MAX_LENGTH + 1];
    bool value1;
    boost::int32_t value2;
    double value3;
};

struct union_AB
{
    union_AB(const struct_A& a);
    union_AB(const struct_B& b);
    union_AB(const union_AB& other);
    union_AB(const union_AB_msg& msg);
    ~union_AB();
    union_AB& operator=(const union_AB& other);
    union_AB& operator=(const union_AB_msg& msg);
    bool operator==(const union_AB& other) const;
    void export_to(union_AB_msg& target) const;
    boost::variant<struct_A, struct_B> value;
};

class instruction
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    instruction();
    instruction(const instruction& other);
    instruction& operator=(const instruction& other);
    inline std::size_t get_size() const { return msg_.ByteSize(); }
    void serialize(simulation_grid::communication::message_sink& sink) const;
    msg_status deserialize(const simulation_grid::communication::message_source& source);
    inline bool is_terminate_msg() { return msg_.opcode() == simulation_grid::grid_db::instruction_msg::TERMINATE; }
    inline bool is_append_msg() { return msg_.opcode() == simulation_grid::grid_db::instruction_msg::APPEND; }
    inline const simulation_grid::grid_db::terminate_msg& get_terminate_msg() { return msg_.terminate_msg(); }
    inline const simulation_grid::grid_db::append_msg& get_append_msg() { return msg_.append_msg(); }
    void set_terminate_msg(const simulation_grid::grid_db::terminate_msg& msg);
    void set_append_msg(const simulation_grid::grid_db::append_msg& msg);
private:
    simulation_grid::grid_db::instruction_msg msg_;
};

class result
{
public:
    enum msg_status
    {
	WELLFORMED = 0,
	MALFORMED = 1
    };
    result();
    result(const result& other);
    result& operator=(const result& other);
    inline std::size_t get_size() const { return msg_.ByteSize(); }
    void serialize(simulation_grid::communication::message_sink& sink) const;
    msg_status deserialize(const simulation_grid::communication::message_source& source);
    inline bool is_malformed_message_msg() { return msg_.opcode() == simulation_grid::grid_db::result_msg::MALFORMED_MESSAGE; }
    inline bool is_invalid_argument_msg() { return msg_.opcode() == simulation_grid::grid_db::result_msg::INVALID_ARGUMENT; }
    inline bool is_confirmation_msg() { return msg_.opcode() == simulation_grid::grid_db::result_msg::CONFIRMATION; }
    inline bool is_index_msg() { return msg_.opcode() == simulation_grid::grid_db::result_msg::INDEX; }
    inline bool is_failed_op_msg() { return msg_.opcode() == simulation_grid::grid_db::result_msg::FAILED_OP; }
    inline const simulation_grid::grid_db::malformed_message_msg& get_malformed_message_msg() { return msg_.malformed_message_msg(); }
    inline const simulation_grid::grid_db::invalid_argument_msg& get_invalid_argument_msg() { return msg_.invalid_argument_msg(); }
    inline const simulation_grid::grid_db::confirmation_msg& get_confirmation_msg() { return msg_.confirmation_msg(); }
    inline const simulation_grid::grid_db::index_msg& get_index_msg() { return msg_.index_msg(); }
    inline const simulation_grid::grid_db::failed_op_msg& get_failed_op_msg() { return msg_.failed_op_msg(); }
    void set_malformed_message_msg(const simulation_grid::grid_db::malformed_message_msg& msg);
    void set_invalid_argument_msg(const simulation_grid::grid_db::invalid_argument_msg& msg);
    void set_confirmation_msg(const simulation_grid::grid_db::confirmation_msg& msg);
    void set_index_msg(const simulation_grid::grid_db::index_msg& msg);
    void set_failed_op_msg(const simulation_grid::grid_db::failed_op_msg& msg);
private:
    simulation_grid::grid_db::result_msg msg_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
