#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "log_service_msg.hpp"

namespace scm = simulation_grid::communication;
namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

struct_A::struct_A(const char* k, const char* v)
{
    if (UNLIKELY_EXT(strlen(k) > MAX_LENGTH) || UNLIKELY_EXT(strlen(v) > MAX_LENGTH))
    {
        throw grid_db_error("Maximum value length exceeded")
                << info_component_identity("struct_A")
                << info_data_identity(key);
    }
    strncpy(key, k, sizeof(key));
    strncpy(value, v, sizeof(value));
}

struct_A::struct_A(const struct_A& other)
{
    strncpy(key, other.key, sizeof(key));
    strncpy(value, other.value, sizeof(value));
}

struct_A::struct_A(const struct_A_msg& msg)
{
    strncpy(key, msg.key().c_str(), sizeof(key));
    strncpy(value, msg.value().c_str(), sizeof(value));
}

struct_A::~struct_A()
{ }

struct_A& struct_A::operator=(const struct_A& other)
{
    if (this != &other)
    {
        strncpy(key, other.key, sizeof(key));
        strncpy(value, other.value, sizeof(value));
    }
    return *this;
}

struct_A& struct_A::operator=(const struct_A_msg& msg)
{
    strncpy(key, msg.key().c_str(), sizeof(key));
    strncpy(value, msg.value().c_str(), sizeof(value));
    return *this;
}

bool struct_A::operator==(const struct_A& other) const
{
    return (this == &other) ||
	    (strncmp(key, other.key, sizeof(key)) == 0 &&
	    strncmp(value, other.value, sizeof(value)) == 0);
}

void struct_A::export_to(struct_A_msg& target) const
{
    target.set_key(key);
    target.set_value(value);
}

struct_B::struct_B(const char* k, bool a, boost::int32_t b, double c) :
	value1(a), value2(b), value3(c)
{
    strncpy(key, k, sizeof(key));
}

struct_B::struct_B(const struct_B& other) :
	value1(other.value1), value2(other.value2), value3(other.value3)
{
    strncpy(key, other.key, sizeof(key));
}

struct_B::struct_B(const struct_B_msg& msg) :
	value1(msg.value1()), value2(msg.value2()), value3(msg.value3())
{
    strncpy(key, msg.key().c_str(), sizeof(key));
}

struct_B::~struct_B()
{ }

struct_B& struct_B::operator=(const struct_B& other)
{
    if (this != &other)
    {
        strncpy(key, other.key, sizeof(key));
	value1 = other.value1;
	value2 = other.value2;
	value3 = other.value3;
    }
    return *this;
}

struct_B& struct_B::operator=(const struct_B_msg& msg)
{
    strncpy(key, msg.key().c_str(), sizeof(key));
    value1 = msg.value1();
    value2 = msg.value2();
    value3 = msg.value3();
    return *this;
}

bool struct_B::operator==(const struct_B& other) const
{
    return (this == &other) ||
	    (strncmp(key, other.key, sizeof(key)) == 0 &&
	    value1 == other.value1 &&
	    value2 == other.value2 &&
	    value3 == other.value3);
}

void struct_B::export_to(struct_B_msg& target) const
{
    target.set_key(key);
    target.set_value1(value1);
    target.set_value2(value2);
    target.set_value3(value3);
}

union_AB::union_AB(const struct_A& a) :
    value(a)
{ }

union_AB::union_AB(const struct_B& b) :
    value(b)
{ }

union_AB::union_AB(const union_AB& other) :
    value(other.value)
{ }

union_AB::union_AB(const union_AB_msg& msg)
{
    if (msg.tag() == union_AB_msg::STRUCT_A)
    {
	value = msg.a();
    }
    else
    {
	value = msg.b();
    }
}

union_AB::~union_AB()
{ }

union_AB& union_AB::operator=(const union_AB& other)
{
    if (this != &other)
    {
	value = other.value;
    }
    return *this;
}

union_AB& union_AB::operator=(const union_AB_msg& msg)
{
    if (msg.tag() == union_AB_msg::STRUCT_A)
    {
	value = msg.a();
    }
    else
    {
	value = msg.b();
    }
    return *this;
}

bool union_AB::operator==(const union_AB& other) const
{
    return (this == &other) || (value == other.value);
}

class union_AB_exporter : public boost::static_visitor<>
{
public:
    union_AB_exporter(union_AB_msg& msg);
    void operator()(const struct_A& a);
    void operator()(const struct_B& b);
private:
    union_AB_msg& msg_;
};

union_AB_exporter::union_AB_exporter(union_AB_msg& msg) :
    msg_(msg)
{ }

void union_AB_exporter::operator()(const struct_A& a)
{
    msg_.set_tag(union_AB_msg::STRUCT_A);
    a.export_to(*msg_.mutable_a());
}

void union_AB_exporter::operator()(const struct_B& b)
{
    msg_.set_tag(union_AB_msg::STRUCT_B);
    b.export_to(*msg_.mutable_b());
}

void union_AB::export_to(union_AB_msg& target) const
{
    union_AB_exporter exporter(target);
    boost::apply_visitor(exporter, value);
}

instruction::instruction() :
    msg_()
{ }

instruction::instruction(const instruction& other) :
    msg_(other.msg_)
{ }

instruction& instruction::operator=(const instruction& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
    }
    return *this;
}

void instruction::serialize(scm::message_sink& sink) const
{
    std::size_t required_size = msg_.ByteSize();
    msg_.SerializeToArray(sink.data(required_size), required_size);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction::msg_status instruction::deserialize(const scm::message_source& source)
{
    msg_status status = MALFORMED;
    if (msg_.ParseFromArray(source.data(), source.size()))
    {
	if ((is_terminate_msg() && msg_.has_terminate_msg()) ||
	    (is_append_msg() && msg_.has_append_msg()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void instruction::set_terminate_msg(const terminate_msg& msg)
{
    msg_.set_opcode(instruction_msg::TERMINATE);
    *msg_.mutable_terminate_msg() = msg;
}

void instruction::set_append_msg(const append_msg& msg)
{
    msg_.set_opcode(instruction_msg::APPEND);
    *msg_.mutable_append_msg() = msg;
}

result::result() :
     msg_()
{
    msg_.set_opcode(result_msg::CONFIRMATION);
}

result::result(const result& other) :
    msg_(other.msg_)
{ }

result& result::operator=(const result& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
    }
    return *this;
}

void result::serialize(scm::message_sink& sink) const
{
    std::size_t required_size = msg_.ByteSize();
    msg_.SerializeToArray(sink.data(required_size), required_size);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
result::msg_status result::deserialize(const scm::message_source& source)
{
    msg_status status = MALFORMED;
    if (msg_.ParseFromArray(source.data(), source.size()))
    {
	if ((is_malformed_message_msg() && msg_.has_malformed_message_msg()) ||
	    (is_invalid_argument_msg() && msg_.has_invalid_argument_msg()) ||
	    (is_confirmation_msg() && msg_.has_confirmation_msg()) ||
	    (is_index_msg() && msg_.has_index_msg()) ||
	    (is_failed_op_msg() && msg_.has_failed_op_msg()))
	{
	    status = WELLFORMED;
	}
    }
    return status;
}

void result::set_malformed_message_msg(const malformed_message_msg& msg)
{
    msg_.set_opcode(result_msg::MALFORMED_MESSAGE);
    *msg_.mutable_malformed_message_msg() = msg;
}

void result::set_invalid_argument_msg(const invalid_argument_msg& msg)
{
    msg_.set_opcode(result_msg::INVALID_ARGUMENT);
    *msg_.mutable_invalid_argument_msg() = msg;
}

void result::set_confirmation_msg(const confirmation_msg& msg)
{
    msg_.set_opcode(result_msg::CONFIRMATION);
    *msg_.mutable_confirmation_msg() = msg;
}

void result::set_index_msg(const index_msg& msg)
{
    msg_.set_opcode(result_msg::INDEX);
    *msg_.mutable_index_msg() = msg;
}

void result::set_failed_op_msg(const failed_op_msg& msg)
{
    msg_.set_opcode(result_msg::FAILED_OP);
    *msg_.mutable_failed_op_msg() = msg;
}

} // namespace grid_db
} // namespace simulation_grid
