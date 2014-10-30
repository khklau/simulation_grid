#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>
#include "log_service_msg.hpp"

namespace sgd = simulation_grid::grid_db;

namespace simulation_grid {
namespace grid_db {

struct_A::struct_A(const char* k, const char* v)
{
    if (UNLIKELY_EXT(strlen(key) > MAX_LENGTH) || UNLIKELY_EXT(strlen(value) > MAX_LENGTH))
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

void struct_B::export_to(struct_B_msg& target) const
{
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
    msg_(),
    buf_(static_cast<size_t>(msg_.SpaceUsed()))
{ }

instruction::instruction(const instruction& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

instruction& instruction::operator=(const instruction& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
}

void instruction::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
instruction::msg_status instruction::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
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
     msg_(),
     buf_(static_cast<size_t>(msg_.SpaceUsed()))
{
    msg_.set_opcode(result_msg::CONFIRMATION);
}

result::result(const result& other) :
    msg_(other.msg_),
    buf_(static_cast<size_t>(other.msg_.SpaceUsed()))
{ }

result& result::operator=(const result& other)
{
    if (&other != this)
    {
	msg_ = other.msg_;
	buf_.rebuild();
    }
    return *this;
}

void result::serialize(zmq::socket_t& socket)
{
    buf_.rebuild(static_cast<size_t>(msg_.ByteSize()));
    msg_.SerializeToArray(buf_.data(), buf_.size());
    socket.send(buf_);
}

/**
 * Unfortunately we need this because Protocol Buffers doesn't support unions, so we have to fake it which is not type safe
 */
result::msg_status result::deserialize(zmq::socket_t& socket)
{
    msg_status status = MALFORMED;
    socket.recv(&buf_);
    if (msg_.ParseFromArray(buf_.data(), buf_.size()))
    {
	if ((is_malformed_message_msg() && msg_.has_malformed_message_msg()) ||
	    (is_invalid_argument_msg() && msg_.has_invalid_argument_msg()) ||
	    (is_confirmation_msg() && msg_.has_confirmation_msg()) ||
	    (is_index_msg() && msg_.has_index_msg()))
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

} // namespace grid_db
} // namespace simulation_grid
