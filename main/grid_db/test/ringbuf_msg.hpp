#ifndef SUPERNOVA_STORAGE_RINGBUF_MSG_HPP
#define SUPERNOVA_STORAGE_RINGBUF_MSG_HPP

#include "ringbuf_msg.pb.h"
#include <google/protobuf/message.h>
#include <supernova/communication/message_access.hpp>

namespace supernova {
namespace storage {

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
    inline bool is_query_front() { return msg_.opcode() == supernova::storage::instruction::QUERY_FRONT; }
    inline bool is_query_back() { return msg_.opcode() == supernova::storage::instruction::QUERY_BACK; }
    inline bool is_query_capacity() { return msg_.opcode() == supernova::storage::instruction::QUERY_CAPACITY; }
    inline bool is_query_count() { return msg_.opcode() == supernova::storage::instruction::QUERY_COUNT; }
    inline bool is_query_empty() { return msg_.opcode() == supernova::storage::instruction::QUERY_EMPTY; }
    inline bool is_query_full() { return msg_.opcode() == supernova::storage::instruction::QUERY_FULL; }
    inline bool is_push_front() { return msg_.opcode() == supernova::storage::instruction::PUSH_FRONT; }
    inline bool is_pop_back() { return msg_.opcode() == supernova::storage::instruction::POP_BACK; }
    inline bool is_export_element() { return msg_.opcode() == supernova::storage::instruction::EXPORT_ELEMENT; }
    inline const supernova::storage::terminate_instr& get_terminate() { return msg_.terminate(); }
    inline const supernova::storage::query_front_instr& get_query_front() { return msg_.query_front(); }
    inline const supernova::storage::query_back_instr& get_query_back() { return msg_.query_back(); }
    inline const supernova::storage::query_capacity_instr& get_query_capacity() { return msg_.query_capacity(); }
    inline const supernova::storage::query_count_instr& get_query_count() { return msg_.query_count(); }
    inline const supernova::storage::query_empty_instr& get_query_empty() { return msg_.query_empty(); }
    inline const supernova::storage::query_full_instr& get_query_full() { return msg_.query_full(); }
    inline const supernova::storage::push_front_instr& get_push_front() { return msg_.push_front(); }
    inline const supernova::storage::pop_back_instr& get_pop_back() { return msg_.pop_back(); }
    inline const supernova::storage::export_element_instr& get_export_element() { return msg_.export_element(); }
    void set_terminate(const supernova::storage::terminate_instr& instr);
    void set_query_front(const supernova::storage::query_front_instr& instr);
    void set_query_back(const supernova::storage::query_back_instr& instr);
    void set_query_capacity(const supernova::storage::query_capacity_instr& instr);
    void set_query_count(const supernova::storage::query_count_instr& instr);
    void set_query_empty(const supernova::storage::query_empty_instr& instr);
    void set_query_full(const supernova::storage::query_full_instr& instr);
    void set_push_front(const supernova::storage::push_front_instr& instr);
    void set_pop_back(const supernova::storage::pop_back_instr& instr);
    void set_export_element(const supernova::storage::export_element_instr& instr);
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
    inline bool is_element() { return msg_.opcode() == supernova::storage::result::ELEMENT; }
    inline bool is_size() { return msg_.opcode() == supernova::storage::result::SIZE; }
    inline bool is_predicate() { return msg_.opcode() == supernova::storage::result::PREDICATE; }
    inline const supernova::storage::malformed_message_result& get_malformed_message() { return msg_.malformed_message(); }
    inline const supernova::storage::invalid_argument_result& get_invalid_argument() { return msg_.invalid_argument(); }
    inline const supernova::storage::confirmation_result& get_confirmation() { return msg_.confirmation(); }
    inline const supernova::storage::element_result& get_element() { return msg_.element(); }
    inline const supernova::storage::size_result& get_size() { return msg_.size(); }
    inline const supernova::storage::predicate_result& get_predicate() { return msg_.predicate(); }
    void set_malformed_message(const supernova::storage::malformed_message_result& result);
    void set_invalid_argument(const supernova::storage::invalid_argument_result& result);
    void set_confirmation(const supernova::storage::confirmation_result& result);
    void set_element(const supernova::storage::element_result& result);
    void set_size(const supernova::storage::size_result& result);
    void set_predicate(const supernova::storage::predicate_result& result);
private:
    supernova::storage::result msg_;
};

} // namespace storage
} // namespace supernova

#endif
