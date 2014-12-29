#include "message_access.hpp"

namespace simulation_grid {
namespace communication {

message_source::message_source(std::size_t initial_size) :
	buf_(initial_size)
{ }

message_source::~message_source()
{ }

message_sink::message_sink(std::size_t initial_size) :
	buf_(initial_size)
{ }

message_sink::~message_sink()
{ }

void* message_sink::data(std::size_t required_size)
{
    buf_.rebuild(required_size);
    return buf_.data();
}

} // namespace communication
} // namespace simulation_grid
