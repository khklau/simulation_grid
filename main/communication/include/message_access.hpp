#ifndef SIMULATION_GRID_COMMUNICATION_MESSAGE_ACCESS_HPP
#define SIMULATION_GRID_COMMUNICATION_MESSAGE_ACCESS_HPP

#include <zmq.hpp>

namespace simulation_grid {
namespace communication {

class message_source
{
public:
    message_source(std::size_t initial_size);
    ~message_source();
    inline const void* data() const { return buf_.data(); };
    inline std::size_t size() const { return buf_.size(); };
protected:
    zmq::message_t buf_;
};

class message_sink
{
public:
    message_sink(std::size_t initial_size);
    ~message_sink();
    void* data(std::size_t required_size);
protected:
    zmq::message_t buf_;
};

} // namespace communication
} // namespace simulation_grid

#endif
