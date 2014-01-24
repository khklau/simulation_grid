#ifndef SIMULATION_GRID_NODE_CONFIG_H
#define SIMULATION_GRID_NODE_CONFIG_H

#include <string>
#include <iosfwd>

namespace simulation_grid {
namespace manager {

static const unsigned short DEFAULT_PORT = 2300;

struct config
{
    config();
    config(const config& other);
    config& operator=(const config& other);
    ~config();
    unsigned short port;
};

namespace parse_result
{
    enum value
    {
	pass = 0,
	fail
    };
}

parse_result::value parse_cmd_line(const int argc, char* const argv[],
	config& conf, std::ostringstream& err_msg);

} // namespace manager
} // namespace simulation_grid

#endif
