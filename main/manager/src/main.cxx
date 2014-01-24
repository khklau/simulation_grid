#include <iostream>
#include <sstream>
#include "config.hpp"

int main(int argc, char* argv[])
{
    namespace manager = simulation_grid::manager;
    std::ostringstream err;
    manager::config conf;
    manager::parse_result::value result = manager::parse_cmd_line(argc, argv, conf, err);
    if (result == manager::parse_result::fail)
    {
	std::cerr << err.str() << std::endl;
	return 1;
    }
    return 0;
}
