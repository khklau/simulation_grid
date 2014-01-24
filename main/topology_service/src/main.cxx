#include <iostream>
#include <sstream>
#include "config.hpp"

int main(int argc, char* argv[])
{
    namespace ts = simulation_grid::topology_service;
    std::ostringstream err;
    ts::config conf;
    ts::parse_result::value result = ts::parse_cmd_line(argc, argv, conf, err);
    if (result == ts::parse_result::fail)
    {
	std::cerr << err.str() << std::endl;
	return 1;
    }
    return 0;
}
