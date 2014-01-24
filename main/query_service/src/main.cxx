#include <iostream>
#include <sstream>
#include "config.hpp"

int main(int argc, char* argv[])
{
    namespace qs = simulation_grid::query_service;
    std::ostringstream err;
    qs::config conf;
    qs::parse_result::value result = qs::parse_cmd_line(argc, argv, conf, err);
    if (result == qs::parse_result::fail)
    {
	std::cerr << err.str() << std::endl;
	return 1;
    }
    return 0;
}
