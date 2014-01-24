#include <string>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/errors.hpp>
#include "config.h"

namespace simulation_grid {
namespace manager {


config::config() :
	manager_port(2300), manager_address(""), bot_path("")
{ }


config::config(const config& other) :
	manager_port(other.manager_port),
	manager_address(other.manager_address),
	bot_path(other.bot_path)
{ }


config& config::operator=(const config& other)
{
    if (this != &other)
    {
	manager_port = other.manager_port;
	manager_address = other.manager_address;
	bot_path = other.bot_path;
    }
    return *this;
}


config::~config() { }


parse_result::value parse_cmd_line(const int argc, char* const argv[],
	config& conf, std::ostringstream& err_msg)
{
    config tmp;
    namespace bpo = boost::program_options;
    bpo::options_description descr("Usage: manager [options] bot-path");
    descr.add_options()
	    ("help,h", "This help text")
	    ("port,p", bpo::value<unsigned short>(&tmp.manager_port)->default_value(DEFAULT_PORT),
		    "Port number for grid topology query service")
    bpo::variables_map vm;
    try
    {
	bpo::store(bpo::command_line_parser(argc, argv).options(descr).run(), vm);
	bpo::notify(vm);
    }
    catch (bpo::error& ex)
    {
	err_msg << "Error: " << ex.what() << "\n";
        err_msg << descr;
	return parse_result::fail;
    }
    if (vm.count("help"))
    {
        err_msg << descr;
	return parse_result::fail;
    }
    conf = tmp;
    return parse_result::pass;
}


} // namespace simulation_grid
} // namespace manager
