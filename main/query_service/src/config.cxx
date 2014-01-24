#include <string>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/errors.hpp>
#include "config.hpp"

namespace simulation_grid {
namespace query_service {


config::config() :
	port(2300)
{ }


config::config(const config& other) :
	port(other.port)
{ }


config& config::operator=(const config& other)
{
    if (this != &other)
    {
	port = other.port;
    }
    return *this;
}


config::~config() { }


parse_result::value parse_cmd_line(const int argc, char* const argv[],
	config& conf, std::ostringstream& err_msg)
{
    config tmp;
    namespace bpo = boost::program_options;
    bpo::options_description descr("Usage: query_service [options]");
    descr.add_options()
	    ("help,h", "This help text")
	    ("port,p", bpo::value<unsigned short>(&tmp.port)->default_value(DEFAULT_PORT),
		    "Port number for grid query service");
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


} // namespace query_service
} // namespace simulation_grid
