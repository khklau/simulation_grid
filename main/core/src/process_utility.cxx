#include <stdexcept>
#include <boost/filesystem/operations.hpp>
#include "compiler_extensions.hpp"
#include "process_utility.hpp"

namespace bf = boost::filesystem;

namespace simulation_grid {
namespace core {
namespace process_utility {

#if defined __linux__
bf::path current_exe_path()
{
    bf::path proc_exe("/proc/self/exe");
    if (UNLIKELY_EXT(!bf::exists(proc_exe)))
    {
	throw std::runtime_error("Invalid Linux system: /proc/self/exe does not exist");
    }
    if (UNLIKELY_EXT(!bf::is_symlink(proc_exe)))
    {
	throw std::runtime_error("Invalid Linux system: /proc/self/exe is not a symbolic link");
    }
    return bf::read_symlink(proc_exe);
}
#endif

} // process_utility
} // namespace core
} // namespace simulation_grid
