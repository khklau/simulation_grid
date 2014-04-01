#ifndef SIMULATION_GRID_CORE_PROCESS_UTILITY
#define SIMULATION_GRID_CORE_PROCESS_UTILITY

#include <boost/filesystem/path.hpp>

namespace simulation_grid {
namespace core {
namespace process_utility {

#if defined __linux__
boost::filesystem::path current_exe_path();
#endif

} // process_utility
} // namespace core
} // namespace simulation_grid

#endif
