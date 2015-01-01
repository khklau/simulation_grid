#ifndef SUPERNOVA_CORE_PROCESS_UTILITY
#define SUPERNOVA_CORE_PROCESS_UTILITY

#include <boost/filesystem/path.hpp>

namespace supernova {
namespace core {
namespace process_utility {

#if defined __linux__
boost::filesystem::path current_exe_path();
#endif

} // namespace process_utility
} // namespace core
} // namespace supernova

#endif
