#include "log_memory.hpp"
#include "log_memory.hxx"
#include <cstring>
#include <limits>
#include <simulation_grid/core/compiler_extensions.hpp>

namespace simulation_grid {
namespace grid_db {

const char* LOG_TYPE_TAG = "simulation_grid::grid_db::log_memory";

log_header::log_header(const version& ver, boost::uint64_t regsize, log_index maxidx) :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    memory_version(ver),
    header_size(sizeof(log_header)),
    region_size(regsize),
    tail_index(0),
    max_index(maxidx)
{
    strncpy(memory_type_tag, LOG_TYPE_TAG, sizeof(memory_type_tag));
}

} // namespace grid_db
} // namespace simulation_grid
