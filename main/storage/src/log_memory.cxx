#include "log_memory.hpp"
#include "log_memory.hxx"
#include <cstring>
#include <limits>
#include <supernova/core/compiler_extensions.hpp>

namespace supernova {
namespace storage {

const char* LOG_TYPE_TAG = "supernova::storage::log_memory";

log_header::log_header(const version& ver, boost::uint64_t regsize, log_index maxidx) :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    memory_version(ver),
    header_size(sizeof(log_header)),
    region_size(regsize),
    max_index(maxidx)
{
    strncpy(memory_type_tag, LOG_TYPE_TAG, sizeof(memory_type_tag));
    back_index = get_null_index();
}

log_index log_header::get_null_index() const
{
    return max_index + 1;
}

} // namespace storage
} // namespace supernova
