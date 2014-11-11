#ifndef SIMULATION_GRID_GRID_DB_LOG_MEMORY_HXX
#define SIMULATION_GRID_GRID_DB_LOG_MEMORY_HXX

#include "log_memory.hpp"
#include <boost/atomic.hpp>
#include <simulation_grid/core/compiler_extensions.hpp>
#include <simulation_grid/grid_db/exception.hpp>

namespace bip = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

extern const char* LOG_TYPE_TAG;

#ifdef LEVEL1_DCACHE_LINESIZE

struct log_header
{
    log_header(const version& ver, boost::uint64_t regsize, log_index maxidx);
    boost::uint16_t endianess_indicator;
    char memory_type_tag[48];
    version memory_version;
    boost::uint16_t header_size;
    boost::uint64_t region_size;
    log_index max_index;
    boost::atomic<log_index> tail_index;
} __attribute__((aligned(LEVEL1_DCACHE_LINESIZE)));

#endif

template <class entry_t>
struct log_container
{
    log_container(const version& ver, boost::uint64_t regsize, log_index maxidx);
    log_header header;
    entry_t log[];
};

template <class entry_t>
log_container<entry_t>::log_container(const version& ver, boost::uint64_t regsize, log_index maxidx) :
    header(ver, regsize, maxidx)
{ }

template <class entry_t>
void check(const bip::mapped_region& region)
{
    const log_container<entry_t>* container = static_cast<const log_container<entry_t>*>(region.get_address());
    if (UNLIKELY_EXT(!container))
    {
	throw malformed_db_error("Could not find log container")
		<< info_component_identity("log_memory");
    }
    if (UNLIKELY_EXT(container->header.endianess_indicator != std::numeric_limits<boost::uint8_t>::max()))
    {
        throw unsupported_db_error("Memory requires byte swapping")
                << info_component_identity("log_memory");
    }
    if (UNLIKELY_EXT(strncmp(
	    container->header.memory_type_tag,
	    LOG_TYPE_TAG,
	    sizeof(container->header.memory_type_tag))))
    {
        throw malformed_db_error("Incorrect memory type tag found")
                << info_component_identity("log_memory");
    }
    if (UNLIKELY_EXT(container->header.memory_version < LOG_MIN_SUPPORTED_VERSION ||
            container->header.memory_version > LOG_MAX_SUPPORTED_VERSION))
    {
        throw unsupported_db_error("Unsuported memory version")
                << info_component_identity("log_memory")
                << info_version_found(container->header.memory_version)
                << info_min_supported_version(LOG_MIN_SUPPORTED_VERSION)
                << info_max_supported_version(LOG_MAX_SUPPORTED_VERSION);
    }
    if (UNLIKELY_EXT(sizeof(log_header) != container->header.header_size))
    {
        throw malformed_db_error("Wrong header size")
                << info_component_identity("log_memory");
    }
    if (UNLIKELY_EXT(region.get_size() != container->header.region_size))
    {
        throw malformed_db_error("Wrong region size")
                << info_component_identity("log_memory");
    }
    std::size_t expected_max = ((region.get_size() - sizeof(log_container<entry_t>)) / sizeof(entry_t)) - 1;
    if (UNLIKELY_EXT(expected_max != container->header.max_index))
    {
        throw malformed_db_error("Log entry size mismatch")
                << info_component_identity("log_memory");
    }
    if (UNLIKELY_EXT(container->header.max_index < container->header.tail_index.load(boost::memory_order_relaxed)))
    {
        throw malformed_db_error("Tail index is greater than maximum allowed index")
                << info_component_identity("log_memory");
    }
}

template <class entry_t>
void init_log(bip::mapped_region& region)
{
    void* base = region.get_address();
    if (UNLIKELY_EXT(!base))
    {
	throw malformed_db_error("Could not find log container")
		<< info_component_identity("log_memory");
    }
    std::size_t region_size = region.get_size();
    new (base) log_container<entry_t>(
	    LOG_MAX_SUPPORTED_VERSION,
	    region_size,
	    ((region_size - sizeof(log_container<entry_t>)) / sizeof(entry_t)) - 1);
}

template <class entry_t>
log_reader_handle<entry_t>::log_reader_handle(const bip::mapped_region& region) :
    region_(region)
{
    check<entry_t>(region_);
}

template <class entry_t>
log_reader_handle<entry_t>::~log_reader_handle()
{ }

template <class entry_t>
log_index log_reader_handle<entry_t>::get_max_index() const
{
    const log_container<entry_t>* container = static_cast<const log_container<entry_t>*>(region_.get_address());
    assert(container);
    return container->header.max_index;
}

template <class entry_t>
log_owner_handle<entry_t>::log_owner_handle(open_mode mode, bip::mapped_region& region) :
    region_(region)
{
    if (mode == open_new)
    {
	init_log<entry_t>(region_);
    }
    else
    {
	check<entry_t>(region_);
    }
}

template <class entry_t>
log_owner_handle<entry_t>::~log_owner_handle()
{ }

} // namespace grid_db
} // namespace simulation_grid

#endif
