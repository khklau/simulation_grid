#ifndef SIMULATION_GRID_GRID_DB_PRIMITIVE_HPP
#define SIMULATION_GRID_GRID_DB_PRIMITIVE_HPP

#include <cstdint>

namespace simulation_grid {
namespace grid_db {

union partition_id
{
    uint64_t uint_value;
    char cstr_value[8] __attribute__ ((aligned (8)));
};

union container_id
{
    uint64_t uint_value;
    char cstr_value[8] __attribute__ ((aligned (8)));
};

struct storage_id
{
    partition_id partition;
    container_id container;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
