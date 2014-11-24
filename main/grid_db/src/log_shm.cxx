#include "log_shm.hxx"
#include <boost/interprocess/shared_memory_object.hpp>

namespace bip = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

bip::shared_memory_object& init_shared_memory(bip::shared_memory_object& shm, std::size_t size)
{
    shm.truncate(size);
    return shm;
}

} // namespace grid_db
} // namespace simulation_grid
