#include "log_shm.hxx"
#include <boost/interprocess/shared_memory_object.hpp>

namespace bip = boost::interprocess;

namespace supernova {
namespace storage {

bip::shared_memory_object& init_shared_memory(bip::shared_memory_object& shm, std::size_t size)
{
    shm.truncate(size);
    return shm;
}

} // namespace storage
} // namespace supernova
