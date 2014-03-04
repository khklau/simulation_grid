#include <boost/interprocess/creation_tags.hpp>
#include "mmap_container.hpp"

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;

namespace simulation_grid {
namespace grid_db {

mmap_container_reader::mmap_container_reader(const bf::path& path) :
	path_(path), mmap_(bi::open_only, path.string().c_str())
{ }

mmap_container_reader::~mmap_container_reader()
{ }

const bf::path& mmap_container_reader::get_path() const
{
    return path_;
}

size_t mmap_container_reader::get_size() const
{
    return mmap_.get_size();
}

} // namespace grid_db
} // namespace simulation_grid
