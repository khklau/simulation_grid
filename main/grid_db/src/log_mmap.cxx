#include "log_mmap.hxx"
#include <fstream>

namespace bfs = boost::filesystem;

namespace simulation_grid {
namespace grid_db {

const bfs::path& init_file(const bfs::path& path, std::size_t size)
{
    if (!bfs::exists(path))
    {
	std::filebuf fbuf;
	fbuf.open(path.string().c_str(),
		std::ios_base::out |
		std::ios_base::trunc |
		std::ios_base::binary);
	fbuf.pubseekoff(size - 1, std::ios_base::beg);
	fbuf.sputc('\0');
    }
    return path;
}

} // namespace grid_db
} // namespace simulation_grid
