#include "log_mmap.hxx"
#include <fstream>
#include <boost/filesystem/operations.hpp>

namespace bfs = boost::filesystem;

namespace simulation_grid {
namespace grid_db {

const bfs::path& init_file(const bfs::path& file, std::size_t size)
{
    if (!bfs::exists(file))
    {
	std::filebuf fbuf;
	fbuf.open(file.string().c_str(),
		std::ios_base::out |
		std::ios_base::trunc |
		std::ios_base::binary);
	fbuf.pubseekoff(size - 1, std::ios_base::beg);
	fbuf.sputc('\0');
    }
    return file;
}

} // namespace grid_db
} // namespace simulation_grid
