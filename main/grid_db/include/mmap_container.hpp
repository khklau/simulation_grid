#ifndef SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP
#define SIMULATION_GRID_GRID_DB_MMAP_CONTAINER_HPP

#include <boost/noncopyable.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/interprocess/segment_manager.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>

namespace simulation_grid {
namespace grid_db {

// TODO: replace with type alias after moving to a C++11 compiler
template <class content_t>
struct mmap_allocator
{
    typedef boost::interprocess::allocator<content_t, 
	    boost::interprocess::managed_mapped_file::segment_manager> type;
};

// TODO: replace with type alias after moving to a C++11 compiler
template <class content_t>
struct mmap_vector 
{
    typedef typename mmap_allocator<content_t>::type allocator_t;
    typedef boost::interprocess::vector<content_t, allocator_t> type;
};

class mmap_container_reader
{
public:
    mmap_container_reader(const boost::filesystem::path& path);
    ~mmap_container_reader();
    const boost::filesystem::path& get_path() const;
    size_t get_size() const;
    template <class content_t> bool exists(const char* id);
    template <class content_t> const content_t& read(const char* id);
private:
    boost::filesystem::path path_;
    boost::interprocess::managed_mapped_file mmap_;
};

} // namespace grid_db
} // namespace simulation_grid

#endif
