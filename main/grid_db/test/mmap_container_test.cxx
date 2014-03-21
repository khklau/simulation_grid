#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "mmap_container.hpp"
#include "mmap_container.hxx"

namespace bf = boost::filesystem;
namespace gd = simulation_grid::grid_db;

TEST(mmap_container_test, exists_scalar)
{
    bf::path tmp_path = bf::unique_path();
    std::ofstream tmp_file(tmp_path.string().c_str());
    tmp_file << std::endl;
    std::remove(tmp_path.string().c_str());
}
