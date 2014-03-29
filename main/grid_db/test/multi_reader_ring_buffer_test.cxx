#include <memory>
#include <boost/cstdint.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <gtest/gtest.h>
#include "multi_reader_ring_buffer.hpp"
#include "multi_reader_ring_buffer.hxx"

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;
namespace sg = simulation_grid::grid_db;

TEST(multi_reader_ring_buffer_test, front_reference_lifetime)
{
    bf::path tmp_path = bf::unique_path();
    bi::managed_mapped_file file(bi::create_only, tmp_path.string().c_str(), 4096);
    sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type* buf = file.construct<sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type>("RB")(4, &file);
    const boost::uint32_t expected1 = 1U;
    buf->push_front(expected1);
    const boost::uint32_t& actual1 = buf->front();
    EXPECT_EQ(expected1, actual1) << "front element is not just pushed element";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    const boost::uint32_t expected2 = 2U;
    buf->push_front(expected2);
    const boost::uint32_t& actual2 = buf->front();
    EXPECT_EQ(expected2, actual2) << "front element is not just pushed element";
    EXPECT_EQ(2U, buf->element_count()) << "element count is wrong";
    ASSERT_EQ(expected1, actual1) << "reference to original front element is lost after subsequent push";
    bf::remove(tmp_path);
}

TEST(multi_reader_ring_buffer_test, already_popped_back_element)
{
    bf::path tmp_path = bf::unique_path();
    bi::managed_mapped_file file(bi::create_only, tmp_path.string().c_str(), 4096);
    sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type* buf = file.construct<sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type>("RB")(4, &file);
    const boost::uint32_t expected1 = 1U;
    buf->push_front(expected1);
    const boost::uint32_t& actual1 = buf->back();
    EXPECT_EQ(expected1, actual1) << "back element is not first element pushed";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    const boost::uint32_t expected2 = buf->back();
    buf->push_front(2U);
    const boost::uint32_t& actual2 = buf->back();
    EXPECT_EQ(expected2, actual2) << "back element is not the same after push";
    EXPECT_EQ(2U, buf->element_count()) << "element count is wrong";
    const boost::uint32_t& expected3 = buf->back();
    buf->pop_back(expected3);
    const boost::uint32_t& actual3 = buf->back();
    EXPECT_NE(expected3, actual3) << "back element was not popped";
    EXPECT_EQ(1U, buf->element_count()) << "element count is wrong";
    buf->pop_back(expected3);
    ASSERT_EQ(1U, buf->element_count()) << "attempting to pop an element that was already popped should have failed";
    bf::remove(tmp_path);
}

TEST(multi_reader_ring_buffer_test, overfill)
{
    bf::path tmp_path = bf::unique_path();
    bi::managed_mapped_file file(bi::create_only, tmp_path.string().c_str(), 4096);
    sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type* buf = file.construct<sg::mmap_multi_reader_ring_buffer<boost::uint32_t>::type>("RB")(4, &file);
    EXPECT_TRUE(buf->empty()) << "initial state of ring buffer is not empty";
    buf->push_front(1U);
    buf->push_front(2U);
    buf->push_front(3U);
    buf->push_front(4U);
    EXPECT_TRUE(buf->full()) << "ring buffer not filled to capacity";
    const boost::uint32_t& expected1 = buf->back();
    buf->push_front(5U);
    const boost::uint32_t& actual1 = buf->back();
    ASSERT_NE(expected1, actual1) << "back element was not popped";
    bf::remove(tmp_path);
}
