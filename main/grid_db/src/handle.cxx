#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <simulation_grid/grid_db/entry.pb.h>
#include <simulation_grid/grid_db/exception.hpp>
#include "handle.hpp"

using boost::interprocess::shared_memory_object;
using boost::interprocess::mapped_region;
using boost::interprocess::open_only;
using boost::interprocess::read_only;
using simulation_grid::grid_db::entry;

namespace simulation_grid {
namespace grid_db {

class read_handle_impl
{
public:
    read_handle_impl(const std::string& db_id) :
	    db_id_(db_id),
	    shmem_(open_only, db_id.c_str(), read_only), region_()
    {
	region_ = mapped_region(shmem_, read_only);
    }

    ~read_handle_impl() { }

    const entry& get_db_entry() const
    {
	const entry* address = static_cast<const entry*>(region_.get_address());
	if (!address)
	{
	    throw handle_error("Could not get address of entry") << info_db_id(db_id_);
	}
	return *address;
    }

private:
    std::string db_id_;
    shared_memory_object shmem_;
    mapped_region region_;
};

read_handle::read_handle(const std::string& db_id) :
	impl_(0)
{
    impl_ = new read_handle_impl(db_id);
}

read_handle::~read_handle()
{
    delete impl_;
}

const entry& read_handle::get_db_entry() const
{
    return impl_->get_db_entry();
}

} // namespace grid_db
} // namespace simulation_grid
