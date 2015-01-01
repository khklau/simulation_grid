#include <istream>
#include <ostream>
#include <fstream>
#include <string>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <boost/cstdint.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/noncopyable.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/optional.hpp>
#include <boost/ref.hpp>
#include <boost/signals2.hpp>
#include <boost/thread/thread.hpp>
#include <google/protobuf/message.h>
#include <supernova/core/compiler_extensions.hpp>
#include <supernova/core/signal_notifier.hpp>
#include <supernova/communication/request_reply_service.hpp>
#include <zmq.hpp>
#include <signal.h>
#include "exception.hpp"
#include "log_shm.hpp"
#include "log_shm.hxx"
#include "log_service_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpo = boost::program_options;
namespace bsi = boost::signals2;
namespace bsy = boost::system;
namespace sco = supernova::core;
namespace scm = supernova::communication;
namespace sst = supernova::storage;

namespace {

typedef boost::uint16_t port_t;

static const port_t DEFAULT_PORT = 22220U;
static const size_t DEFAULT_SIZE = 1 << 24;

namespace ipc {

enum type
{
    shm = 0,
    mmap
};

inline std::ostream& operator<<(std::ostream& out, const ipc::type& source)
{
    switch (source)
    {
	case ipc::shm:
	{
	    out << "shm";
	    break;
	}
	case ipc::mmap:
	{
	    out << "mmap";
	    break;
	}
	default: { }
    }
    return out;
}

inline std::istream& operator>>(std::istream& in, ipc::type& target)
{
    if (!in.good())
    {
	return in;
    }
    std::string tmp;
    in >> tmp;
    boost::algorithm::to_lower(tmp);
    std::string shm("shm");
    std::string mmap("mmap");
    if (tmp == shm)
    {
	target = ipc::shm;
    }
    else if (tmp == mmap)
    {
	target = ipc::mmap;
    }
    else
    {
	in.setstate(std::ios::failbit);
    }
    return in;
}

} // namespace ipc

struct config
{
    config() : ipc(ipc::shm), port(DEFAULT_PORT), size(DEFAULT_SIZE) { }
    config(ipc::type ipc_, const std::string& name_, port_t port_ = DEFAULT_PORT,
	    size_t size_ = DEFAULT_SIZE) :
    	ipc(ipc_), name(name_), port(port_), size(size_)
    { }
    ipc::type ipc;
    std::string name;
    port_t port;
    size_t size;
};

typedef boost::optional<config> parse_result;

parse_result parse_cmd_line(const int argc, char* const argv[], std::ostringstream& err_msg)
{
    parse_result result;
    config tmp;
    bpo::options_description descr("Usage: log_shm_service [options] ipc name");
    descr.add_options()
            ("help,h", "This help text")
            ("port,p", bpo::value<port_t>(&tmp.port)->default_value(DEFAULT_PORT),
                    "Port number to listen on")
            ("size,s", bpo::value<size_t>(&tmp.size)->default_value(DEFAULT_SIZE),
                    "Size of ring buffer in bytes")
            ("ipc,i", bpo::value<ipc::type>(&tmp.ipc)->required(),
                    "IPC method: (shm|mmap)")
            ("name,n", bpo::value<std::string>(&tmp.name)->required(),
                    "Name of the shared memory/mapped file");
    bpo::positional_options_description pos;
    pos.add("ipc", 1);
    pos.add("name", 1);
    bpo::variables_map vm;
    try
    {
        bpo::store(bpo::command_line_parser(argc, argv).options(descr).positional(pos).run(), vm);
        bpo::notify(vm);
    }
    catch (bpo::error& ex)
    {
        err_msg << "ERROR: " << ex.what() << "\n";
        err_msg << descr;
        return result;
    }
    if (vm.count("help"))
    {
        err_msg << descr;
        return result;
    }
    result = tmp;
    return result;
}

class log_service
{
public:
    log_service(const config& config);
    ~log_service();
    void start();
    void stop();
    bool terminated() const { return service_.has_stopped(); }
private:
    void run();
    void receive_instruction(const scm::request_reply_service::source&, scm::request_reply_service::sink&);
    void exec_terminate(const sst::terminate_msg& input, sst::result& output);
    void exec_append(const sst::append_msg& input, sst::result& output);
    sst::instruction instr_;
    sst::result result_;
    sst::log_shm_owner<sst::union_AB> owner_;
    sco::signal_notifier notifier_;
    scm::request_reply_service service_;
};

log_service::log_service(const config& config) :
    instr_(),
    result_(),
    owner_(config.name, config.size),
    notifier_(),
    service_("127.0.0.1", config.port, sizeof(instr_), sizeof(result_))
{
    notifier_.add(SIGTERM, boost::bind(&log_service::stop, this));
    notifier_.add(SIGINT, boost::bind(&log_service::stop, this));
    notifier_.start();
}

log_service::~log_service()
{
    notifier_.stop();
    stop();
}

void log_service::start()
{
    if (service_.has_stopped())
    {
	service_.reset();
    }
    run();
}

void log_service::stop()
{
    service_.stop();
}

void log_service::run()
{
    if (!terminated())
    {
	scm::request_reply_service::receive_func func(
		boost::bind(&log_service::receive_instruction, this, _1, _2));
	service_.submit(func);
	service_.start();
    }
}

void log_service::receive_instruction(const scm::request_reply_service::source& source, scm::request_reply_service::sink& sink)
{
    sst::instruction::msg_status status = instr_.deserialize(source);
    if (UNLIKELY_EXT(status == sst::instruction::MALFORMED))
    {
	sst::malformed_message_msg tmp;
	result_.set_malformed_message_msg(tmp);
    }
    else if (instr_.is_terminate_msg())
    {
	exec_terminate(instr_.get_terminate_msg(), result_);
    }
    else if (instr_.is_append_msg())
    {
	exec_append(instr_.get_append_msg(), result_);
    }
    else
    {
	sst::malformed_message_msg tmp;
	result_.set_malformed_message_msg(tmp);
    }
    result_.serialize(sink);
    if (!terminated())
    {
	scm::request_reply_service::receive_func func(
		boost::bind(&log_service::receive_instruction, this, _1, _2));
	service_.submit(func);
    }
}

void log_service::exec_terminate(const sst::terminate_msg& input, sst::result& output)
{
    stop();
    sst::confirmation_msg tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation_msg(tmp);
}

void log_service::exec_append(const sst::append_msg& input, sst::result& output)
{
    sst::union_AB entry(input.entry());
    boost::optional<sst::log_index> result = owner_.append(entry);
    if (result)
    {
	sst::index_msg success;
	success.set_sequence(input.sequence());
	success.set_index(result.get());
	output.set_index_msg(success);
    }
    else
    {
	sst::failed_op_msg failure;
	failure.set_sequence(input.sequence());
	output.set_failed_op_msg(failure);
    }
}

} // anonymous namespace

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::ostringstream err;
    parse_result config = parse_cmd_line(argc, argv, err);
    if (!config)
    {
	std::cerr << err.str() << std::endl;
	return 1;
    }
    switch (config.get().ipc)
    {
	case ipc::shm:
	{
	    {
		log_service service(config.get());
		service.start();
	    }
	    bip::shared_memory_object::remove(config.get().name.c_str());
	    break;
	}
	case ipc::mmap:
	{
	    {
		log_service service(config.get());
		service.start();
	    }
	    bfs::remove(config.get().name);
	    break;
	}
    }
    return 0;
}
