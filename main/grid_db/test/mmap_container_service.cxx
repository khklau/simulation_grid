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
#include <simulation_grid/core/compiler_extensions.hpp>
#include <zmq.hpp>
#include <signal.h>
#include "exception.hpp"
#include "mmap_container.hpp"
#include "mmap_container.hxx"
#include "container_msg.hpp"

namespace bas = boost::asio;
namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpo = boost::program_options;
namespace bsi = boost::signals2;
namespace bsy = boost::system;
namespace sgd = simulation_grid::grid_db;

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
    bpo::options_description descr("Usage: mmap_container_service [options] ipc name");
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

class signal_notifier : private boost::noncopyable
{
public:
    typedef bsi::signal<void ()> channel;
    typedef boost::function<void ()> receiver;
    signal_notifier();
    ~signal_notifier();
    bool running() const { return thread_ != 0; }
    void add(int signal, const receiver& slot);
    void reset();
    void start();
    void stop();
private:
    static const size_t MAX_SIGNAL = 64U; // TODO : work out how to base this on SIGRTMAX
    void run();
    void handle(const bsy::error_code& error, int signal);
    boost::thread* thread_;
    bas::io_service service_;
    bas::signal_set sigset_;
    channel chanset_[MAX_SIGNAL];
};

signal_notifier::signal_notifier() :
    thread_(0), service_(), sigset_(service_)
{ }

signal_notifier::~signal_notifier()
{
    try
    {
	for (std::size_t iter = 0; iter < MAX_SIGNAL; ++iter)
	{
		chanset_[iter].disconnect_all_slots();
	}
	if (!service_.stopped())
	{
	    service_.stop();
	    if (running())
	    {
		thread_->join();
		delete thread_;
	    }
	}
    }
    catch (...)
    {
	// do nothing
    }
}

void signal_notifier::add(int signal, const receiver& slot)
{
    std::size_t usignal = static_cast<std::size_t>(signal);
    if (!running() && usignal < MAX_SIGNAL)
    {
	chanset_[usignal].connect(slot);
	sigset_.add(signal);
    }
}

void signal_notifier::reset()
{
    boost::function2<void, const bsy::error_code&, int> handler(
	    boost::bind(&signal_notifier::handle, this, 
	    bas::placeholders::error, bas::placeholders::signal_number));
    sigset_.async_wait(handler);
}

void signal_notifier::start()
{
    if (!running())
    {
	boost::function<void ()> entry(boost::bind(&signal_notifier::run, this));
	thread_ = new boost::thread(entry);
    }
}

void signal_notifier::stop()
{
    service_.stop();
}

void signal_notifier::run()
{
    reset();
    service_.run();
}

void signal_notifier::handle(const bsy::error_code& error, int signal)
{
    std::size_t usignal = static_cast<std::size_t>(signal);
    if (!error && usignal < MAX_SIGNAL)
    {
	chanset_[usignal]();
    }
    reset();
}

class container_service
{
public:
    container_service(const config& config);
    ~container_service();
    void start();
    void stop();
    bool terminated() const { return service_.stopped(); }
private:
    static int init_zmq_socket(zmq::socket_t& socket, const config& config);
    void run();
    void receive_instruction(const bsy::error_code& error, size_t);
    void exec_terminate(const sgd::terminate_instr& input, sgd::result_msg& output);
    void exec_exists_string(const sgd::exists_string_instr& input, sgd::result_msg& output);
    void exec_exists_struct(const sgd::exists_struct_instr& input, sgd::result_msg& output);
    void exec_read_string(const sgd::read_string_instr& input, sgd::result_msg& output);
    void exec_read_struct(const sgd::read_struct_instr& input, sgd::result_msg& output);
    void exec_write_string(const sgd::write_string_instr& input, sgd::result_msg& output);
    void exec_write_struct(const sgd::write_struct_instr& input, sgd::result_msg& output);
    void exec_process_read_metadata(const sgd::process_read_metadata_instr& input, sgd::result_msg& output);
    void exec_process_write_metadata(const sgd::process_write_metadata_instr& input, sgd::result_msg& output);
    void exec_collect_garbage_1(const sgd::collect_garbage_1_instr& input, sgd::result_msg& output);
    void exec_collect_garbage_2(const sgd::collect_garbage_2_instr& input, sgd::result_msg& output);
    void exec_get_reader_token_id(const sgd::get_reader_token_id_instr& input, sgd::result_msg& output);
    void exec_get_last_read_revision(const sgd::get_last_read_revision_instr& input, sgd::result_msg& output);
    void exec_get_oldest_string_revision(const sgd::get_oldest_string_revision_instr& input, sgd::result_msg& output);
    void exec_get_oldest_struct_revision(const sgd::get_oldest_struct_revision_instr& input, sgd::result_msg& output);
    void exec_get_global_oldest_revision_read(const sgd::get_global_oldest_revision_read_instr& input, sgd::result_msg& output);
    void exec_get_registered_keys(const sgd::get_registered_keys_instr& input, sgd::result_msg& output);
    void exec_get_string_history_depth(const sgd::get_string_history_depth_instr& input, sgd::result_msg& output);
    void exec_get_struct_history_depth(const sgd::get_struct_history_depth_instr& input, sgd::result_msg& output);
    sgd::mvcc_mmap_owner owner_;
    bas::io_service service_;
    signal_notifier notifier_;
    sgd::instruction_msg instr_;
    sgd::result_msg result_;
    zmq::context_t context_;
    zmq::socket_t socket_;
    bas::posix::stream_descriptor stream_;
};

container_service::container_service(const config& config) :
    owner_(bfs::path(config.name.c_str()), config.size),
    service_(),
    notifier_(),
    instr_(),
    result_(),
    context_(1),
    socket_(context_, ZMQ_REP),
    stream_(service_, init_zmq_socket(socket_, config))
{
    notifier_.add(SIGTERM, boost::bind(&container_service::stop, this));
    notifier_.add(SIGINT, boost::bind(&container_service::stop, this));
    notifier_.start();
}

container_service::~container_service()
{
    stream_.release();
    socket_.close();
    context_.close();
    notifier_.stop();
    stop();
}

void container_service::start()
{
    if (service_.stopped())
    {
	service_.reset();
    }
    run();
}

void container_service::stop()
{
    service_.stop();
}

int container_service::init_zmq_socket(zmq::socket_t& socket, const config& config)
{
    std::string address(str(boost::format("tcp://127.0.0.1:%d") % config.port));
    socket.bind(address.c_str());

    // TODO this is currently POSIX specific, add a Windows version
    int fd = 0;
    size_t size = sizeof(fd);
    socket.getsockopt(ZMQ_FD, &fd, &size);
    if (UNLIKELY_EXT(size != sizeof(fd)))
    {
	throw std::runtime_error("Can't find ZeroMQ socket file descriptor");
    }
    return fd;
}

void container_service::run()
{
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&container_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run();
    }
}

void container_service::receive_instruction(const bsy::error_code& error, size_t)
{
    int event = 0;
    size_t size = sizeof(event);
    socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    // More than 1 message may be available, so we need to consume all of them
    while (LIKELY_EXT(!error && size == sizeof(event)) && (event & ZMQ_POLLIN))
    {
	sgd::instruction_msg::msg_status status = instr_.deserialize(socket_);
	if (UNLIKELY_EXT(status == sgd::instruction_msg::MALFORMED))
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	else if (instr_.is_terminate())
	{
	    exec_terminate(instr_.get_terminate(), result_);
	}
	else if (instr_.is_exists_string())
	{
	    exec_exists_string(instr_.get_exists_string(), result_);
	}
	else if (instr_.is_exists_struct())
	{
	    exec_exists_struct(instr_.get_exists_struct(), result_);
	}
	else if (instr_.is_read_string())
	{
	    exec_read_string(instr_.get_read_string(), result_);
	}
	else if (instr_.is_read_struct())
	{
	    exec_read_struct(instr_.get_read_struct(), result_);
	}
	else if (instr_.is_write_string())
	{
	    exec_write_string(instr_.get_write_string(), result_);
	}
	else if (instr_.is_write_struct())
	{
	    exec_write_struct(instr_.get_write_struct(), result_);
	}
	else if (instr_.is_process_read_metadata())
	{
	    exec_process_read_metadata(instr_.get_process_read_metadata(), result_);
	}
	else if (instr_.is_process_write_metadata())
	{
	    exec_process_write_metadata(instr_.get_process_write_metadata(), result_);
	}
	else if (instr_.is_collect_garbage_1())
	{
	    exec_collect_garbage_1(instr_.get_collect_garbage_1(), result_);
	}
	else if (instr_.is_collect_garbage_2())
	{
	    exec_collect_garbage_2(instr_.get_collect_garbage_2(), result_);
	}
	else if (instr_.is_get_reader_token_id())
	{
	    exec_get_reader_token_id(instr_.get_get_reader_token_id(), result_);
	}
	else if (instr_.is_get_last_read_revision())
	{
	    exec_get_last_read_revision(instr_.get_get_last_read_revision(), result_);
	}
	else if (instr_.is_get_oldest_string_revision())
	{
	    exec_get_oldest_string_revision(instr_.get_get_oldest_string_revision(), result_);
	}
	else if (instr_.is_get_oldest_struct_revision())
	{
	    exec_get_oldest_struct_revision(instr_.get_get_oldest_struct_revision(), result_);
	}
	else if (instr_.is_get_global_oldest_revision_read())
	{
	    exec_get_global_oldest_revision_read(instr_.get_get_global_oldest_revision_read(), result_);
	}
	else if (instr_.is_get_registered_keys())
	{
	    exec_get_registered_keys(instr_.get_get_registered_keys(), result_);
	}
	else if (instr_.is_get_string_history_depth())
	{
	    exec_get_string_history_depth(instr_.get_get_string_history_depth(), result_);
	}
	else if (instr_.is_get_struct_history_depth())
	{
	    exec_get_struct_history_depth(instr_.get_get_struct_history_depth(), result_);
	}
	else
	{
	    sgd::malformed_message_result tmp;
	    result_.set_malformed_message(tmp);
	}
	result_.serialize(socket_);
	socket_.getsockopt(ZMQ_EVENTS, &event, &size);
    }
    if (!terminated())
    {
	boost::function2<void, const bsy::error_code&, size_t> func(
		boost::bind(&container_service::receive_instruction, this, _1, _2));
	stream_.async_read_some(boost::asio::null_buffers(), func);
	service_.run_one();
    }
}

void container_service::exec_terminate(const sgd::terminate_instr& input, sgd::result_msg& output)
{
    stop();
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    output.set_confirmation(tmp);
}

void container_service::exec_exists_string(const sgd::exists_string_instr& input, sgd::result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    bool result(owner_.exists<string_value>(input.key().c_str()));
    tmp.set_predicate(result);
    output.set_predicate(tmp);
}

void container_service::exec_exists_struct(const sgd::exists_struct_instr& input, sgd::result_msg& output)
{
    sgd::predicate_result tmp;
    tmp.set_sequence(input.sequence());
    bool result(owner_.exists<struct_value>(input.key().c_str()));
    tmp.set_predicate(result);
    output.set_predicate(tmp);
}

void container_service::exec_read_string(const sgd::read_string_instr& input, sgd::result_msg& output)
{
    sgd::string_value_result tmp;
    tmp.set_sequence(input.sequence());
    std::string result(owner_.read<string_value>(input.key().c_str()).c_str);
    tmp.set_value(result);
    output.set_string_value(tmp);
}

void container_service::exec_read_struct(const sgd::read_struct_instr& input, sgd::result_msg& output)
{
    sgd::struct_value_result tmp;
    tmp.set_sequence(input.sequence());
    struct_value result(owner_.read<struct_value>(input.key().c_str()));
    tmp.set_value1(result.value1);
    tmp.set_value2(result.value2);
    tmp.set_value3(result.value3);
    output.set_struct_value(tmp);
}

void container_service::exec_write_string(const sgd::write_string_instr& input, sgd::result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    string_value value(input.value().c_str());
    owner_.write<string_value>(input.key().c_str(), value);
    output.set_confirmation(tmp);
}

void container_service::exec_write_struct(const sgd::write_struct_instr& input, sgd::result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    struct_value value(input.value1(), input.value2(), input.value3());
    owner_.write<struct_value>(input.key().c_str(), value);
    output.set_confirmation(tmp);
}

void container_service::exec_process_read_metadata(const sgd::process_read_metadata_instr& input, sgd::result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    owner_.process_read_metadata(input.from(), input.to());
    output.set_confirmation(tmp);
}

void container_service::exec_process_write_metadata(const sgd::process_write_metadata_instr& input, sgd::result_msg& output)
{
    sgd::confirmation_result tmp;
    tmp.set_sequence(input.sequence());
    owner_.process_write_metadata(input.max_attempts());
    output.set_confirmation(tmp);
}

void container_service::exec_collect_garbage_1(const sgd::collect_garbage_1_instr& input, sgd::result_msg& output)
{
    sgd::key_result tmp;
    tmp.set_sequence(input.sequence());
    std::string result(owner_.collect_garbage(input.max_attempts()));
    tmp.set_key(result);
    output.set_key(tmp);
}

void container_service::exec_collect_garbage_2(const sgd::collect_garbage_2_instr& input, sgd::result_msg& output)
{
    sgd::key_result tmp;
    tmp.set_sequence(input.sequence());
    std::string result(owner_.collect_garbage(input.from(), input.max_attempts()));
    tmp.set_key(result);
    output.set_key(tmp);
}

void container_service::exec_get_reader_token_id(const sgd::get_reader_token_id_instr& input, sgd::result_msg& output)
{
    sgd::token_id_result tmp;
    tmp.set_sequence(input.sequence());
    reader_token_id result(owner_.get_reader_token_id());
    tmp.set_token_id(result);
    output.set_token_id(tmp);
}

void container_service::exec_get_last_read_revision(const sgd::get_last_read_revision_instr& input, sgd::result_msg& output)
{
    sgd::revision_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_last_read_revision();
    tmp.set_revision(result);
    output.set_revision(tmp);
}

void container_service::exec_get_oldest_string_revision(const sgd::get_oldest_string_revision_instr& input, sgd::result_msg& output)
{
    sgd::revision_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_oldest_revision<string_value>(input.key().c_str());
    tmp.set_revision(result);
    output.set_revision(tmp);
}

void container_service::exec_get_oldest_struct_revision(const sgd::get_oldest_struct_revision_instr& input, sgd::result_msg& output)
{
    sgd::revision_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_oldest_revision<struct_value>(input.key().c_str());
    tmp.set_revision(result);
    output.set_revision(tmp);
}

void container_service::exec_get_global_oldest_revision_read(const sgd::get_global_oldest_revision_read_instr& input, sgd::result_msg& output)
{
    sgd::revision_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_global_oldest_revision_read();
    tmp.set_revision(result);
    output.set_revision(tmp);
}

void container_service::exec_get_registered_keys(const sgd::get_registered_keys_instr& input, sgd::result_msg& output)
{
    sgd::key_list_result tmp;
    tmp.set_sequence(input.sequence());
    std::vector<std::string> result(owner_.get_registered_keys());
    for (std::vector<std::string>::const_iterator iter = result.begin(); iter != result.end(); ++iter)
    {
	tmp.add_key_list(*iter);
    }
    output.set_key_list(tmp);
}

void container_service::exec_get_string_history_depth(const sgd::get_string_history_depth_instr& input, sgd::result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_history_depth<string_value>(input.key().c_str());
    tmp.set_size(result);
    output.set_size(tmp);
}

void container_service::exec_get_struct_history_depth(const sgd::get_struct_history_depth_instr& input, sgd::result_msg& output)
{
    sgd::size_result tmp;
    tmp.set_sequence(input.sequence());
    boost::uint64_t result = owner_.get_history_depth<struct_value>(input.key().c_str());
    tmp.set_size(result);
    output.set_size(tmp);
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
		container_service service(config.get());
		service.start();
	    }
	    bip::shared_memory_object::remove(config.get().name.c_str());
	    break;
	}
	case ipc::mmap:
	{
	    {
		container_service service(config.get());
		service.start();
	    }
	    bfs::remove(config.get().name);
	    break;
	}
    }
    return 0;
}
