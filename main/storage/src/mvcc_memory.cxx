#include "mvcc_memory.hpp"
#include <cstring>
#include <exception>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/function.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/optional.hpp>
#include <boost/thread/thread.hpp>
#include <supernova/core/compiler_extensions.hpp>
#include <supernova/storage/exception.hpp>
#include "mvcc_memory.hxx"

namespace bfs = boost::filesystem;
namespace bip = boost::interprocess;
namespace bpt = boost::posix_time;

namespace supernova {
namespace storage {

mvcc_key::mvcc_key()
{
    c_str[0] = '\0';
}

mvcc_key::mvcc_key(const char* key)
{
    if (UNLIKELY_EXT(strlen(key) > MVCC_MAX_KEY_LENGTH))
    {
	throw storage_error("Maximum key length exceeded")
		<< info_component_identity("mvcc_key")
		<< info_data_identity(key);
    }
    strncpy(c_str, key, sizeof(c_str));
}

mvcc_key::mvcc_key(const mvcc_key& other)
{
    strncpy(c_str, other.c_str, sizeof(c_str));
}

mvcc_key::~mvcc_key()
{ }

mvcc_key& mvcc_key::operator=(const mvcc_key& other)
{
    if (this != &other)
    {
	strncpy(c_str, other.c_str, sizeof(c_str));
    }
    return *this;
}

bool mvcc_key::operator<(const mvcc_key& other) const
{
    return strncmp(c_str, other.c_str, sizeof(c_str)) < 0;
}

mvcc_header::mvcc_header() :
    endianess_indicator(std::numeric_limits<boost::uint8_t>::max()),
    memory_version(MVCC_MAX_SUPPORTED_VERSION), 
    header_size(sizeof(mvcc_header))
{
    strncpy(file_type_tag, MVCC_FILE_TYPE_TAG, sizeof(file_type_tag));
}

} // namespace storage
} // namespace supernova
