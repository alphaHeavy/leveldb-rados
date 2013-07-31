#include <leveldb/c.h>

extern "C"
{
extern leveldb_env_t* leveldb_create_rados_env(const char* config_file, const char* pool_name);
}
