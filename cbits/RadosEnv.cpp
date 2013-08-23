#include <rados/librados.hpp>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/options.h>
#include <iostream>
#include <memory>
#include <boost/filesystem/path.hpp>
#include <boost/make_shared.hpp>

#include "RadosEnv.h"

using namespace std;
using boost::filesystem::path;

static leveldb::Status IOError(const std::string& context, int err_number) {
  return leveldb::Status::IOError(context, strerror(err_number));
}

class RadosSequentialFile : public leveldb::SequentialFile
{
public:
  RadosSequentialFile(const boost::shared_ptr<librados::IoCtx>& ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
    , off_(0)
  {
  }

private:
  virtual leveldb::Status Read(size_t n, leveldb::Slice* result, char* scratch)
  {
    librados::bufferlist bl;
    const int r = ctx_->read(fname_, bl, n, off_);
    if (r < 0)
    {
       return IOError("RadosSequentialFile::Read: " + fname_, -r);
    }

    bl.copy(0, r, scratch);
    off_ += r;

    *result = leveldb::Slice(scratch, r);

    return leveldb::Status::OK();
  }

  virtual leveldb::Status Skip(uint64_t n)
  {
    off_ += n;

    return leveldb::Status::OK();
  }

private:
  const boost::shared_ptr<librados::IoCtx> ctx_;
  const std::string fname_;
  uint64_t off_;
};

class RadosRandomAccessFile : public leveldb::RandomAccessFile
{
public:
  RadosRandomAccessFile(const boost::shared_ptr<librados::IoCtx>& ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
  {
  }

private:
  virtual leveldb::Status Read(uint64_t offset, size_t n, leveldb::Slice* result, char* scratch) const
  {
    librados::bufferlist bl;
    const int r = ctx_->read(fname_, bl, n, offset);
    if (r < 0)
    {
       return IOError("RadosRandomAccessFile::Read: " + fname_, -r);
    }

    bl.copy(0, r, scratch);

    *result = leveldb::Slice(scratch, r);

    return leveldb::Status::OK();
  }

private:
  const boost::shared_ptr<librados::IoCtx> ctx_;
  const std::string fname_;
};

class RadosWritableFile : public leveldb::WritableFile
{
public:
  RadosWritableFile(const boost::shared_ptr<librados::IoCtx>& ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
  {
  }

private:
  virtual leveldb::Status Append(const leveldb::Slice& data)
  {
    // copy the data into a Rados bufferlist then queue an append
    librados::bufferlist bl;
    bl.append(data.data(), data.size());
    std::auto_ptr<librados::AioCompletion> c(librados::Rados::aio_create_completion());
    int err = ctx_->aio_append(fname_, c.get(), bl, data.size());
    if (err < 0)
    {
      return IOError("RadosWriteableFile/Append: " + fname_, -err);
    }

    c.release();
    return leveldb::Status::OK();
  }

  virtual leveldb::Status Close()
  {
    // nop
    return leveldb::Status::OK();
  }

  static void FreeAioCompletion(librados::completion_t cb, void* arg)
  {
    static_cast<librados::AioCompletion*>(arg)->release();
  }

  virtual leveldb::Status Flush()
  {
    std::auto_ptr<librados::AioCompletion> c(librados::Rados::aio_create_completion());
    c->set_complete_callback(c.get(), FreeAioCompletion);
    int err = ctx_->aio_flush_async(c.get());
    if (err < 0)
    {
      return IOError("RadosWriteableFile/Flush: " + fname_, -err);
    }

    c.release();

    return leveldb::Status::OK();
  }

  virtual leveldb::Status Sync()
  {
    int err = ctx_->aio_flush();
    if (err < 0)
    {
      return IOError("RadosWriteableFile/Sync: " + fname_, -err);
    }

    return leveldb::Status::OK();
  }

private:
  const boost::shared_ptr<librados::IoCtx> ctx_;
  const std::string fname_;
};

class RadosEnv : public leveldb::EnvWrapper
{
public:
  RadosEnv(const boost::shared_ptr<void>& parent, const boost::shared_ptr<librados::IoCtx>& ctx)
    : leveldb::EnvWrapper(Env::Default())
    , parent_(parent)
    , ctx_(ctx)
  {
  }

private:
  virtual leveldb::Status NewSequentialFile(const std::string& fname, leveldb::SequentialFile** result)
  {
    *result = new RadosSequentialFile(ctx_, fname);
    return leveldb::Status::OK();
  }

  virtual leveldb::Status NewRandomAccessFile(const std::string& fname, leveldb::RandomAccessFile** result)
  {
    *result = new RadosRandomAccessFile(ctx_, fname);
    return leveldb::Status::OK();
  }

  virtual leveldb::Status NewWritableFile(const std::string& fname, leveldb::WritableFile** result)
  {
    int err = ctx_->create(fname, true);
    if (err < 0)
    {
      return IOError("NewWritableFile: " + fname, -err);
    }

    *result = new RadosWritableFile(ctx_, fname);
    return leveldb::Status::OK();
  }

  virtual bool FileExists(const std::string& fname)
  {
    size_t size = 0;
    time_t mtime = 0;
    if (ctx_->stat(fname, &size, &mtime) < 0)
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  virtual leveldb::Status GetChildren(const std::string& dir, std::vector<std::string>* result)
  {
    result->clear();
    for (librados::ObjectIterator it = ctx_->objects_begin(); it != ctx_->objects_end(); ++it)
    {
      const std::pair<std::string, std::string>& cur = *it;
      const path p(cur.first);
      result->push_back(p.filename().string());
    }

    return leveldb::Status::OK();
  }

  virtual leveldb::Status DeleteFile(const std::string& fname)
  {
    int err = ctx_->remove(fname);
    if (err < 0)
    {
      return IOError("DeleteFile: " + fname, -err);
    }

    return leveldb::Status::OK();
  }

  virtual leveldb::Status CreateDir(const std::string& dirname)
  {
    return leveldb::Status::OK();
  }

  virtual leveldb::Status DeleteDir(const std::string& dirname)
  {
    return leveldb::Status::OK();
  }

  virtual leveldb::Status GetFileSize(const std::string& fname, uint64_t* file_size)
  {
    size_t size = 0;
    time_t mtime = 0;
    int err = ctx_->stat(fname, &size, &mtime);
    if (err < 0)
    {
      return IOError("GetFileSize/stat: " + fname, -err);
    }

    *file_size = static_cast<uint64_t>(size);

    return leveldb::Status::OK();
  }

  virtual leveldb::Status RenameFile(const std::string& src, const std::string& target)
  {
    size_t size = 0;
    time_t mtime = 0;
    int err = ctx_->stat(src, &size, &mtime);
    if (err < 0)
    {
      return IOError("RenameFile/stat: " + src, -err);
    }

    librados::bufferlist bl;
    err = ctx_->read(src, bl, size, 0);
    if (err < 0)
    {
      return IOError("RenameFile/read: " + src, -err);
    }

    err = ctx_->write_full(target, bl);
    if (err < 0)
    {
      return IOError("RenameFile/write_full: " + target, -err);
    }

    err = ctx_->remove(src);
    if (err < 0)
    {
      return IOError("RenameFile/remove: " + src, -err);
    }

    return leveldb::Status::OK();
  }

  virtual leveldb::Status LockFile(const std::string& fname, leveldb::FileLock** lock)
  {
    // @TODO: use lock_exclusive/unlock when we upgrade to dumpling
    *lock = new leveldb::FileLock();

    return leveldb::Status::OK();
  }

  virtual leveldb::Status UnlockFile(leveldb::FileLock* lock)
  {
    // TODO: delete lock?

    return leveldb::Status::OK();
  }

  virtual leveldb::Status GetTestDirectory(std::string* path)
  {
    *path = "tmp/";

    return leveldb::Status::OK();
  }

private:
  const boost::shared_ptr<librados::IoCtx> ctx_;
  const boost::shared_ptr<void> parent_;
};

// sadly this is internal to LevelDB
struct leveldb_env_t
{
  leveldb::Env* rep;
  bool is_default;
};

leveldb_env_t* leveldb_create_rados_env(const char* config_file, const char* pool_name)
{
  int err;
  boost::shared_ptr<librados::Rados> rados(new librados::Rados());
  err = rados->init(NULL);
  if (err < 0)
  {
    cerr << "Rados::init() failed: " << strerror(-err);
    return NULL;
  }

  err = rados->conf_read_file(config_file);
  if (err < 0)
  {
    cerr << "Rados::conf_read_file() failed: " << strerror(-err);
    return NULL;
  }

  err = rados->connect();
  if (err < 0)
  {
    cerr << "Rados::connect() failed: " << strerror(-err);
    return NULL;
  }

  librados::IoCtx c;
  err = rados->ioctx_create("leveldb", c);
  if (err < 0)
  {
    cerr << "Rados::ioctx_create() failed: " << strerror(-err);
    return NULL;
  }

  boost::shared_ptr<librados::IoCtx> ioctx(boost::make_shared<librados::IoCtx>(c));

  RadosEnv env(rados, ioctx);

  leveldb_env_t* result = new leveldb_env_t;
  result->rep = new RadosEnv(rados, ioctx);
  result->is_default = false;
  return result;
}
