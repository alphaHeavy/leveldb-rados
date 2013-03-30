#include <rados/librados.hpp>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/options.h>
#include <iostream>
#include <boost/filesystem/path.hpp>

using namespace std;
using boost::filesystem::path;

static leveldb::Status IOError(const std::string& context, int err_number) {
  return leveldb::Status::IOError(context, strerror(err_number));
}

class RadosSequentialFile : public leveldb::SequentialFile
{
public:
  RadosSequentialFile(librados::IoCtx ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
    , off_(0)
  {
  }

private:
  virtual leveldb::Status Read(size_t n, leveldb::Slice* result, char* scratch)
  {
    librados::bufferlist bl;
    const int r = ctx_.read(fname_, bl, n, off_);
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
  librados::IoCtx ctx_;
  const std::string fname_;
  uint64_t off_;
};

class RadosRandomAccessFile : public leveldb::RandomAccessFile
{
public:
  RadosRandomAccessFile(librados::IoCtx ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
  {
  }

private:
  virtual leveldb::Status Read(uint64_t offset, size_t n, leveldb::Slice* result, char* scratch) const
  {
    librados::bufferlist bl;
    const int r = ctx_.read(fname_, bl, n, offset);
    if (r < 0)
    {
       return IOError("RadosRandomAccessFile::Read: " + fname_, -r);
    }

    bl.copy(0, r, scratch);

    *result = leveldb::Slice(scratch, r);

    return leveldb::Status::OK();
  }

private:
  mutable librados::IoCtx ctx_;
  const std::string fname_;
};

class RadosWritableFile : public leveldb::WritableFile
{
public:
  RadosWritableFile(librados::IoCtx ctx, const std::string& fname)
    : ctx_(ctx)
    , fname_(fname)
  {
  }

private:
  virtual leveldb::Status Append(const leveldb::Slice& data)
  {
    librados::bufferlist bl;
    bl.push_back(ceph::buffer::ptr(data.data(), data.size()));
    int err = ctx_.append(fname_, bl, data.size());
    if (err < 0)
    {
      return IOError(string("RadosWriteableFile/Append: ") + fname_, -err);
    }

    return leveldb::Status::OK();
  }

  virtual leveldb::Status Close()
  {
    // nop
    return leveldb::Status::OK();
  }

  virtual leveldb::Status Flush()
  {
    // nop
    return leveldb::Status::OK();
  }

  virtual leveldb::Status Sync()
  {
    // nop
    return leveldb::Status::OK();
  }

private:
  librados::IoCtx ctx_;
  const std::string fname_;
};

class RadosEnv : public leveldb::EnvWrapper
{
public:
  RadosEnv(const librados::IoCtx& ctx)
    : leveldb::EnvWrapper(Env::Default())
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
    int err = ctx_.create(fname, true);
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
    if (ctx_.stat(fname, &size, &mtime) < 0)
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
    for (librados::ObjectIterator it = ctx_.objects_begin(); it != ctx_.objects_end(); ++it)
    {
      const std::pair<std::string, std::string>& cur = *it;
      const path p(cur.first);
      result->push_back(p.filename().string());
    }

    return leveldb::Status::OK();
  }

  virtual leveldb::Status DeleteFile(const std::string& fname)
  {
    int err = ctx_.remove(fname);
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
    int err = ctx_.stat(fname, &size, &mtime);
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
    int err = ctx_.stat(src, &size, &mtime);
    if (err < 0)
    {
      return IOError("RenameFile/stat: " + src, -err);
    }

    librados::bufferlist bl;
    err = ctx_.read(src, bl, size, 0);
    if (err < 0)
    {
      return IOError("RenameFile/read: " + src, -err);
    }

    err = ctx_.write_full(target, bl);
    if (err < 0)
    {
      return IOError("RenameFile/write_full: " + target, -err);
    }

    err = ctx_.remove(src);
    if (err < 0)
    {
      return IOError("RenameFile/remove: " + src, -err);
    }

#if 0
    int err = ctx_.clone_range(target, 0, src, 0, size);
    if (err < 0)
    {
      // return leveldb::Status::IOError(string("RenameFile/clone_range: ") + src + " " + target + " " + strerror(-err));
    }
#endif

    return leveldb::Status::OK();
  }

  virtual leveldb::Status LockFile(const std::string& fname, leveldb::FileLock** lock)
  {
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
  librados::IoCtx ctx_;
};

int main()
{
  int err;
  librados::Rados rados;
  // rados.init("/etc/ceph/ceph.conf");
  err = rados.init(NULL);
  if (err < 0)
  {
    cerr << "Rados::init() failed: " << strerror(-err);
    return 1;
  }

  err = rados.conf_read_file("/etc/ceph/ceph.conf");
  if (err < 0)
  {
    cerr << "Rados::conf_read_file() failed: " << strerror(-err);
    return 1;
  }

  err = rados.connect();
  if (err < 0)
  {
    cerr << "Rados::connect() failed: " << strerror(-err);
    return 1;
  }

  librados::IoCtx ioctx;
  err = rados.ioctx_create("leveldb", ioctx);
  if (err < 0)
  {
    cerr << "Rados::ioctx_create() failed: " << strerror(-err);
    return 1;
  }

  RadosEnv env(ioctx);

  leveldb::Options options;
  options.create_if_missing = true;
  options.env = &env;

  leveldb::Status s;
  leveldb::DB* db;
  s = leveldb::DB::Open(options, "dbname", &db);
  if (!s.ok())
  {
    cerr << s.ToString() << endl;
    return 1;
  }

  s = db->Put(leveldb::WriteOptions(), "key", "value");
  if (!s.ok())
  {
    cerr << s.ToString() << endl;
    return 1;
  }

  std::string val;
  s = db->Get(leveldb::ReadOptions(), "key", &val);
  if (!s.ok())
  {
    cerr << s.ToString() << endl;
    return 1;
  }

  db->CompactRange(NULL, NULL);

  delete db;

#if 0
  librados::ObjectReadOperation oro;
  librados::bufferlist bl;
  oro.read(0, 4, &bl, NULL);
  ioctx.operate("test", &oro, NULL);

  cout << bl << endl;
#endif

  return 0;
}
