//
// Created by Harper on 6/18/21.
//

#include <iostream>
#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include <leveldb/filter_policy.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"

using namespace std;
using namespace leveldb;
// using namespace colsm;
/*
Status LocalReadBlock(RandomAccessFile* file, const ReadOptions& options,
                      const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}*/

unique_ptr<FilterBlockReader> ReadFilter(const Options& options,
                                         RandomAccessFile* file,
                                         const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return nullptr;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(file, opt, filter_handle, &block).ok()) {
    return nullptr;
  }
  //  if (block.heap_allocated) {
  //    rep_->filter_data = block.data.data();  // Will need to delete later
  //  }
  return unique_ptr<FilterBlockReader>(
      new FilterBlockReader(options.filter_policy, block.data));
}

void ReadMeta(const Footer& footer, Options& options, RandomAccessFile* file) {
  // it is an empty block.
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    std::cout << iter->key().ToString() << " ==> " << iter->value().ToString()
              << '\n';
  }

  delete iter;
  delete meta;
}

int main(int argc, char** argv) {
  string filename = string(argv[1]);
  leveldb::Env* env = leveldb::Env::Default();

  RandomAccessFile* file = nullptr;
  env->NewRandomAccessFile(filename, &file);
  uint64_t size;
  env->GetFileSize(filename, &size);

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return 1;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return 1;

  Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  ReadMeta(footer, options, file);

  delete options.filter_policy;

  return 0;
}
