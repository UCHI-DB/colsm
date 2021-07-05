//
// Created by harper on 7/5/21.
//

#include "vert_helper.h"

#include <leveldb/comparator.h>
#include <leveldb/slice.h>

#include "table/block.h"
#include "table/format.h"

namespace colsm {
using namespace leveldb;
bool IsVerticalTable(leveldb::Env* env, const std::string& filename) {
  RandomAccessFile* file;
  uint64_t size;
  Status s;
  s = env->NewRandomAccessFile(filename, &file);
  s = env->GetFileSize(filename, &size);
  if (!s.ok()) {
    return false;
  }
  Slice footer_input;
  char footer_space[Footer::kEncodedLength];
  s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                 &footer_input, footer_space);
  if (!s.ok()) {
    return false;
  }

  Footer footer;
  s = footer.DecodeFrom(&footer_input);

  ReadOptions opt;
  BlockContents contents;
  if (!ReadBlock(file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return false;
  }
  Block* meta = new Block(contents);

  bool usev = false;
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "vformat";
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    usev = 0 == strncmp(iter->value().data(), "true", 4);
  }
  delete iter;
  delete meta;
  return usev;
}

}  // namespace colsm