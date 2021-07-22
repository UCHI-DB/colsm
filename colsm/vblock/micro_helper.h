//
// Created by harper on 7/22/21.
//

#ifndef LEVELDB_MICRO_HELPER_H
#define LEVELDB_MICRO_HELPER_H

#include <leveldb/options.h>
#include <memory>
#include <string>
#include <table/block.h>
#include <table/format.h>

namespace colsm {

using namespace leveldb;
BlockContents compressBlock(CompressionType, Slice& input);

BlockContents decompress(CompressionType, BlockContents);

/**
 * This class wraps a BlockContents, and decompress the data every time it is
 * requested
 */
class CompressBlockCore : public BlockCore {
 private:
  CompressionType ctype_;
  BlockContents content_;
  std::unique_ptr<BlockCore> inner_;

 public:
  CompressBlockCore(CompressionType, BlockContents);

  virtual ~CompressBlockCore();

  size_t size() const override;

  Iterator* NewIterator(const Comparator* comparator) override;
};
}  // namespace colsm

#endif  // LEVELDB_MICRO_HELPER_H
