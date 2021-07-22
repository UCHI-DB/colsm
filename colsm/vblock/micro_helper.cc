//
// Created by harper on 7/22/21.
//

#include "micro_helper.h"

namespace colsm {

using namespace leveldb;
BlockContents compressBlock(CompressionType ctype, Slice& input) {
  switch(ctype) {
    case kSnappyCompression:
    case kZlibCompression:
    case kNoCompression:
    default:
  }
}

BlockContents decompress(CompressionType ctype, BlockContents compressed) {
  case kSnappyCompression:
  case kZlibCompression:
  case kNoCompression:
  default:
}
}

CompressBlockCore::CompressBlockCore(CompressionType ctype,
                                     BlockContents content)
    : ctype_(ctype), content_(content) {}

CompressBlockCore::~CompressBlockCore() {
  if (content_.heap_allocated) {
    delete[] content_.data;
  }
}

size_t CompressBlockCore::size() const { return inner_->size(); }

Iterator* CompressBlockCore::NewIterator(const Comparator* comparator) {
  // Everytime we request a new iterator, we decompress the content
  auto decompressed = decompress(ctype, content_);
  auto data_pointer = decompressed.data.data();
  auto data_length = decompressed.data.size();
  uint32_t last = *((uint32_t*)(data_pointer + data_length - 4));
  if (last == colsm::MAGIC) {
    inner_ = std::unique_ptr<BlockCore>(new colsm::VertBlockCore(contents));
  } else {
    inner_ = std::unique_ptr<BlockCore>(new BasicBlockCore(contents));
  }
  return inner_->NewIterator(comparator);
}
}  // namespace colsm