//
// Created by harper on 7/22/21.
//

#include "micro_helper.h"

#include "port/port_stdcxx.h"

#include "vert_block.h"

namespace colsm {

using namespace leveldb;
using namespace std;

inline BlockContents copy(Slice input) {
  char* copied = new char[input.size()];
  memcpy(copied, input.data(), input.size());
  return BlockContents{Slice(copied, input.size()), true, true};
}

BlockContents compressBlock(CompressionType ctype, Slice& input) {
  switch (ctype) {
    case kSnappyCompression: {
      std::string buffer;
      if (port::Snappy_Compress(input.data(), input.size(), &buffer)) {
        return copy(Slice(buffer));
      } else {
        return copy(input);
      }
    }
    case kZlibCompression: {
      std::string buffer;
      if (port::Zlib_Compress(input.data(), input.size(), &buffer)) {
        return copy(Slice(buffer));
      } else {
        return copy(input);
      }
    }
    case kNoCompression:
    default:
      return copy(input);
  }
}

BlockContents decompress(CompressionType ctype, BlockContents compressed) {
  switch (ctype) {
    case kSnappyCompression: {
      size_t ulen;
      port::Snappy_GetUncompressedLength(compressed.data.data(),
                                         compressed.data.size(), &ulen);
      auto buffer = new char[ulen];
      port::Snappy_Uncompress(compressed.data.data(), compressed.data.size(),
                              buffer);
      return BlockContents{Slice(buffer, ulen), true, true};
    }
    case kZlibCompression: {
      size_t ulen = compressed.data.size() * 2;
      auto buffer = new char[ulen];
      port::Zlib_Uncompress(compressed.data.data(), compressed.data.size(),
                            buffer, &ulen);
      return BlockContents{Slice(buffer, ulen), true, true};
    }
    case kNoCompression:
    default:
      return BlockContents{compressed.data, true, false};
  }
}

CompressBlockCore::CompressBlockCore(CompressionType ctype,
                                     BlockContents content)
    : ctype_(ctype), content_(content) {}

CompressBlockCore::~CompressBlockCore() {
  if (content_.heap_allocated) {
    delete[] content_.data.data();
  }
}

size_t CompressBlockCore::size() const { return inner_->size(); }

Iterator* CompressBlockCore::NewIterator(const Comparator* comparator) {
  // Everytime we request a new iterator, we decompress the content
  auto decompressed = decompress(ctype_, content_);
  auto data_pointer = decompressed.data.data();
  auto data_length = decompressed.data.size();
  uint32_t last = *((uint32_t*)(data_pointer + data_length - 4));
  if (last == colsm::MAGIC) {
    inner_ = std::unique_ptr<BlockCore>(new colsm::VertBlockCore(decompressed));
  } else {
    inner_ = std::unique_ptr<BlockCore>(new BasicBlockCore(decompressed));
  }
  return inner_->NewIterator(comparator);
}
}  // namespace colsm