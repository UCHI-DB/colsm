//
// Created by Hao Jiang on 12/17/20.
//

#ifndef LEVELDB_VERT_CODER_H
#define LEVELDB_VERT_CODER_H

#include <cstdint>

#include "leveldb/slice.h"

using namespace leveldb;
namespace colsm {

namespace encoding {}

class Encoder {
 public:
  virtual void Encode(const Slice&) = 0;

  virtual void Close() = 0;

  virtual uint32_t EstimateSize() = 0;

  virtual void Dump(uint8_t*) = 0;
};

class Decoder {
 public:
  virtual void Attach(const uint8_t*) = 0;

  // Move forward by records
  virtual void Skip(uint32_t offset) = 0;

  virtual Slice Decode() = 0;
};

class Encoding {
 public:
  virtual Encoder* encoder() = 0;

  virtual Decoder* decoder() = 0;
};

enum Encodings {
  // Store data in <length, value> pair
  PLAIN,
  // Store data in <offset...> <value...>
  LENGTH,
  BITPACK
};

class EncodingFactory {
 public:
  static Encoding& Get(Encodings);
};
}  // namespace colsm

#endif  // LEVELDB_VERT_CODER_H
