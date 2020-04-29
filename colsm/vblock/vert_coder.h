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
  virtual ~Encoder() = default;

  virtual void Encode(const Slice&) {}

  virtual void Encode(const uint64_t&) {}

  virtual void Encode(const uint32_t&) {}

  virtual void Encode(const uint8_t&) {}

  virtual void Close() = 0;

  virtual uint32_t EstimateSize() = 0;

  virtual void Dump(uint8_t*) = 0;
};

class Decoder {
 public:
  virtual ~Decoder() = default;

  virtual void Attach(const uint8_t*) = 0;

  // Move forward by records
  virtual void Skip(uint32_t offset) = 0;

  virtual Slice Decode() { return Slice(); }

  virtual uint64_t DecodeU64() { return 0; }

  virtual uint32_t DecodeU32() { return 0; }

  virtual uint8_t DecodeU8() { return 0; }
};

class Encoding {
 public:
  virtual Encoder* encoder() = 0;

  virtual Decoder* decoder() = 0;
};

enum Encodings {
  // For String
  // Store data in <length, value> pair
  PLAIN,
  // Store data in <offset...> <value...>
  LENGTH,
  // For numbers
  BITPACK,
  RL8,
  DELTA
};

class EncodingFactory {
 public:
  static Encoding& Get(Encodings);
};
}  // namespace colsm

#endif  // LEVELDB_VERT_CODER_H
