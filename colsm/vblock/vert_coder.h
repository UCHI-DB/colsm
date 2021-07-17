//
// Created by Hao Jiang on 12/17/20.
//

#ifndef LEVELDB_VERT_CODER_H
#define LEVELDB_VERT_CODER_H

#include <cstdint>
#include <memory>
#include <unpacker.h>

#include "leveldb/slice.h"

using namespace leveldb;
namespace colsm {

namespace encoding {

class Encoder {
 public:
  virtual ~Encoder() = default;

  virtual void Open() {}

  virtual void Encode(const Slice&) {}

  virtual void Encode(const uint64_t&) {}

  virtual void Encode(const uint32_t&) {}

  virtual void Encode(const uint8_t&) {}

  virtual void Close() = 0;

  virtual uint32_t EstimateSize() const = 0;

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
  virtual std::unique_ptr<Encoder> encoder() = 0;

  virtual std::shared_ptr<Decoder> decoder() = 0;
};

enum EncodingType {
  // For String
  // Store data in <length, value> pair
  PLAIN,
  // Store data in <offset...> <value...>
  LENGTH,
  // For numbers
  BITPACK,
  RUNLENGTH,
  DELTA
};

namespace string {

class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Open() override;
  void Encode(const Slice& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class PlainDecoder : public Decoder {
 private:
  uint8_t* length_pointer_;
  const uint8_t* data_pointer_;

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  Slice Decode() override;
};

class LengthEncoder : public Encoder {
 private:
  uint32_t offset_ = 0;
  std::vector<uint32_t> length_;
  std::string buffer_;

 public:
  void Open() override;
  void Encode(const Slice& value);
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class LengthDecoder : public Decoder {
 private:
  uint32_t* length_pointer_;
  const uint8_t* data_base_;
  const uint8_t* data_pointer_;

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  Slice Decode() override;
};

class EncodingFactory {
 public:
  static Encoding& Get(EncodingType);
};
}  // namespace string

namespace u64 {

class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Open() override;
  void Encode(const uint64_t& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class PlainDecoder : public Decoder {
 private:
  uint64_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint64_t DecodeU64() override;
};

/**
 * Delta and RLE hybrid encoding
 */
class DeltaEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

  int64_t delta_prev_ = 0;
  uint64_t rle_value_;
  uint32_t rle_counter_ = 0;

 public:
  void Open() override;
  void Encode(const uint64_t& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class DeltaDecoder : public Decoder {
 private:
  uint64_t base_ = 0;
  uint64_t rle_value_;
  uint32_t rle_counter_ = 0;
  uint8_t* pointer_;

  void LoadEntry();

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint64_t DecodeU64() override;
};

class EncodingFactory {
 public:
  static Encoding& Get(EncodingType);
};
}  // namespace u64

namespace u32 {

class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Open() override;
  void Encode(const uint32_t& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class PlainDecoder : public Decoder {
 private:
  uint32_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint32_t DecodeU32() override;
};

class BitpackEncoder : public Encoder {
 private:
  std::vector<uint32_t> buffer_;

 public:
  void Open() override;
  void Encode(const uint32_t& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class BitpackDecoder : public Decoder {
 private:
  const uint8_t* base_;
  uint8_t* pointer_;
  uint8_t bit_width_;
  uint8_t index_;
  sboost::Unpacker* unpacker_;
  uint32_t unpacked_[8];

  void LoadNextGroup();

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint32_t DecodeU32() override;
};

class EncodingFactory {
 public:
  static Encoding& Get(EncodingType);
};
}  // namespace u32

namespace u8 {
class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Open() override;
  void Encode(const uint8_t& value) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class PlainDecoder : public Decoder {
 private:
  uint8_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint8_t DecodeU8() override;
};

class RleEncoder : public Encoder {
 private:
  std::vector<uint32_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry();

 public:
  void Open() override;
  void Encode(const uint8_t& entry) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class RleDecoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_ = 0;
  uint32_t* pointer_;

  inline void readEntry() {
    auto result = *(pointer_++);
    value_ = result & 0xFF;
    counter_ = result >> 8;
  }

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint8_t DecodeU8() override;
};

class RleVarIntEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry();

 public:
  void Open() override;
  void Encode(const uint8_t& entry) override;
  uint32_t EstimateSize() const override;
  void Close() override;
  void Dump(uint8_t* output) override;
};

class RleVarIntDecoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_ = 0;
  uint8_t* pointer_;

  void readEntry();

 public:
  void Attach(const uint8_t* buffer) override;
  void Skip(uint32_t offset) override;
  uint8_t DecodeU8() override;
};

class EncodingFactory {
 public:
  static Encoding& Get(EncodingType);
};
}  // namespace u8

}  // namespace encoding
}  // namespace colsm

#endif  // LEVELDB_VERT_CODER_H
