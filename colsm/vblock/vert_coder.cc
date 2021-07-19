//
// Created by Hao Jiang on 12/17/20.
//

#include "vert_coder.h"

#include <byteutils.h>
#include <cstring>
#include <immintrin.h>
#include <sstream>
#include <unpacker.h>
#include <vector>

#include "util/coding.h"

#include "../respool/respool.h"

using namespace leveldb;
namespace colsm {
namespace encoding {
inline void write32(std::vector<uint8_t>& dest, uint32_t value) {
  auto oldsize = dest.size();
  dest.resize(oldsize + 4);
  *(uint32_t*)(dest.data() + oldsize) = value;
}

inline uint32_t read32(uint8_t*& pointer) {
  uint32_t value = *((uint32_t*)pointer);
  pointer += 4;
  return value;
}

inline uint32_t read64(uint8_t*& pointer) {
  uint64_t value = *((uint64_t*)pointer);
  pointer += 8;
  return value;
}

inline void write64(std::vector<uint8_t>& dest, uint64_t value) {
  auto oldsize = dest.size();
  dest.resize(oldsize + 8);
  *(uint64_t*)(dest.data() + oldsize) = value;
}

inline void writeVar32(std::vector<uint8_t>& dest, uint32_t value) {
  while (value >= 0x80) {
    dest.push_back(((uint8_t)value) | 0x80);
    value >>= 7;
  }
  dest.push_back((uint8_t)value);
}

inline void writeVar64(std::vector<uint8_t>& dest, uint64_t value) {
  while (value >= 0x80) {
    dest.push_back(((uint8_t)value) | 0x80);
    value >>= 7;
  }
  dest.push_back((uint8_t)value);
}

inline uint32_t readVar32(uint8_t*& pointer) {
  uint8_t bytec = 0;
  uint32_t result = 0;
  uint8_t byte;
  do {
    byte = *(pointer++);
    result |= (byte & 0x7F) << ((bytec++) * 7);
  } while (byte & 0x80);
  return result;
}

inline uint64_t readVar64(uint8_t*& pointer) {
  uint8_t bytec = 0;
  uint64_t result = 0;
  uint8_t byte;
  do {
    byte = *(pointer++);
    result |= (byte & 0x7F) << ((bytec++) * 7);
  } while (byte & 0x80);
  return result;
}

inline uint64_t zigzagEncoding(int64_t value) {
  return (value << 1) ^ (value >> 63);
}

inline int64_t zigzagDecoding(uint64_t value) {
  return (value >> 1) ^ -(value & 1);
}

template <class E, class D>
class EncodingTemplate : public Encoding {
 private:
  simple_pool<Encoder> epool_;
  simple_pool<Decoder> dpool_;

 public:
  EncodingTemplate()
      : epool_(10, []() { return new E(); }),
        dpool_(10, []() { return new D(); }) {}

  std::unique_ptr<Encoder> encoder() override {
    //        return epool_.get();
    return std::make_unique<E>();
  }

  std::shared_ptr<Decoder> decoder() override {
    return dpool_.get();
    //    return std::make_unique<D>();
  }
};

namespace string {
void PlainEncoder::Open() { buffer_.clear(); }

void PlainEncoder::Encode(const Slice& value) {
  auto buffer_size = buffer_.size();
  buffer_.resize(buffer_size + value.size() + 4);
  *((uint32_t*)(buffer_.data() + buffer_size)) = value.size();
  memcpy(buffer_.data() + buffer_size + 4, value.data(), value.size());
}

uint32_t PlainEncoder::EstimateSize() const { return buffer_.size(); }

void PlainEncoder::Close() {}

void PlainEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void PlainDecoder::Attach(const uint8_t* buffer) {
  length_pointer_ = (uint8_t*)buffer;
  data_pointer_ = buffer + 4;
}

void PlainDecoder::Skip(uint32_t offset) {
  for (uint32_t i = 0; i < offset; ++i) {
    auto length = *((uint32_t*)length_pointer_);
    data_pointer_ += length + 4;
    length_pointer_ += length + 4;
  }
}

Slice PlainDecoder::Decode() {
  auto length = *((uint32_t*)length_pointer_);
  auto result = Slice(reinterpret_cast<const char*>(data_pointer_), length);
  data_pointer_ += length + 4;
  length_pointer_ += length + 4;
  return result;
}

void LengthEncoder::Open() {
  offset_ = 0;
  length_.clear();
  length_.push_back(0);
  buffer_.clear();
}

void LengthEncoder::Encode(const Slice& value) {
  offset_ += value.size();
  length_.push_back(offset_);
  buffer_.append(value.data(), value.size());
}

uint32_t LengthEncoder::EstimateSize() const {
  return length_.size() * 4 + buffer_.size() + 4;
}

void LengthEncoder::Close() {}

void LengthEncoder::Dump(uint8_t* output) {
  auto pointer = output;

  auto len_size = sizeof(uint32_t) * length_.size();
  *((uint32_t*)pointer) = len_size;
  pointer += 4;
  memcpy(pointer, length_.data(), len_size);
  pointer += len_size;
  memcpy(pointer, buffer_.data(), buffer_.size());
}

void LengthDecoder::Attach(const uint8_t* buffer) {
  uint32_t data_pos = *((uint32_t*)buffer);
  length_pointer_ = (uint32_t*)(buffer + 4);
  data_base_ = buffer + data_pos + 4;
  data_pointer_ = data_base_;
}

void LengthDecoder::Skip(uint32_t offset) {
  length_pointer_ += offset;
  data_pointer_ = data_base_ + *length_pointer_;
}

Slice LengthDecoder::Decode() {
  auto length = *(length_pointer_ + 1) - (*length_pointer_);
  auto result = Slice(reinterpret_cast<const char*>(data_pointer_), length);
  data_pointer_ += length;
  length_pointer_++;
  return result;
}

Encoding& EncodingFactory::Get(EncodingType encoding) {
  static EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
  static EncodingTemplate<LengthEncoder, LengthDecoder> lengthEncoding;
  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case LENGTH:
      return lengthEncoding;
    default:
      return plainEncoding;
  }
}

}  // namespace string

namespace u64 {

void PlainEncoder::Open() { buffer_.clear(); }

void PlainEncoder::Encode(const uint64_t& value) { write64(buffer_, value); }

uint32_t PlainEncoder::EstimateSize() const { return buffer_.size(); }

void PlainEncoder::Close() {}

void PlainEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void PlainDecoder::Attach(const uint8_t* buffer) {
  raw_pointer_ = (uint64_t*)buffer;
}

void PlainDecoder::Skip(uint32_t offset) { raw_pointer_ += offset; }

uint64_t PlainDecoder::DecodeU64() { return *(raw_pointer_++); }

void DeltaEncoder::Open() {
  buffer_.clear();
  delta_prev_ = 0;
  rle_counter_ = 0;
}

void DeltaEncoder::Encode(const uint64_t& value) {
  auto delta = zigzagEncoding(value - delta_prev_);
  if (delta != rle_value_) {
    if (rle_counter_ > 0) {
      writeVar64(buffer_, rle_value_);
      writeVar32(buffer_, rle_counter_);
    }
    rle_value_ = delta;
    rle_counter_ = 1;
  } else {
    rle_counter_++;
  }
  delta_prev_ = value;
}

uint32_t DeltaEncoder::EstimateSize() const {
  return buffer_.size() + (rle_counter_ != 0) * 12;
}

void DeltaEncoder::Close() {
  writeVar64(buffer_, rle_value_);
  writeVar32(buffer_, rle_counter_);
  rle_counter_ = 0;
}

void DeltaEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void DeltaDecoder::LoadEntry() {
  rle_value_ = zigzagDecoding(readVar64(pointer_));
  rle_counter_ = readVar32(pointer_);
}

void DeltaDecoder::Attach(const uint8_t* buffer) {
  pointer_ = (uint8_t*)buffer;
}

void DeltaDecoder::Skip(uint32_t offset) {
  uint32_t remain = offset;
  while (remain >= rle_counter_) {
    remain -= rle_counter_;
    base_ += rle_value_ * rle_counter_;
    LoadEntry();
  }
  rle_counter_ -= remain;
  base_ += rle_value_ * remain;
}

uint64_t DeltaDecoder::DecodeU64() {
  if (rle_counter_ == 0) {
    LoadEntry();
  }
  rle_counter_--;
  base_ += rle_value_;
  return base_;
}

void BitpackEncoder::Open() {
  buffer_.clear();
  min_ = INT64_MAX;
  max_ = 0;
}

void BitpackEncoder::Encode(const uint64_t& value) {
  buffer_.push_back(value);
  min_ = std::min(min_, value);
  max_ = std::max(max_, value);
}

uint32_t BitpackEncoder::EstimateSize() const {
  uint8_t bit_width = 64 - _lzcnt_u64(max_ - min_);
  // The buffer should be large enough for a 256 bit read after valid data
  uint32_t buffer_group_size = (buffer_.size() + 7) >> 3;
  uint32_t size = 1 + bit_width * buffer_group_size + 32;
  return size;
}

void BitpackEncoder::Close() {}

void BitpackEncoder::Dump(uint8_t* output) {
  uint8_t bit_width = 64 - _lzcnt_u64(max_ - min_);
  *((uint64_t*)output) = min_;
  *(output + 8) = bit_width;
  assert(bit_width < 32);

  std::vector<uint32_t> another;
  another.reserve(buffer_.size());
  for (auto& i : buffer_) {
    another.push_back(i - min_);
  }
  sboost::byteutils::bitpack(another.data(), buffer_.size(), bit_width,
                             output + 9);
}

void BitpackDecoder::LoadNextGroup() {
  auto up = unpacker_->unpack(pointer_);
  memcpy(unpacked_, (uint8_t*)&up, 32);
  index_ = 0;
  pointer_ += bit_width_;
}

void BitpackDecoder::Attach(const uint8_t* buffer) {
  base_ = buffer;
  min_ = *((uint64_t*)buffer);
  bit_width_ = *(buffer + 8);
  pointer_ = (uint8_t*)buffer + 9;
  assert(bit_width_ < 32);
  unpacker_ = sboost::unpackers[bit_width_];
  LoadNextGroup();
}

void BitpackDecoder::Skip(uint32_t offset) {
  index_ += offset;
  auto group_index = index_ >> 3;
  index_ &= 0x7;
  if (group_index > 0) {
    pointer_ += (group_index - 1) * bit_width_;
    auto index_back = index_;
    LoadNextGroup();
    index_ = index_back;
  }
}

uint64_t BitpackDecoder::DecodeU64() {
  auto entry = unpacked_[index_];
  index_++;
  if (index_ >= 8) {
    LoadNextGroup();
  }
  return entry + min_;
}

Encoding& EncodingFactory::Get(EncodingType encoding) {
  static EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
  static EncodingTemplate<DeltaEncoder, DeltaDecoder> deltaEncoding;
  static EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case DELTA:
      return deltaEncoding;
    case BITPACK:
      return bitpackEncoding;
    default:
      return plainEncoding;
  }
}
}  // namespace u64

namespace u32 {

void PlainEncoder::Open() { buffer_.clear(); }
void PlainEncoder::Encode(const uint32_t& value) { write32(buffer_, value); }

uint32_t PlainEncoder::EstimateSize() const { return buffer_.size(); }

void PlainEncoder::Close() {}

void PlainEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void PlainDecoder::Attach(const uint8_t* buffer) {
  raw_pointer_ = (uint32_t*)buffer;
}

void PlainDecoder::Skip(uint32_t offset) { raw_pointer_ += offset; }

uint32_t PlainDecoder::DecodeU32() { return *(raw_pointer_++); }

void BitpackEncoder::Open() { buffer_.clear(); }

void BitpackEncoder::Encode(const uint32_t& value) { buffer_.push_back(value); }

uint32_t BitpackEncoder::EstimateSize() const {
  uint32_t bit_width = 32 - _lzcnt_u32(buffer_.back());
  // The buffer should be large enough for a 256 bit read after valid data
  uint32_t buffer_group_size = (buffer_.size() + 7) >> 3;
  uint32_t size = 1 + bit_width * buffer_group_size + 32;
  return size;
}

void BitpackEncoder::Close() {}

void BitpackEncoder::Dump(uint8_t* output) {
  uint8_t bit_width = 32 - _lzcnt_u32(buffer_.back());
  *(output) = bit_width;
  sboost::byteutils::bitpack(buffer_.data(), buffer_.size(), bit_width,
                             output + 1);
}

void BitpackDecoder::LoadNextGroup() {
  auto up = unpacker_->unpack(pointer_);
  memcpy(unpacked_, (uint8_t*)&up, 32);
  index_ = 0;
  pointer_ += bit_width_;
}

void BitpackDecoder::Attach(const uint8_t* buffer) {
  base_ = buffer;
  bit_width_ = *buffer;
  pointer_ = (uint8_t*)buffer + 1;
  unpacker_ = sboost::unpackers[bit_width_];
  LoadNextGroup();
}

void BitpackDecoder::Skip(uint32_t offset) {
  index_ += offset;
  auto group_index = index_ >> 3;
  index_ &= 0x7;
  if (group_index > 0) {
    pointer_ += (group_index - 1) * bit_width_;
    auto index_back = index_;
    LoadNextGroup();
    index_ = index_back;
  }
}

uint32_t BitpackDecoder::DecodeU32() {
  auto entry = unpacked_[index_];
  index_++;
  if (index_ >= 8) {
    LoadNextGroup();
  }
  return entry;
}

Encoding& EncodingFactory::Get(EncodingType encoding) {
  static EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
  static EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;
  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case BITPACK:
      return bitpackEncoding;
    default:
      return plainEncoding;
  }
}
}  // namespace u32

namespace u8 {
void PlainEncoder::Open() { buffer_.clear(); }
void PlainEncoder::Encode(const uint8_t& value) { buffer_.push_back(value); }
uint32_t PlainEncoder::EstimateSize() const { return buffer_.size(); }
void PlainEncoder::Close() {}
void PlainEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void PlainDecoder::Attach(const uint8_t* buffer) {
  raw_pointer_ = (uint8_t*)buffer;
}

void PlainDecoder::Skip(uint32_t offset) { raw_pointer_ += offset; }

uint8_t PlainDecoder::DecodeU8() { return *(raw_pointer_++); }

void RleEncoder::writeEntry() {
  buffer_.push_back((last_counter_ << 8) + last_value_);
}

void RleEncoder::Open() {
  buffer_.clear();
  last_value_ = 0xFF;
  last_counter_ = 0;
}
void RleEncoder::Encode(const uint8_t& entry) {
  if (entry == last_value_ && last_counter_ != 0) {
    last_counter_++;
  } else {
    if (last_counter_ > 0) {
      writeEntry();
    }
    last_value_ = entry;
    last_counter_ = 1;
  }
}

uint32_t RleEncoder::EstimateSize() const {
  return (buffer_.size() + (last_counter_ != 0)) * sizeof(uint32_t);
}

void RleEncoder::Close() {
  if (last_counter_ > 0) {
    writeEntry();
    last_counter_ = 0;
  }
}

void RleEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size() * sizeof(uint32_t));
}

void RleDecoder::Attach(const uint8_t* buffer) {
  pointer_ = (uint32_t*)buffer;
  readEntry();
}

void RleDecoder::Skip(uint32_t offset) {
  auto remain = offset;
  while (remain >= counter_) {
    remain -= counter_;
    readEntry();
  }
  counter_ -= remain;
}

uint8_t RleDecoder::DecodeU8() {
  if (counter_ == 0) {
    readEntry();
  }
  auto result = value_;
  counter_--;
  return result;
}

void RleVarIntEncoder::writeEntry() {
  buffer_.push_back(last_value_);
  // Write var int
  writeVar32(buffer_, last_counter_);
}

void RleVarIntEncoder::Open() {
  buffer_.clear();
  last_value_ = 0xFF;
  last_counter_ = 0;
}

void RleVarIntEncoder::Encode(const uint8_t& entry) {
  if (entry == last_value_ && last_counter_ != 0) {
    last_counter_++;
  } else {
    if (last_counter_ > 0) {
      writeEntry();
    }
    last_value_ = entry;
    last_counter_ = 1;
  }
}

uint32_t RleVarIntEncoder::EstimateSize() const {
  return buffer_.size() + (last_counter_ != 0) * 5;
}

void RleVarIntEncoder::Close() {
  if (last_counter_ > 0) {
    writeEntry();
    last_counter_ = 0;
  }
}

void RleVarIntEncoder::Dump(uint8_t* output) {
  memcpy(output, buffer_.data(), buffer_.size());
}

void RleVarIntDecoder::readEntry() {
  value_ = *(pointer_++);
  counter_ = readVar32(pointer_);
}

void RleVarIntDecoder::Attach(const uint8_t* buffer) {
  pointer_ = (uint8_t*)buffer;
}

void RleVarIntDecoder::Skip(uint32_t offset) {
  auto remain = offset;
  while (remain >= counter_) {
    remain -= counter_;
    readEntry();
  }
  counter_ -= remain;
}

uint8_t RleVarIntDecoder::DecodeU8() {
  if (counter_ == 0) {
    readEntry();
  }
  counter_--;
  return value_;
}

Encoding& EncodingFactory::Get(EncodingType encoding) {
  static EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
  static EncodingTemplate<RleEncoder, RleDecoder> rleEncoding;
  static EncodingTemplate<RleVarIntEncoder, RleVarIntDecoder> rleVarEncoding;

  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case RUNLENGTH:
      return rleEncoding;
    case BITPACK:
      return rleVarEncoding;
    default:
      return plainEncoding;
  }
}
}  // namespace u8

}  // namespace encoding
}  // namespace colsm