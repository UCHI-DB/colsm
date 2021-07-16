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

// Use a resource pool of encoder/decoder
template <class E, class D>
class EncodingTemplate : public Encoding {
private:
    ResPool<E> epool_;
    ResPool<D> dpool_;
 public:
    EncodingTemplate():epool_(10),dpool_(10) {}

  std::unique_ptr<Encoder> encoder() override {
    return epool_.Get();
  }

  std::unique_ptr<Decoder> decoder() override {
    return dpool_.Get();
  }
};

namespace string {
class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Encode(const Slice& value) override {
    auto buffer_size = buffer_.size();
    buffer_.resize(buffer_size + value.size() + 4);
    *((uint32_t*)(buffer_.data() + buffer_size)) = value.size();
    memcpy(buffer_.data() + buffer_size + 4, value.data(), value.size());
  }

  uint32_t EstimateSize() override { return buffer_.size(); }

  void Close() override {}

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class PlainDecoder : public Decoder {
 private:
  uint8_t* length_pointer_;
  const uint8_t* data_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    length_pointer_ = (uint8_t*)buffer;
    data_pointer_ = buffer + 4;
  }

  void Skip(uint32_t offset) override {
    for (uint32_t i = 0; i < offset; ++i) {
      auto length = *((uint32_t*)length_pointer_);
      data_pointer_ += length + 4;
      length_pointer_ += length + 4;
    }
  }

  Slice Decode() override {
    auto length = *((uint32_t*)length_pointer_);
    auto result = Slice(reinterpret_cast<const char*>(data_pointer_), length);
    data_pointer_ += length + 4;
    length_pointer_ += length + 4;
    return result;
  }
};

class LengthEncoder : public Encoder {
 private:
  uint32_t offset_ = 0;
  std::vector<uint32_t> length_;
  std::string buffer_;

 public:
  LengthEncoder() { length_.push_back(0); }

  void Encode(const Slice& value) override {
    offset_ += value.size();
    length_.push_back(offset_);
    buffer_.append(value.data(), value.size());
  }

  uint32_t EstimateSize() override {
    return length_.size() * 4 + buffer_.size() + 4;
  }

  void Close() override {}

  void Dump(uint8_t* output) override {
    auto pointer = output;

    auto len_size = sizeof(uint32_t) * length_.size();
    *((uint32_t*)pointer) = len_size;
    pointer += 4;
    memcpy(pointer, length_.data(), len_size);
    pointer += len_size;
    memcpy(pointer, buffer_.data(), buffer_.size());
  }
};

class LengthDecoder : public Decoder {
 private:
  uint32_t* length_pointer_;
  const uint8_t* data_base_;
  const uint8_t* data_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    uint32_t data_pos = *((uint32_t*)buffer);
    length_pointer_ = (uint32_t*)(buffer + 4);
    data_base_ = buffer + data_pos + 4;
    data_pointer_ = data_base_;
  }

  void Skip(uint32_t offset) override {
    length_pointer_ += offset;
    data_pointer_ = data_base_ + *length_pointer_;
  }

  Slice Decode() override {
    auto length = *(length_pointer_ + 1) - (*length_pointer_);
    auto result = Slice(reinterpret_cast<const char*>(data_pointer_), length);
    data_pointer_ += length;
    length_pointer_++;
    return result;
  }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

EncodingTemplate<LengthEncoder, LengthDecoder> lengthEncoding;

Encoding& EncodingFactory::Get(EncodingType encoding) {
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
class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Encode(const uint64_t& value) override { write64(buffer_, value); }

  uint32_t EstimateSize() override { return buffer_.size(); }

  void Close() override {}

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class PlainDecoder : public Decoder {
 private:
  uint64_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    raw_pointer_ = (uint64_t*)buffer;
  }

  void Skip(uint32_t offset) override { raw_pointer_ += offset; }

  uint64_t DecodeU64() override { return *(raw_pointer_++); }
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
  void Encode(const uint64_t& value) override {
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

  uint32_t EstimateSize() override {
    return buffer_.size() + (rle_counter_ != 0) * 12;
  }

  void Close() override {
    writeVar64(buffer_, rle_value_);
    writeVar32(buffer_, rle_counter_);
    rle_counter_ = 0;
  }

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class DeltaDecoder : public Decoder {
 private:
  uint64_t base_ = 0;
  uint64_t rle_value_;
  uint32_t rle_counter_ = 0;
  uint8_t* pointer_;

  void LoadEntry() {
    rle_value_ = zigzagDecoding(readVar64(pointer_));
    rle_counter_ = readVar32(pointer_);
  }

 public:
  void Attach(const uint8_t* buffer) override { pointer_ = (uint8_t*)buffer; }

  void Skip(uint32_t offset) override {
    uint32_t remain = offset;
    while (remain >= rle_counter_) {
      remain -= rle_counter_;
      base_ += rle_value_ * rle_counter_;
      LoadEntry();
    }
    rle_counter_ -= remain;
    base_ += rle_value_ * remain;
  }

  uint64_t DecodeU64() override {
    if (rle_counter_ == 0) {
      LoadEntry();
    }
    rle_counter_--;
    base_ += rle_value_;
    return base_;
  }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
EncodingTemplate<DeltaEncoder, DeltaDecoder> deltaEncoding;

Encoding& EncodingFactory::Get(EncodingType encoding) {
  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case DELTA:
      return deltaEncoding;
    default:
      return plainEncoding;
  }
}
}  // namespace u64

namespace u32 {
class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Encode(const uint32_t& value) override { write32(buffer_, value); }

  uint32_t EstimateSize() override { return buffer_.size(); }

  void Close() override {}

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class PlainDecoder : public Decoder {
 private:
  uint32_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    raw_pointer_ = (uint32_t*)buffer;
  }

  void Skip(uint32_t offset) override { raw_pointer_ += offset; }

  uint32_t DecodeU32() override { return *(raw_pointer_++); }
};

class BitpackEncoder : public Encoder {
 private:
  std::vector<uint32_t> buffer_;

 public:
  void Encode(const uint32_t& value) override { buffer_.push_back(value); }

  uint32_t EstimateSize() override {
    uint32_t bit_width = 32 - _lzcnt_u32(buffer_.back());
    // The buffer should be large enough for a 256 bit read after valid data
    uint32_t buffer_group_size = (buffer_.size() + 7) >> 3;
    uint32_t size = 1 + bit_width * buffer_group_size + 32;
    return size;
  }

  void Close() override {}

  void Dump(uint8_t* output) override {
    uint8_t bit_width = 32 - _lzcnt_u32(buffer_.back());
    *(output) = bit_width;
    sboost::byteutils::bitpack(buffer_.data(), buffer_.size(), bit_width,
                               output + 1);
  }
};

class BitpackDecoder : public Decoder {
 private:
  const uint8_t* base_;
  uint8_t* pointer_;
  uint8_t bit_width_;
  uint8_t index_;
  sboost::Unpacker* unpacker_;
  alignas(64) __m256i unpacked_;

  void LoadNextGroup() {
    unpacked_ = unpacker_->unpack(pointer_);
    index_ = 0;
    pointer_ += bit_width_;
  }

 public:
  void Attach(const uint8_t* buffer) override {
    base_ = buffer;
    bit_width_ = *buffer;
    pointer_ = (uint8_t*)buffer + 1;
    unpacker_ = sboost::unpackers[bit_width_];
    LoadNextGroup();
  }

  void Skip(uint32_t offset) override {
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

  uint32_t DecodeU32() override {
    auto entry = (unpacked_[index_ / 2] >> (32 * (index_ & 1))) & (uint32_t)-1;
    index_++;
    if (index_ >= 8) {
      LoadNextGroup();
    }
    return entry;
  }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

Encoding& EncodingFactory::Get(EncodingType encoding) {
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
class PlainEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;

 public:
  void Encode(const uint8_t& value) override { buffer_.push_back(value); }

  uint32_t EstimateSize() override { return buffer_.size(); }

  void Close() override {}

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class PlainDecoder : public Decoder {
 private:
  uint8_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    raw_pointer_ = (uint8_t*)buffer;
  }

  void Skip(uint32_t offset) override { raw_pointer_ += offset; }

  uint8_t DecodeU8() override { return *(raw_pointer_++); }
};

class RleEncoder : public Encoder {
 private:
  std::vector<uint32_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry() { buffer_.push_back((last_counter_ << 8) + last_value_); }

 public:
  void Encode(const uint8_t& entry) override {
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

  uint32_t EstimateSize() override {
    return (buffer_.size() + (last_counter_ != 0)) * sizeof(uint32_t);
  }

  void Close() override {
    if (last_counter_ > 0) {
      writeEntry();
      last_counter_ = 0;
    }
  }

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size() * sizeof(uint32_t));
  }
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
  void Attach(const uint8_t* buffer) override {
    pointer_ = (uint32_t*)buffer;
    readEntry();
  }

  void Skip(uint32_t offset) override {
    auto remain = offset;
    while (remain >= counter_) {
      remain -= counter_;
      readEntry();
    }
    counter_ -= remain;
  }

  uint8_t DecodeU8() override {
    if (counter_ == 0) {
      readEntry();
    }
    auto result = value_;
    counter_--;
    return result;
  }
};

class RleVarIntEncoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry() {
    buffer_.push_back(last_value_);
    // Write var int
    writeVar32(buffer_, last_counter_);
  }

 public:
  void Encode(const uint8_t& entry) override {
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

  uint32_t EstimateSize() override {
    return buffer_.size() + (last_counter_ != 0) * 5;
  }

  void Close() override {
    if (last_counter_ > 0) {
      writeEntry();
      last_counter_ = 0;
    }
  }

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class RleVarIntDecoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_ = 0;
  uint8_t* pointer_;

  void readEntry() {
    value_ = *(pointer_++);
    counter_ = readVar32(pointer_);
  }

 public:
  void Attach(const uint8_t* buffer) override { pointer_ = (uint8_t*)buffer; }

  void Skip(uint32_t offset) override {
    auto remain = offset;
    while (remain >= counter_) {
      remain -= counter_;
      readEntry();
    }
    counter_ -= remain;
  }

  uint8_t DecodeU8() override {
    if (counter_ == 0) {
      readEntry();
    }
    counter_--;
    return value_;
  }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
EncodingTemplate<RleEncoder, RleDecoder> rleEncoding;
EncodingTemplate<RleVarIntEncoder, RleVarIntDecoder> rleVarEncoding;

Encoding& EncodingFactory::Get(EncodingType encoding) {
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