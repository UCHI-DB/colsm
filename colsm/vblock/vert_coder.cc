//
// Created by Hao Jiang on 12/17/20.
//

#include "vert_coder.h"

#include <cstring>
#include <sstream>
#include <vector>

#include "util/coding.h"

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
}

inline uint64_t readVar64(uint8_t*& pointer) {
  uint8_t bytec = 0;
  uint64_t result = 0;
  uint8_t byte;
  do {
    byte = *(pointer++);
    result |= (byte & 0x7F) << ((bytec++) * 7);
  } while (byte & 0x80);
}

template <class E, class D>
class EncodingTemplate : public Encoding {
 public:
  Encoder* encoder() override { return new E(); }

  Decoder* decoder() override { return new D(); }
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
  uint32_t* length_pointer_;
  const uint8_t* data_pointer_;
  uint8_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    length_pointer_ = (uint32_t*)buffer;
    data_pointer_ = buffer + 4;
    raw_pointer_ = (uint8_t*)buffer;
  }

  void Skip(uint32_t offset) override {
    for (uint32_t i = 0; i < offset; ++i) {
      auto length = *length_pointer_;
      data_pointer_ += length;
      length_pointer_ = (uint32_t*)data_pointer_;
      data_pointer_ += 4;
    }
  }

  Slice Decode() override {
    auto result =
        Slice(reinterpret_cast<const char*>(data_pointer_), *(length_pointer_));
    data_pointer_ += *length_pointer_ + 4;
    length_pointer_ += *length_pointer_;
    return result;
  }
};

class LengthEncoder : public Encoder {
 private:
  uint32_t offset_ = 0;
  std::vector<uint32_t> length_;
  std::string buffer_;

 public:
  void Encode(const Slice& value) override {
    length_.push_back(offset_);
    offset_ += value.size();
    buffer_.append(value.data(), value.size());
  }

  uint32_t EstimateSize() override {
    return length_.size() * 4 + buffer_.size() + 4;
  }

  void Close() override { length_.push_back(offset_); }

  void Dump(uint8_t* output) override {
    auto total_len = length_.size() * 4;
    auto pointer = output;

    *((uint32_t*)pointer) = total_len;
    pointer += 4;
    memcpy(pointer, length_.data(), total_len);
    pointer += total_len;
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
    uint32_t length_len = *((uint32_t*)buffer);
    length_pointer_ = (uint32_t*)(buffer + 4);
    data_base_ = buffer + 4 + length_len;
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

Encoding& EncodingFactory::Get(Encodings encoding) {
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
  uint8_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    raw_pointer_ = (uint8_t*)buffer;
  }

  void Skip(uint32_t offset) override { raw_pointer_ += 8 * offset; }

  uint64_t DecodeU64() override { return read64(raw_pointer_); }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

Encoding& EncodingFactory::Get(Encodings encoding) {
  switch (encoding) {
    case PLAIN:
      return plainEncoding;
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
  uint8_t* raw_pointer_;

 public:
  void Attach(const uint8_t* buffer) override {
    raw_pointer_ = (uint8_t*)buffer;
  }

  void Skip(uint32_t offset) override { raw_pointer_ += 4 * offset; }

  uint32_t DecodeU32() override { return read32(raw_pointer_); }
};
class BitpackEncoder : public Encoder {
 public:
  void Encode(const Slice& entry) override {}

  void Encode(const uint32_t& entry) override {}

  uint32_t EstimateSize() override { return 0; }

  void Close() override {}

  void Dump(uint8_t* output) override {}
};

class BitpackDecoder : public Decoder {
 public:
  void Attach(const uint8_t* buffer) override {}

  void Skip(uint32_t offset) override {}

  Slice Decode() override { return Slice(0, 0); }

  uint32_t DecodeU32() override { return 0; }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;
EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

Encoding& EncodingFactory::Get(Encodings encoding) {
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
  std::vector<uint8_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry() {
    buffer_.push_back(last_value_);
    write32(buffer_, last_counter_);
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

  uint32_t EstimateSize() override { return buffer_.size() + 5; }

  void Close() override {
    if (last_counter_ > 0) {
      writeEntry();
    }
  }

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class RleDecoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_;
  uint8_t* pointer_;

  inline void readEntry() {
    value_ = *(pointer_++);
    counter_ = read32(pointer_);
  }

 public:
  void Attach(const uint8_t* buffer) override {
    pointer_ = (uint8_t*)buffer;
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
    auto result = value_;
    counter_--;
    if (counter_ == 0) {
      readEntry();
    }
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

  uint32_t EstimateSize() override { return buffer_.size() + 5; }

  void Close() override {
    if (last_counter_ > 0) {
      writeEntry();
    }
  }

  void Dump(uint8_t* output) override {
    memcpy(output, buffer_.data(), buffer_.size());
  }
};

class RleVarIntDecoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_;
  uint8_t* pointer_;

  void readEntry() {
    value_ = *(pointer_++);
    counter_ = readVar32(pointer_);
  }

 public:
  void Attach(const uint8_t* buffer) override {
    pointer_ = (uint8_t*)buffer;
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
    auto result = value_;
    counter_--;
    if (counter_ == 0) {
      readEntry();
    }
    return result;
  }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

EncodingTemplate<RleEncoder, RleDecoder> rleEncoding;
EncodingTemplate<RleVarIntEncoder, RleVarIntDecoder> rleVarEncoding;

Encoding& EncodingFactory::Get(Encodings encoding) {
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