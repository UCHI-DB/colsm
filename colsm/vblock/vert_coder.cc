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

 public:
  void Attach(const uint8_t* buffer) override {
    length_pointer_ = (uint32_t*)buffer;
    data_pointer_ = buffer + 4;
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

class Rle8Encoder : public Encoder {
 private:
  std::vector<uint8_t> buffer_;
  uint8_t last_value_ = 0xFF;
  uint32_t last_counter_ = 0;

  void writeEntry() {
    buffer_.push_back(last_value_);
    // Write var int
    while (last_value_ >= 0x80) {
      buffer_.push_back((uint8_t)(last_value_ | 0x80));
      last_value_ >>= 7;
    }
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

class Rle8Decoder : public Decoder {
 private:
  uint8_t value_;
  uint32_t counter_;
  uint8_t* pointer_;

  void readEntry() {
    value_ = *(pointer_++);
    uint8_t bytec = 0;
    counter_ = 0;
    uint8_t byte;
    do {
      byte = *(pointer_++);
      counter_ |= (byte & 0x7F) << ((bytec++) * 7);
    } while (byte & 0x80);
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

class Delta64Encoder : public Encoder {
 private:
  std::vector<uint8_t*> buffer_;

 public:
  void Encode(const Slice& entry) override {}

  void Encode(const uint64_t& entry) override {}

  void Encode(const uint8_t& entry) override {}

  uint32_t EstimateSize() override { return 0; }

  void Close() override {}

  void Dump(uint8_t* output) override {}
};

class DeltaDecoder : public Decoder {
 public:
  void Attach(const uint8_t* buffer) override {}

  void Skip(uint32_t offset) override {}

  Slice Decode() override { return Slice(0, 0); }

  uint64_t DecodeU64() override { return 0; }

  uint8_t DecodeU8() override { return 0; }
};

template <class E, class D>
class EncodingTemplate : public Encoding {
 public:
  Encoder* encoder() override { return new E(); }

  Decoder* decoder() override { return new D(); }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

EncodingTemplate<LengthEncoder, LengthDecoder> lengthEncoding;

EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

EncodingTemplate<Rle8Encoder, Rle8Decoder> rle8Encoding;

EncodingTemplate<DeltaEncoder, DeltaDecoder> deltaEncoding;

Encoding& EncodingFactory::Get(Encodings encoding) {
  switch (encoding) {
    case PLAIN:
      return plainEncoding;
    case LENGTH:
      return lengthEncoding;
    case BITPACK:
      return bitpackEncoding;
    case RL8:
      return rle8Encoding;
    case DELTA:
      return deltaEncoding;
    default:
      return plainEncoding;
  }
}
}  // namespace colsm