//
// Created by Hao Jiang on 12/17/20.
//

#include <cstring>
#include <vector>
#include "vert_coder.h"

namespace leveldb {
namespace vert {

class PlainEncoder : public Encoder {
private:
    std::vector<uint32_t> length_;
    std::string buffer_;
public:
    void Encode(const Slice &value) override {
        length_.push_back(buffer_.size());
        buffer_.append(value.data(), value.size());
    }

    uint32_t EstimateSize() override {
        return length_.size() * 4 + buffer_.size();
    }

    void Finish(uint8_t *output) override {
        auto total_len = length_.size() * 4;
        auto pointer = output;

        *((uint32_t *) pointer) = total_len;
        pointer += 4;
        memcpy(pointer, length_.data(), total_len);
        pointer += total_len;
        memcpy(pointer, buffer_.data(), buffer_.size());
    }
};

class PlainDecoder : public Decoder {
private:
    uint32_t *length_pointer_;
    const uint8_t *data_base_;
    const uint8_t *data_pointer_;
public:
    void Attach(const uint8_t *buffer) override {
        uint32_t length_len = *(uint32_t *) buffer;
        length_pointer_ = (uint32_t *) (buffer + 4);
        data_base_ = buffer + 4 + length_len;
        data_pointer_ = data_base_;
    }

    void Move(uint32_t offset) override {
        length_pointer_ += offset;
        data_pointer_ = data_base_ + *length_pointer_;
    }

    Slice Decode() override {
        return Slice(reinterpret_cast<const char *>(data_pointer_), *(length_pointer_ + 1) - *length_pointer_);
    }
};

class BitpackEncoder : public Encoder {
public:
    void Encode(const Slice &entry) override {

    }

    uint32_t EstimateSize() override {

    }

    void Finish(uint8_t *output) override {

    }
};

class BitpackDecoder : public Decoder {
public:
    void Attach(const uint8_t *buffer) override {

    }

    void Move(uint32_t offset) override {

    }

    Slice Decode() override {

    }
};

template<class E, class D>
class EncodingTemplate : public Encoding {
public:
    Encoder *encoder() {
        return new E();
    }

    Decoder *decoder() {
        return new D();
    }
};

EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

Encoding &EncodingFactory::Get(Encodings encoding) {
    switch (encoding) {
        case PLAIN:
            return plainEncoding;
        case BITPACK:
            return bitpackEncoding;
        default:
            return plainEncoding;
    }
}
}
}