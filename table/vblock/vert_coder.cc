//
// Created by Hao Jiang on 12/17/20.
//

#include <cstring>
#include <vector>
#include <sstream>
#include "vert_coder.h"

namespace leveldb {
    namespace vert {

        class PlainEncoder : public Encoder {
        private:
            std::vector<uint8_t> buffer_;
        public:
            void Encode(const Slice &value) override {
                auto buffer_size = buffer_.size();
                buffer_.resize(buffer_size + value.size() + 4);
                *((uint32_t *) (buffer_.data() + buffer_size)) = value.size();
                memcpy(buffer_.data() + buffer_size + 4, value.data(), value.size());
            }

            uint32_t EstimateSize() override {
                return buffer_.size();
            }

            void Close() override {

            }

            void Dump(uint8_t *output) override {
                memcpy(output, buffer_.data(), buffer_.size());
            }
        };

        class PlainDecoder : public Decoder {
        private:
            uint32_t *length_pointer_;
            const uint8_t *data_pointer_;
        public:
            void Attach(const uint8_t *buffer) override {
                length_pointer_ = (uint32_t *) buffer;
                data_pointer_ = buffer + 4;
            }

            void Skip(uint32_t offset) override {
                for (uint32_t i = 0; i < offset; ++i) {
                    auto length = *length_pointer_;
                    data_pointer_ += length;
                    length_pointer_ = (uint32_t *) data_pointer_;
                    data_pointer_ += 4;
                }
            }

            Slice Decode() override {
                auto result = Slice(reinterpret_cast<const char *>(data_pointer_), *(length_pointer_));
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
            void Encode(const Slice &value) override {
                length_.push_back(offset_);
                offset_ += value.size();
                buffer_.append(value.data(), value.size());
            }

            uint32_t EstimateSize() override {
                return length_.size() * 4 + buffer_.size() + 4;
            }

            void Close() override {
                length_.push_back(offset_);
            }

            void Dump(uint8_t *output) override {
                auto total_len = length_.size() * 4;
                auto pointer = output;

                *((uint32_t *) pointer) = total_len;
                pointer += 4;
                memcpy(pointer, length_.data(), total_len);
                pointer += total_len;
                memcpy(pointer, buffer_.data(), buffer_.size());
            }
        };

        class LengthDecoder : public Decoder {
        private:
            uint32_t *length_pointer_;
            const uint8_t *data_base_;
            const uint8_t *data_pointer_;
        public:
            void Attach(const uint8_t *buffer) override {
                uint32_t length_len = *((uint32_t *) buffer);
                length_pointer_ = (uint32_t *) (buffer + 4);
                data_base_ = buffer + 4 + length_len;
                data_pointer_ = data_base_;
            }

            void Skip(uint32_t offset) override {
                length_pointer_ += offset;
                data_pointer_ = data_base_ + *length_pointer_;
            }

            Slice Decode() override {
                auto length = *(length_pointer_ + 1) - (*length_pointer_);
                auto result = Slice(reinterpret_cast<const char *>(data_pointer_), length);
                data_pointer_ += length;
                length_pointer_++;
                return result;
            }
        };

        class BitpackEncoder : public Encoder {
        public:
            void Encode(const Slice &entry) override {}

            uint32_t EstimateSize() override {}

            void Close() override {}

            void Dump(uint8_t *output) override {}
        };

        class BitpackDecoder : public Decoder {
        public:
            void Attach(const uint8_t *buffer) override {}

            void Skip(uint32_t offset) override {}

            Slice Decode() override {}
        };

        template<class E, class D>
        class EncodingTemplate : public Encoding {
        public:
            Encoder *encoder() override {
                return new E();
            }

            Decoder *decoder() override {
                return new D();
            }
        };

        EncodingTemplate<PlainEncoder, PlainDecoder> plainEncoding;

        EncodingTemplate<LengthEncoder, LengthDecoder> lengthEncoding;

        EncodingTemplate<BitpackEncoder, BitpackDecoder> bitpackEncoding;

        Encoding &EncodingFactory::Get(Encodings encoding) {
            switch (encoding) {
                case PLAIN:
                    return plainEncoding;
                case LENGTH:
                    return lengthEncoding;
                case BITPACK:
                    return bitpackEncoding;
                default:
                    return plainEncoding;
            }
        }
    }
}