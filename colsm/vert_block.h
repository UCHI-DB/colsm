//
// Created by harper on 12/9/20.
//

#ifndef LEVELDB_VERT_BLOCK_H
#define LEVELDB_VERT_BLOCK_H

#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

#include "table/format.h"
#include "vert_coder.h"

namespace leveldb {
    namespace vert {

        class VertBlockMeta {
        private:
            uint32_t num_section_;
            // Section offsets
            std::vector<uint64_t> offsets_;
            int32_t start_min_;
            uint8_t start_bitwidth_;
            uint8_t *starts_;

            std::vector<uint32_t> starts_plain_;

            uint32_t BitPackSize() {
                return (start_bitwidth_ * num_section_ + 63) >> 6 << 3;
            }

        public:
            VertBlockMeta();

            virtual ~VertBlockMeta();

            /**
             * Read the metadata from the given buffer location.
             * @return the bytes read
             */
            uint32_t Read(const char *);

            /**
             * Write metadata to the buffer
             * @return the bytes written
             */
            uint32_t Write(char *);

            uint32_t EstimateSize();

            /**
             * Add a section to the meta
             * @param offset offset in byte of the section
             * @param start_value start_value of the section
             */
            void AddSection(uint64_t offset, int32_t start_value);

            void Finish();

            /**
             * Search for the first section containing the value
             * @param value
             * @return index of the section
             */
            uint64_t Search(int32_t value);
        };

        class VertSection {
        private:
            // Basic Info
            uint32_t num_entry_;
            int32_t start_value_;
            uint8_t bit_width_;
            uint32_t estimated_size_;

            bool reading_;

            // Encoded Key Data
            const uint8_t *keys_data_;

            // Key Buffer
            std::vector<uint32_t> keys_plain_;

            Encoding* value_encoding_;
            Encoder *value_encoder_ = NULL;
            Decoder *value_decoder_ = NULL;

            uint32_t BitPackSize() { return (bit_width_ * num_entry_ + 63) >> 6 << 3; }

        public:
            VertSection();

            VertSection(const Encodings&);

            virtual ~VertSection();

            uint32_t NumEntry() { return num_entry_; }

            uint8_t BitWidth() { return bit_width_; }

            int32_t StartValue() { return start_value_; }

            void StartValue(int32_t sv) { start_value_ = sv; }

            const uint8_t *KeysData() { return keys_data_; }

            /**
             * Expose Decoder for iterator operations
             *
             * @return
             */
            Decoder *ValueDecoder() { return value_decoder_; }

            //
            // Functions for writer mode
            //
            void Add(int32_t key, const Slice &value);

            uint32_t EstimateSize();

            void Dump(char *);

            //
            // Functions for reader mode
            //
            void Read(const char *);

            /**
             * Find target in the section
             * @param target
             * @return
             */
            int32_t Find(int32_t target);

            /**
             * Find the first entry that is geq target
             * @param target
             * @return
             */
            int32_t FindStart(int32_t target);
        };


        class VertBlock {
        public:
            // Initialize the block with the specified contents.
            explicit VertBlock(const BlockContents &);

            VertBlock(const VertBlock &) = delete;

            VertBlock &operator=(const VertBlock &) = delete;

            ~VertBlock();

            size_t size() const { return size_; }

            Iterator *NewIterator(const Comparator *comparator);

        private:
            class VIter;

            const char *data_;
            size_t size_;
            bool owned_;
        };
    }  // namespace vert
}  // namespace leveldb

#endif  // LEVELDB_VERT_BLOCK_H
