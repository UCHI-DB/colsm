// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <vector>
#include <memory>

#include "leveldb/iterator.h"

namespace leveldb {

    struct BlockContents;

    class Comparator;

    class BlockCore {
    public:
        virtual size_t size() const = 0;

        virtual Iterator *NewIterator(const Comparator *comparator) = 0;
    };


    class Block {
    private:
        std::unique_ptr<BlockCore> core_;
    public:
        // Initialize the block with the specified contents.
        explicit Block(const BlockContents &contents);

        Block(const Block &) = delete;

        Block &operator=(const Block &) = delete;

        ~Block();

        size_t size() const { return core_->size(); }

        Iterator *NewIterator(const Comparator *comparator) {
            return core_->NewIterator(comparator);
        }

    };

    class BasicBlockCore : public BlockCore {
    public:
        // Initialize the block with the specified contents.
        explicit BasicBlockCore(const BlockContents &contents);

        ~BasicBlockCore();

        size_t size() const override { return size_; }

        Iterator *NewIterator(const Comparator *comparator) override;

    private:
        class Iter;

        uint32_t NumRestarts() const;

        const char *data_;
        size_t size_;
        uint32_t restart_offset_;  // Offset in data_ of restart array
        bool owned_;               // Block owns data_[]
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
