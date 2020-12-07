// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};

class VertBlockMeta {
 private:
  uint32_t num_section_;
  uint64_t* section_offsets_;
  int32_t start_min_;
  uint8_t start_bitwidth_;
  uint8_t* starts_;

 public:
  VertBlockMeta();
  virtual ~VertBlockMeta();

  /**
   * Read the metadata from the given buffer location.
   * @return the bytes read
   */
  uint32_t Read(const char*);
  /**
   * Write metadata to the buffer
   * @return the bytes written
   */
  uint32_t Write(char*);
};

class VertBlock {
 public:
  // Initialize the block with the specified contents.
  explicit VertBlock(const BlockContents&);

  VertBlock(const VertBlock&) = delete;
  VertBlock& operator=(const VertBlock&) = delete;

  ~VertBlock();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class VIter;

  const char* data_;

  const char* key_data_;
  const char* value_data_;

  size_t size_;
  bool owned_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
