// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <vector>

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
  // Section offsets
  std::vector<uint64_t> offsets_;
  int32_t start_min_;
  uint8_t start_bitwidth_;
  uint8_t* starts_;

  std::vector<uint32_t> starts_plain_;

  uint32_t BitPackSize() { return (start_bitwidth_ * num_section_ + 63) >> 6 << 3; }

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
  uint32_t num_entry_;
  int32_t start_value_;
  uint8_t bit_width_;
  uint32_t estimated_size_;

  const uint8_t* keys_data_;

  std::vector<uint32_t> keys_plain_;

  uint32_t BitPackSize() { return (bit_width_ * num_entry_ + 63) >> 6; }

 public:
  VertSection();
  virtual ~VertSection();

  uint32_t NumEntry() { return num_entry_; }
  uint8_t BitWidth() { return bit_width_; }
  int32_t StartValue() { return start_value_; }
  void StartValue(int32_t sv) { start_value_ = sv; }
  const uint8_t* KeysData() { return keys_data_; }

  void Add(int32_t key, const Slice& value);
  uint32_t EstimateSize();
  void Write(char*);

  void Read(const char*);

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
  explicit VertBlock(const BlockContents&);

  VertBlock(const VertBlock&) = delete;
  VertBlock& operator=(const VertBlock&) = delete;

  ~VertBlock();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class VIter;

  const char* data_;
  size_t size_;
  bool owned_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
