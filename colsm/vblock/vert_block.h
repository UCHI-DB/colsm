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

#include "table/block.h"
#include "table/format.h"

#include "vert_coder.h"

using namespace leveldb;
namespace colsm {
using namespace encoding;

const uint32_t MAGIC = 0xCAAEDADE;

class VertBlockMeta {
 protected:
  uint32_t num_section_;
  // Section offsets
  std::vector<uint64_t> offsets_;
  int32_t start_min_;
  uint8_t start_bitwidth_;
  uint8_t* starts_;

  std::vector<uint32_t> starts_plain_;

  uint32_t BitPackSize() const {
    return (start_bitwidth_ * num_section_ + 63) >> 6 << 3;
  }

 public:
  VertBlockMeta();

  virtual ~VertBlockMeta();

  void Reset();

  uint32_t NumSection() const { return num_section_; }

  uint32_t SectionOffset(uint32_t);

  /**
   * Read the metadata from the given buffer location.
   * @return the bytes read
   */
  uint32_t Read(const uint8_t*);

  /**
   * Write metadata to the buffer
   * @return the bytes written
   */
  void Write(uint8_t*);

  uint32_t EstimateSize() const;

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

  // For fast lookup on key_data
  const uint8_t* key_data_;
  uint8_t bit_width_;

  std::unique_ptr<Decoder> key_decoder_;
  std::unique_ptr<Decoder> seq_decoder_;
  std::unique_ptr<Decoder> type_decoder_;
  std::unique_ptr<Decoder> value_decoder_;

 public:
  VertSection();

  virtual ~VertSection() = default;

  uint32_t NumEntry() const { return num_entry_; }

  uint8_t BitWidth() const { return bit_width_; }

  int32_t StartValue() const { return start_value_; }

  const uint8_t* KeysData() { return key_data_; }

  /**
   * Expose Decoder for iterator operations
   *
   * @return
   */
  Decoder* KeyDecoder() { return key_decoder_.get(); }
  Decoder* SeqDecoder() { return seq_decoder_.get(); }
  Decoder* TypeDecoder() { return type_decoder_.get(); }
  Decoder* ValueDecoder() { return value_decoder_.get(); }

  void Read(const uint8_t*);

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

class VertBlockCore : public BlockCore {
 public:
  // Initialize the block with the specified contents.
  explicit VertBlockCore(const BlockContents&);

  VertBlockCore(const VertBlockCore&) = delete;

  VertBlockCore& operator=(const VertBlockCore&) = delete;

  ~VertBlockCore();

  size_t size() const { return size_; }

  Iterator* NewIterator(const Comparator* comparator);

 private:
  class VIter;

  const uint8_t* raw_data_;
  size_t size_;
  bool owned_;

  VertBlockMeta meta_;
  const uint8_t* content_data_;
};
}  // namespace colsm

#endif  // LEVELDB_VERT_BLOCK_H
