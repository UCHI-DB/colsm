//
// Created by harper on 12/9/20.
//
//
// VertBlockBuilder generate blocks that are columnar encoded
//
// A vertical block consists of two data columns, the keys and the values.
// As the entries are sorted by keys, we split the columns into blocks by key
// and bit-pack them. The data format is as following:
//
//    data:      sections {num_section}
//               meta_data
//               meta_size
//               MAGIC
//    metadata:  num_section    : uint32_t
//               section_offsets: uint64_t{num_section}
//               start_min      : uint32_t
//               start_bitwidth : uint8_t
//               starts         : bit-packed uint32_t
//    section:   num_entry      : uint32_t
//               start_value    : uint32_t
//               key_offset     : uint32_t
//               key_encoding   : uint8_t
//               seq_offset     : uint32_t
//               seq_encoding   : uint8_t
//               type_offset    : uint32_t
//               type_encoding  : uint8_t
//               value_offset   : uint32_t
//               value_encoding : uint8_t
//               keys {num_entry}
//               seq {num_entry}
//               type {num_entry}
//               values {num_entry}
//
//
//  The value column can be encoded with any valid encoding that supports
//  fast skipping. For now we just use plain encoding

#ifndef LEVELDB_BLOCK_VERT_BUILDER_H
#define LEVELDB_BLOCK_VERT_BUILDER_H

#include <cstdint>
#include <db/dbformat.h>
#include <vector>

#include "table/block_builder.h"

#include "vert_block.h"

using namespace leveldb;
namespace colsm {

class VertSectionBuilder {
 private:
  uint32_t num_entry_;
  uint32_t start_value_;
  EncodingType value_enc_type_;

  std::shared_ptr<Encoder> key_encoder_;
  std::shared_ptr<Encoder> seq_encoder_;
  std::shared_ptr<Encoder> type_encoder_;
  std::shared_ptr<Encoder> value_encoder_;

 public:
  VertSectionBuilder(EncodingType enc_type);

  virtual ~VertSectionBuilder() = default;

  void Open(uint32_t);

  uint32_t StartValue() const { return start_value_; }

  void Reset();

  void Add(ParsedInternalKey key, const Slice& value);

  uint32_t NumEntry() const { return num_entry_; }

  uint32_t EstimateSize() const;

  void Close();

  void Dump(uint8_t*);
};

/**
 * Builder for building vertical blocks
 */
class VertBlockBuilder : public BlockBuilder {
 public:
  EncodingType value_encoding_ = EncodingType::LENGTH;

  explicit VertBlockBuilder(const Options* options, EncodingType);

  VertBlockBuilder(const VertBlockBuilder&) = delete;

  VertBlockBuilder& operator=(const VertBlockBuilder&) = delete;

  virtual ~VertBlockBuilder() = default;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const;

 private:
  uint32_t section_limit_;

  VertBlockMeta meta_;
  VertSectionBuilder current_section_;

  uint64_t offset_;

  std::vector<uint8_t> buffer_;

  void DumpSection();
};

}  // namespace colsm
#endif  // LEVELDB_BLOCK_VERT_BUILDER_H
