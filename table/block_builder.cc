// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>
#include <immintrin.h>

#include "leveldb/comparator.h"
#include "leveldb/options.h"

#include "util/coding.h"

#include "block.h"
#include "byteutils.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  if (options_->comparator->Compare(key, last_key_piece) <= 0) {
    return;
  }
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

//
// VertBlockBuilder generate blocks that are columnar encoded
//
// A vertical block consists of two data columns, the keys and the values.
// As the entries are sorted by keys, we split the columns into blocks by key
// and bit-pack them. The data format is as following:
//
//    data:      metadata
//               sections {num_section}
//    metadata:  num_section    : uint32_t
//               section_offsets: uint64_t{num_section}
//               start_min      : int32_t
//               start_bitwidth : uint8_t
//               starts         : bit-packed uint32_t
//    section:   num_entry      : uint32_t
//               bit_width      : uint8_t
//               keys {num_entry}
//               values {num_entry}
//
//  We temporarily put metadata in header. Later this should be put in footer
//  for writing big files.

//  The value column can be encoded with any valid encoding that supports
//  fast skipping. For now we just use plain encoding

VertBlockBuilder::VertBlockBuilder(const Options* options)
    : section_size_(32768),
      offset_(0),
      current_section_(NULL),
      internal_buffer_(NULL) {}

VertBlockBuilder::~VertBlockBuilder() {
  for (auto& section : section_buffer_) {
    delete section;
  }
  if (current_section_ != NULL) {
    delete current_section_;
  }
  if (internal_buffer_ == NULL) {
    delete internal_buffer_;
  }
}

// Assert the keys and values are both int32_t
void VertBlockBuilder::Add(const Slice& key, const Slice& value) {
  int32_t intkey = *reinterpret_cast<const int32_t*>(key.data());
  if (current_section_ == NULL) {
    current_section_ = new VertSection();
    current_section_->StartValue(intkey);
  }
  current_section_->Add(intkey, value);
  if (current_section_->NumEntry() >= section_size_) {
    DumpSection();
  }
}

void VertBlockBuilder::DumpSection() {
  meta_.AddSection(offset_, current_section_->StartValue());
  offset_ += current_section_->EstimateSize();
  current_section_ = NULL;
}

void VertBlockBuilder::Reset() {
  for (auto& sec : section_buffer_) {
    delete sec;
  }
  if (current_section_ != NULL) {
    delete current_section_;
    current_section_ = NULL;
  }
  offset_ = 0;
  // TODO Reset meta
}

Slice VertBlockBuilder::Finish() {
  DumpSection();
  meta_.Finish();

  auto meta_size = meta_.EstimateSize();
  auto section_size = offset_;

  // Allocate Space
  // TODO In real world this should be directly write to file
  internal_buffer_ = new char[meta_size + section_size];

  meta_.Write(internal_buffer_);
  auto pointer = internal_buffer_ + meta_size;

  for (auto& sec : section_buffer_) {
    sec->Write(pointer);
    pointer += sec->EstimateSize();
  }

  return Slice(internal_buffer_, meta_size + section_size);
}

size_t VertBlockBuilder::CurrentSizeEstimate() const {
  // Not support
  return 0;
}

bool VertBlockBuilder::empty() const {}

}  // namespace leveldb
