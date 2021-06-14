//
// Created by harper on 12/9/20.
//

#include "vert_block_builder.h"

#include "table/format.h"

namespace colsm {

//
// VertBlockBuilder generate blocks that are columnar encoded
//
// A vertical block consists of two data columns, the keys and the values.
// As the entries are sorted by keys, we split the columns into blocks by key
// and bit-pack them. The data format is as following:
//
//    data:      metadata
//               sections {num_section}
//               MAGIC
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
    : BlockBuilder(options),
      section_size_(128),
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
    current_section_ = new VertSection(encoding_);
    current_section_->StartValue(intkey);
  }
  current_section_->Add(intkey, value);
  if (current_section_->NumEntry() >= section_size_) {
    DumpSection();
  }
}

void VertBlockBuilder::DumpSection() {
  if (current_section_ != NULL) {
    current_section_->Close();
    meta_.AddSection(offset_, current_section_->StartValue());
    offset_ += current_section_->EstimateSize();
    section_buffer_.push_back(current_section_);
    current_section_ = NULL;
  }
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

  meta_.Reset();
}

Slice VertBlockBuilder::Finish() {
  DumpSection();
  meta_.Finish();

  auto meta_size = meta_.EstimateSize();
  auto section_size = offset_;

  // Allocate Space
  // TODO In real world this should be directly write to file
  uint32_t buffer_size = meta_size + section_size + 4;
  internal_buffer_ = new char[buffer_size];
  memset(internal_buffer_, 0, meta_size + section_size);
  meta_.Write(internal_buffer_);
  auto pointer = internal_buffer_ + meta_size;

  for (auto& sec : section_buffer_) {
    sec->Dump(pointer);
    pointer += sec->EstimateSize();
  }

  // MAGIC
  *((uint32_t*)(internal_buffer_ + meta_size + section_size)) = MAGIC;

  return Slice(internal_buffer_, buffer_size);
}

size_t VertBlockBuilder::CurrentSizeEstimate() const {
  // sizes of meta
  auto meta_size = meta_.EstimateSize();
  auto section_size = offset_;
  if (current_section_ != 0) {
    // The size of each new section is upper-bounded by two 64-bits
    meta_size += 16;
    section_size += current_section_->EstimateSize();
  }
  // sizes of dumped sections

  return meta_size + section_size + 4;
}

bool VertBlockBuilder::empty() const {
  return section_buffer_.empty() &&
         (current_section_ == nullptr || current_section_->NumEntry() == 0);
}

}  // namespace colsm