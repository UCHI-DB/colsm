//
// Created by harper on 12/9/20.
//

#include "vert_block_builder.h"

#include "db/dbformat.h"

#include "table/format.h"

namespace colsm {

VertSectionBuilder::VertSectionBuilder(EncodingType enc_type,
                                       int32_t start_value)
    : num_entry_(0), value_enc_type_(enc_type), start_value_(start_value) {
  key_encoder_ = u32::EncodingFactory::Get(BITPACK).encoder();
  seq_encoder_ = u64::EncodingFactory::Get(PLAIN).encoder();
  type_encoder_ = u8::EncodingFactory::Get(RUNLENGTH).encoder();

  Encoding& encoding = string::EncodingFactory::Get(enc_type);
  value_encoder_ = encoding.encoder();
}

void VertSectionBuilder::Add(ParsedInternalKey key, const Slice& value) {
  num_entry_++;

  key_encoder_->Encode(
      (uint32_t)(*(int32_t*)(key.user_key.data()) - start_value_));
  seq_encoder_->Encode(key.sequence);
  type_encoder_->Encode((uint8_t)key.type);
  value_encoder_->Encode(value);
}

uint32_t VertSectionBuilder::EstimateSize() {
  return 28 + key_encoder_->EstimateSize() + seq_encoder_->EstimateSize() +
         type_encoder_->EstimateSize() + value_encoder_->EstimateSize();
}

void VertSectionBuilder::Close() {
  key_encoder_->Close();
  seq_encoder_->Close();
  type_encoder_->Close();
  value_encoder_->Close();
}

void VertSectionBuilder::Dump(uint8_t* out) {
  uint8_t* pointer = (uint8_t*)out;
  *reinterpret_cast<uint32_t*>(pointer) = num_entry_;
  pointer += 4;
  *reinterpret_cast<int32_t*>(pointer) = start_value_;
  pointer += 4;

  auto key_size = key_encoder_->EstimateSize();
  auto seq_size = seq_encoder_->EstimateSize();
  auto type_size = type_encoder_->EstimateSize();
  auto value_size = value_encoder_->EstimateSize();

  *((uint32_t*)pointer) = key_size;
  pointer += 4;
  *(pointer++) = BITPACK;
  *((uint32_t*)pointer) = seq_size;
  pointer += 4;
  *(pointer++) = PLAIN;
  *((uint32_t*)pointer) = type_size;
  pointer += 4;
  *(pointer++) = RUNLENGTH;
  *((uint32_t*)pointer) = value_size;
  pointer += 4;
  *(pointer++) = value_enc_type_;

  key_encoder_->Dump(pointer);
  pointer += key_size;
  seq_encoder_->Dump(pointer);
  pointer += seq_size;
  type_encoder_->Dump(pointer);
  pointer += type_size;
  value_encoder_->Dump(pointer);
}

VertBlockBuilder::VertBlockBuilder(const Options* options)
    : BlockBuilder(options), section_limit_(128), offset_(0) {}

// Assert the keys and values are both int32_t
void VertBlockBuilder::Add(const Slice& key, const Slice& value) {
  // Currently we only handle interger keys
  assert(key.size()==12);
  // Need to handle the internal key
  ParsedInternalKey internal_key;
  ParseInternalKey(key, &internal_key);

  // write other parts of the internal key
  if (current_section_ == nullptr) {
    int32_t intkey =
        *reinterpret_cast<const int32_t*>(internal_key.user_key.data());
    current_section_ = std::unique_ptr<VertSectionBuilder>(
        new VertSectionBuilder(value_encoding_, intkey));
  }
  current_section_->Add(internal_key, value);
  if (current_section_->NumEntry() >= section_limit_) {
    DumpSection();
  }
}

void VertBlockBuilder::DumpSection() {
  current_section_->Close();
  meta_.AddSection(offset_, current_section_->StartValue());
  offset_ += current_section_->EstimateSize();

  section_buffer_.push_back(move(current_section_));

  current_section_ = nullptr;
}

void VertBlockBuilder::Reset() {
  section_buffer_.clear();
  current_section_ = NULL;
  offset_ = 0;
  buffer_.clear();
  meta_.Reset();
}

Slice VertBlockBuilder::Finish() {
  if (current_section_ != nullptr) {
    DumpSection();
  }
  meta_.Finish();

  auto meta_size = meta_.EstimateSize();
  auto section_size = offset_;

  // Allocate Space
  // TODO In real world this should be directly write to file
  uint32_t buffer_size = meta_size + section_size + 4;

  buffer_.resize(buffer_size);
  uint8_t* internal_buffer = buffer_.data();

  meta_.Write(internal_buffer);
  auto pointer = internal_buffer + meta_size;

  for (auto& sec : section_buffer_) {
    auto sec_size = sec->EstimateSize();
    sec->Dump(pointer);
    pointer += sec_size;
  }

  // MAGIC
  *((uint32_t*)(internal_buffer + meta_size + section_size)) = MAGIC;

  return Slice((const char*)internal_buffer, buffer_size);
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