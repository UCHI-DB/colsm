//
// Created by harper on 12/9/20.
//

#include "vert_block.h"
#include <immintrin.h>
#include "sboost.h"
#include "unpacker.h"
#include "byteutils.h"

namespace leveldb {
namespace vert {

int eq_packed(const uint8_t* data, uint32_t num_entry, uint8_t bitwidth,
              uint32_t target) {
  uint32_t mask = (1 << bitwidth) - 1;
  uint32_t begin = 0;
  uint32_t end = num_entry - 1;
  while (begin <= end) {
    auto current = (begin + end + 1) / 2;

    auto bits = current * bitwidth;
    auto index = bits >> 3;
    auto offset = bits & 0x7;

    auto extracted = (*(uint32_t*)(data + index) >> offset) & mask;

    if (extracted == target) {
      return current;
    }
    if (extracted > target) {
      end = current - 1;
    } else {
      begin = current + 1;
    }
  }
  return -1;
}

int section_packed(uint8_t* data, uint32_t num_entry, uint8_t bitwidth,
                   uint32_t target) {
  uint32_t mask = (1 << bitwidth) - 1;
  uint32_t begin = 0;
  uint32_t end = num_entry - 1;
  while (begin < end) {
    auto current = (begin + end + 1) / 2;

    auto bits = current * bitwidth;
    auto index = bits >> 3;
    auto offset = bits & 0x7;

    auto extracted = (*(uint32_t*)(data + index) >> offset) & mask;

    if (extracted <= target) {
      begin = current;
    } else {
      end = current - 1;
    }
  }
  return begin;
}

VertBlockMeta::VertBlockMeta()
    : num_section_(0), start_min_(0), start_bitwidth_(0), starts_(NULL) {}

VertBlockMeta::~VertBlockMeta() {}

void VertBlockMeta::AddSection(uint64_t offset, int32_t start_value) {
  num_section_++;
  if (starts_plain_.empty()) {
    start_min_ = start_value;
  }
  starts_plain_.push_back(start_value - start_min_);
  offsets_.push_back(offset);
}

uint64_t VertBlockMeta::Search(int32_t value) {
  //  std::cout << num_section_ << "," << (int32_t)start_bitwidth_ << std::endl;
  auto target = value - start_min_;
  //  sboost::SortedBitpack sbp(start_bitwidth_, target);
  //  auto index = sbp.greater(starts_, num_section_);
  //  return offsets_[index - 1];
  return offsets_[section_packed(starts_, num_section_, start_bitwidth_,
                                 target)];
}

uint32_t VertBlockMeta::Read(const char* in) {
  const char* pointer = in;
  num_section_ = *reinterpret_cast<const uint32_t*>(pointer);
  pointer += 4;
  offsets_.resize(num_section_);
  memcpy(offsets_.data(), pointer, num_section_ * 8);
  pointer += num_section_ * 8;
  start_min_ = *reinterpret_cast<const int32_t*>(pointer);
  pointer += 4;
  start_bitwidth_ = *(pointer++);

  starts_ = (uint8_t*)pointer;

  return pointer - in;
}

void VertBlockMeta::Finish() {
  start_bitwidth_ = 32 - _lzcnt_u32(starts_plain_[starts_plain_.size() - 1]);
}

uint32_t VertBlockMeta::EstimateSize() {
  return 9 + num_section_ * 8 + BitPackSize();
}

uint32_t VertBlockMeta::Write(char* out) {
  char* pointer = out;
  *reinterpret_cast<uint32_t*>(pointer) = num_section_;
  pointer += 4;
  memcpy(pointer, offsets_.data(), 8 * num_section_);
  pointer += 8 * num_section_;
  *reinterpret_cast<int32_t*>(pointer) = start_min_;
  pointer += 4;

  *reinterpret_cast<uint8_t*>(pointer++) = start_bitwidth_;
  sboost::byteutils::bitpack(starts_plain_.data(), num_section_,
                             start_bitwidth_, (uint8_t*)pointer);

  //  memcpy(pointer, starts_, (start_bitwidth_ * num_section_ + 7) >> 3);
}

VertSection::VertSection() : num_entry_(0), estimated_size_(0) {}

VertSection::~VertSection() {}

void VertSection::Add(int32_t key, const Slice& value) {
  num_entry_++;
  keys_plain_.push_back(key - start_value_);
  auto size = value.size();
  values_plain_.append((const char*)&size, 4);
  values_plain_.append(value.data(), value.size());
}

uint32_t VertSection::EstimateSize() {
  if (estimated_size_ != 0) {
    return estimated_size_;
  }
  num_entry_ = keys_plain_.size();
  bit_width_ = 32 - _lzcnt_u32(keys_plain_[num_entry_ - 1]);
  estimated_size_ = 9 + BitPackSize() + values_plain_.size();
  return estimated_size_;
}

void VertSection::Write(char* out) {
  auto pointer = out;
  *reinterpret_cast<uint32_t*>(pointer) = num_entry_;
  pointer += 4;
  *reinterpret_cast<int32_t*>(pointer) = start_value_;
  pointer += 4;
  *reinterpret_cast<uint8_t*>(pointer++) = bit_width_;
  sboost::byteutils::bitpack(keys_plain_.data(), keys_plain_.size(), bit_width_,
                             (uint8_t*)pointer);
  pointer += BitPackSize();
  memcpy(pointer, values_plain_.data(), values_plain_.size());
}

void VertSection::Read(const char* in) {
  auto pointer = in;
  num_entry_ = *reinterpret_cast<const uint32_t*>(pointer);
  pointer += 4;
  start_value_ = *reinterpret_cast<const int32_t*>(pointer);
  pointer += 4;
  bit_width_ = *reinterpret_cast<const uint8_t*>(pointer++);
  keys_data_ = reinterpret_cast<const uint8_t*>(pointer);
  pointer += BitPackSize();
  values_data_ = reinterpret_cast<const uint8_t*>(pointer);
  // Recompute estimated size
  // TODO Temporarily ignoring value size here as we do not store it
  estimated_size_ = 9 + BitPackSize();
}

int32_t VertSection::Find(int32_t target) {
  //  sboost::SortedBitpack sbp(bit_width_, target - start_value_);
  //  return sbp.equal(keys_data_, num_entry_);
  return eq_packed(keys_data_, num_entry_, bit_width_, target - start_value_);
}

int32_t VertSection::FindStart(int32_t target) {
  sboost::SortedBitpack sbp(bit_width_, target - start_value_);
  return sbp.geq(keys_data_, num_entry_);
}

VertBlock::VertBlock(const BlockContents& data)
    : data_(data.data.data()),
      size_(data.data.size()),
      owned_(data.heap_allocated) {}

VertBlock::~VertBlock() {
  if (owned_) {
    delete[] data_;
  }
}

class VertBlock::VIter : public Iterator {
 private:
  const Comparator* const comparator_;
  VertBlockMeta meta_;
  const char* data_pointer_;

  sboost::Unpacker* unpacker_;

  int intkey_;
  Slice key_;
  Slice value_;

  Status status_;

 public:
  VIter(const Comparator* comparator, const char* data)
      : comparator_(comparator), key_((const char*)&intkey_, 4) {
    meta_.Read(data);
    data_pointer_ = data + meta_.EstimateSize();
  }

  void Seek(const Slice& target) override {
    // Scan through blocks
    int32_t target_key = *reinterpret_cast<const int32_t*>(target.data());
    auto section_offset = meta_.Search(target_key);

    VertSection section;
    section.Read(data_pointer_ + section_offset);

    auto index = section.Find(target_key);
    if (index == -1) {
      // Not found
      status_ = Status::NotFound(target);
    } else {
      // Seek to the position, extract the keys and values
      // TODO Switch to a reader and maintain current unpacking location
      // Unpack 8 entries at a time, read bit_width_ bytes at a time
      unpacker_ = sboost::unpackers[section.BitWidth()];
      auto group_index = index >> 3;
      auto group_offset = index & 0x7;
      auto group_start = section.KeysData() + group_index * section.BitWidth();
      auto unpacked = unpacker_->unpack(group_start);
      auto entry =
          (unpacked[group_offset / 2] >> (32 * (group_offset & 1))) & -1;
      intkey_ = entry + section.StartValue();

      // Sequential look up value
      const char* value_pointer = (const char*)section.ValuesData();
      for (auto i = 0; i < index; ++i) {
        value_pointer += 4 + *(const uint32_t*)value_pointer;
      }
      value_ = Slice(value_pointer + 4, *(const uint32_t*)value_pointer);
    }
  }

  void SeekToFirst() override {}

  void SeekToLast() override {}

  void Next() override {}

  void Prev() override {}

  bool Valid() const override { return true; }

  Slice key() const override { return key_; }

  Slice value() const override { return value_; }

  Status status() const override {}
};

Iterator* VertBlock::NewIterator(const Comparator* comparator) {
  return new VIter(comparator, data_);
}

}
}