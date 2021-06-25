//
// Created by harper on 12/5/20.
//

#include "vert_block.h"

#include <gtest/gtest.h>
#include <immintrin.h>

#include "table/block.h"

#include "byteutils.h"
#include "vert_block_builder.h"

using namespace leveldb;
using namespace colsm;

TEST(VertBlockMeta, Read) {
  uint8_t buffer[897];
  memset(buffer, 0, 897);

  // Fill in the buffer
  auto pointer = buffer;
  *(uint32_t*)pointer = 100;
  pointer += 4;
  for (int i = 0; i < 100; ++i) {
    *(uint64_t*)pointer = i;
    pointer += 8;
  }
  *(int32_t*)pointer = 377;
  pointer += 4;

  *(int8_t*)pointer = 7;
  pointer += 1;

  VertBlockMeta meta;
  meta.Read(buffer);

  EXPECT_EQ(897, meta.EstimateSize());
}

TEST(VertBlockMeta, Write) {
  VertBlockMeta meta;
  for (int i = 0; i < 100; ++i) {
    meta.AddSection(2 * i, i + 300);
  }
  meta.Finish();
  // 9 + 100*8 + 700bit = 809 + 88
  EXPECT_EQ(897, meta.EstimateSize());

  uint8_t buffer[897];
  memset(buffer, 0, 897);
  meta.Write(buffer);

  auto pointer = buffer;
  EXPECT_EQ(100, *(uint32_t*)pointer);
  pointer += 4;
  for (auto i = 0; i < 100; ++i) {
    EXPECT_EQ(2 * i, *(uint64_t*)pointer);
    pointer += 8;
  }
  EXPECT_EQ(300, *(int32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(7, *(uint8_t*)pointer);
  pointer++;

  uint8_t pack_buffer[88];
  memset(pack_buffer, 0, 88);
  uint32_t plain[100];
  for (auto i = 0; i < 100; ++i) plain[i] = i;
  sboost::byteutils::bitpack(plain, 100, 7, pack_buffer);

  for (auto i = 0; i < 88; ++i) {
    EXPECT_EQ(pack_buffer[i], (uint8_t) * (pointer++)) << i;
  }
}

TEST(VertBlockMeta, Search) {
  uint8_t buffer[897];
  memset(buffer, 0, 897);

  // Fill in the buffer
  auto pointer = buffer;
  *(uint32_t*)pointer = 100;
  pointer += 4;
  for (int i = 0; i < 100; ++i) {
    *(uint64_t*)pointer = i * 50;
    pointer += 8;
  }
  *(int32_t*)pointer = 377;
  pointer += 4;

  *(int8_t*)pointer = 7;
  pointer += 1;

  uint32_t plain[100];
  for (auto i = 0; i < 100; ++i) {
    plain[i] = i;
  }
  sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

  VertBlockMeta meta;
  meta.Read(buffer);

  EXPECT_EQ(750, meta.Search(392));  // 15 in entrie
}

TEST(VertSection, Read) {
  uint8_t buffer[150];
  memset(buffer, 0, 150);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(int32_t*)pointer = 234;
  pointer += 4;

  *(uint32_t*)pointer = 201;
  pointer += 4;
  *(uint32_t*)pointer = 202;
  pointer += 4;
  *(uint32_t*)pointer = 204;
  pointer += 4;
  *(uint32_t*)pointer = 208;
  pointer += 4;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  EXPECT_EQ(234, section.StartValue());
  EXPECT_EQ(8, section.BitWidth());
  EXPECT_EQ((const uint8_t*)pointer, section.KeysData());
}

TEST(VertSection, Find) {
  uint8_t buffer[113];
  memset(buffer, 0, 113);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(int32_t*)pointer = 234;
  pointer += 4;
  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  // diff is 124, location is 62
  EXPECT_EQ(62, section.Find(358));

  EXPECT_EQ(-1, section.Find(329));
}

TEST(VertSection, FindStart) {
  uint8_t buffer[113];
  memset(buffer, 0, 113);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(int32_t*)pointer = 234;
  pointer += 4;
  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  // diff is 124, location is 62
  EXPECT_EQ(62, section.FindStart(358));
  // Next is 330, diff is 96, location is 48
  EXPECT_EQ(48, section.FindStart(329));
}

class VertBlockMetaForTest : public VertBlockMeta {
 public:
  VertBlockMetaForTest() : VertBlockMeta() {}

  std::vector<uint64_t>& Offset() { return offsets_; }

  uint8_t StartBitWidth() { return start_bitwidth_; }

  uint8_t* Starts() { return starts_; }
};

TEST(VertBlock, Next) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  int buffer = 0;
  Slice key((const char*)&buffer, 4);
  for (uint32_t i = 0; i < 1000000; ++i) {
    buffer = i;
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  auto ite = block.NewIterator(NULL);
  for (int i = 0; i < 1000000; ++i) {
    ite->Next();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(4, key.size()) << i;
    EXPECT_EQ(i, *((int32_t*)key.data())) << i;
    EXPECT_EQ(4, value.size()) << i;
    EXPECT_EQ(i, *((int32_t*)value.data())) << i;
  }
}

TEST(VertBlock, Seek) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  int buffer = 0;
  Slice write_key((const char*)&buffer, 4);
  for (uint32_t i = 0; i < 1000; ++i) {
    buffer = 2 * i;
    builder.Add(write_key, write_key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  {
    auto ite = block.NewIterator(NULL);
    int target_key = 10;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(4, key.size());
    EXPECT_EQ(10, *((int32_t*)key.data()));
    EXPECT_EQ(4, value.size());
    EXPECT_EQ(10, *((int32_t*)value.data()));
  }

  {
    auto ite = block.NewIterator(NULL);
    int target_key = 11;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());
  }
}

TEST(VertBlockTest, SeekThenNext) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  int buffer = 0;
  Slice write_key((const char*)&buffer, 4);
  for (uint32_t i = 0; i < 1000; ++i) {
    buffer = 2 * i;
    builder.Add(write_key, write_key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  {
    auto ite = block.NewIterator(NULL);
    int target_key = 10;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    int i = 0;
    while (ite->Valid()) {
      auto key = ite->key();
      auto value = ite->value();
      EXPECT_EQ(4, key.size());
      EXPECT_EQ((5 + i) * 2, *((int32_t*)key.data())) << i;
      EXPECT_EQ(4, value.size());
      EXPECT_EQ((5 + i) * 2, *((int32_t*)value.data())) << i;
      ite->Next();
      i++;
    }
    EXPECT_EQ(i, 995);
  }
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}