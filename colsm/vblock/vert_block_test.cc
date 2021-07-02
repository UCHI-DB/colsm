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
    plain[i] = i*2+10;
  }
  sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

  VertBlockMeta meta;
  meta.Read(buffer);

  EXPECT_EQ(-1, meta.Search(9));  // 15 in entries
  EXPECT_EQ(17, meta.Search(422));  // 15 in entries
  EXPECT_EQ(17, meta.Search(421));  // 15 in entries
  EXPECT_EQ(18, meta.Search(423));  // 15 in entries
  EXPECT_EQ(99, meta.Search(841));
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
  *(pointer++) = BITPACK;
  *(uint32_t*)pointer = 202;
  pointer += 4;
  *(pointer++) = PLAIN;
  *(uint32_t*)pointer = 204;
  pointer += 4;
  *(pointer++) = RUNLENGTH;
  *(uint32_t*)pointer = 208;
  pointer += 4;
  *(pointer++) = PLAIN;

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
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(int32_t*)pointer = 234;
  pointer += 4;

  *(pointer + 4) = BITPACK;
  pointer += 20;

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
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(int32_t*)pointer = 234;
  pointer += 4;

  *(pointer + 4) = BITPACK;
  pointer += 20;

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

TEST(VertBlock, Next) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  auto ite = block.NewIterator(NULL);
  for (int i = 0; i < 1000000; ++i) {
    ite->Next();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size()) << i;
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(i, *((int32_t*)pkey.user_key.data())) << i;
    EXPECT_EQ(4, pkey.user_key.size()) << i;
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size()) << i;
    EXPECT_EQ(i, *((int32_t*)value.data())) << i;
  }
  delete ite;
}

TEST(VertBlock, Seek) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 2 * i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  {
    auto ite = block.NewIterator(NULL);
    int target_key = 10;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(10, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue,pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(10, *((int32_t*)value.data()));
    delete ite;
  }
  {
    auto ite = block.NewIterator(NULL);
    int target_key = 390;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(390, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue,pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(390, *((int32_t*)value.data()));
    delete ite;
  }
  {
    auto ite = block.NewIterator(NULL);
    int target_key = 11;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());

    delete ite;
  }
}

TEST(VertBlockTest, SeekThenNext) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 2 * i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);
  ParsedInternalKey pkey;

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
      ASSERT_EQ(12, key.size());
      ParseInternalKey(key, &pkey);
      ASSERT_EQ((5 + i) * 2, *((int32_t*)pkey.user_key.data())) << i;
      ASSERT_EQ(1350,pkey.sequence);
      ASSERT_EQ(ValueType::kTypeValue,pkey.type);
      ASSERT_EQ(12, value.size());
      ASSERT_EQ((5 + i) * 2, *((int32_t*)value.data())) << i;
      ite->Next();
      i++;
    }
    EXPECT_EQ(i, 999995);
    delete ite;
  }
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}