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
  *(uint32_t*)pointer = 377;
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

TEST(VertBlockMeta, SingleEntry) {
  VertBlockMeta meta;
  meta.AddSection(45, 300);
  meta.Finish();

  EXPECT_EQ(17, meta.EstimateSize());

  uint8_t buffer[17];
  memset(buffer, 0, 17);
  meta.Write(buffer);

  auto pointer = buffer;
  EXPECT_EQ(1, *(uint32_t*)pointer);
  pointer += 4;

  EXPECT_EQ(45, *(uint64_t*)pointer);
  pointer += 8;

  EXPECT_EQ(300, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(0, *(uint8_t*)pointer);

  VertBlockMeta readMeta;
  readMeta.Read(buffer);

  ASSERT_EQ(0, readMeta.Search(122));
  ASSERT_EQ(0, readMeta.Search(900));
}

TEST(VertBlockMeta, Search) {
  uint8_t buffer[897];

  {
    memset(buffer, 0, 897);
    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t*)pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
      *(uint64_t*)pointer = i * 50;
      pointer += 8;
    }
    *(uint32_t*)pointer = 377;
    pointer += 4;

    *(int8_t*)pointer = 7;
    pointer += 1;

    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) {
      plain[i] = i * 2 + 10;
    }
    sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(0, meta.Search(9));
    EXPECT_EQ(17, meta.Search(422));
    EXPECT_EQ(17, meta.Search(421));
    EXPECT_EQ(18, meta.Search(423));
    EXPECT_EQ(99, meta.Search(841));
  }
  // Test large uint numbers
  {
    memset(buffer, 0, 897);
    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t*)pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
      *(uint64_t*)pointer = i * 50;
      pointer += 8;
    }
    *(uint32_t*)pointer = 0xF0180179;
    pointer += 4;

    *(int8_t*)pointer = 7;
    pointer += 1;

    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) {
      plain[i] = i * 2 + 10;
    }
    sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(0, meta.Search(9));
    EXPECT_EQ(0, meta.Search(0xF0180009));
    EXPECT_EQ(17, meta.Search(0xF01801A6));
    EXPECT_EQ(17, meta.Search(0xF01801A5));
    EXPECT_EQ(18, meta.Search(0xF01801A7));
    EXPECT_EQ(99, meta.Search(0xF0180349));
  }
}

TEST(VertSection, Read) {
  uint8_t buffer[150];
  memset(buffer, 0, 150);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF051234;
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
  *(pointer++) = LENGTH;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  EXPECT_EQ(0xFF051234, section.StartValue());
  EXPECT_EQ(8, section.BitWidth());
  EXPECT_EQ((const uint8_t*)pointer, section.KeysData());
}

TEST(VertSection, Find) {
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF0500EA;
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
  *(pointer++) = LENGTH;

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
  EXPECT_EQ(62, section.Find(0xFF050166));
  EXPECT_EQ(-1, section.Find(0xFF050149));
  EXPECT_EQ(-1, section.Find(0xFF0501F4));
}

TEST(VertSection, FindStart) {
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF0500EA;
  pointer += 4;

  //  *(uint32_t*)pointer = 201;
  //  pointer += 4;
  //  *(pointer++) = BITPACK;
  //  *(uint32_t*)pointer = 202;
  //  pointer += 4;
  //  *(pointer++) = PLAIN;
  //  *(uint32_t*)pointer = 204;
  //  pointer += 4;
  //  *(pointer++) = RUNLENGTH;
  //  *(uint32_t*)pointer = 208;
  //  pointer += 4;
  //  *(pointer++) = LENGTH;

  *(pointer + 4) = BITPACK;
  pointer += 5;
  *(pointer + 4) = PLAIN;
  pointer += 5;
  *(pointer + 4) = RUNLENGTH;
  pointer += 5;
  *(pointer + 4) = LENGTH;
  pointer += 5;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  EXPECT_EQ(0, section.FindStart(0xFF0500B1));
  //   diff is 124, location is 62
  EXPECT_EQ(62, section.FindStart(0xFF050166));
  //   Next is 330, diff is 96, location is 48
  EXPECT_EQ(48, section.FindStart(0xFF050149));
  // Larger than end
  EXPECT_EQ(-1, section.FindStart(0xFF050900));
}

TEST(VertSection, SingleEntry) {
  VertSectionBuilder builder;
  builder.Open(111);
  int value = 111;
  builder.Add(
      ParsedInternalKey(Slice((char*)&value, 4), 1000, ValueType::kTypeValue),
      Slice("This is a value"));
  uint8_t buffer[100];
  memset(buffer,0,100);
  builder.Close();
  builder.Dump(buffer);

  VertSection section;
  section.Read(buffer);

  ASSERT_EQ(0,section.BitWidth());
  ASSERT_EQ(1,section.NumEntry());
  ASSERT_EQ(111,section.StartValue());

  ASSERT_EQ(0,section.KeyDecoder()->DecodeU32());
  ASSERT_EQ(1000,section.SeqDecoder()->DecodeU64());
  ASSERT_EQ(1,section.TypeDecoder()->DecodeU8());
}

TEST(VertSection, LargeBitWidthSearch) {
  FAIL() << "not implemented";
}

TEST(VertBlock, SeekToFirst) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((uint32_t*)buffer) = 0xFF000000 + i;
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

    ite->SeekToFirst();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(0xFF000000, *((uint32_t*)pkey.user_key.data()));
    EXPECT_EQ(4, pkey.user_key.size());
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size());
    EXPECT_EQ(0xFF000000, *((uint32_t*)value.data()));

    delete ite;
  }
}

TEST(VertBlock, SeekToLast) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 0xF0000000 + i;
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

    ite->SeekToLast();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(0xF00F423F, *((uint32_t*)pkey.user_key.data()));
    EXPECT_EQ(4, pkey.user_key.size());
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size());
    EXPECT_EQ(0xF00F423F, *((uint32_t*)value.data()));

    delete ite;
  }
}

TEST(VertBlock, Next) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

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
  ite->SeekToFirst();
  for (int i = 0; i < 1000000; ++i) {
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
    ite->Next();
  }
  delete ite;
}

TEST(VertBlock, Seek) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 2 * i + 10;
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
    // Test exact match on first section
    auto ite = block.NewIterator(NULL);
    int target_key = 30;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(30, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(30, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test exact match on following section
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
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(390, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, within sections
    auto ite = block.NewIterator(NULL);
    int target_key = 79;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(80, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(80, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, smaller than all
    auto ite = block.NewIterator(NULL);
    int target_key = 5;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(10, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(10, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, larger than all
    auto ite = block.NewIterator(NULL);
    int target_key = 5000000;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());
    delete ite;
  }
  {
    // Test no match, between the first two sections
    auto ite = block.NewIterator(NULL);
    int target_key = 265;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(266, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(266, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, between the last two sections
    auto ite = block.NewIterator(NULL);
    int target_key = 1999881;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(1999882, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(1999882, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, after the last sections
    auto ite = block.NewIterator(NULL);
    int target_key = 3999879;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());

    delete ite;
  }
}

TEST(VertBlock, SeekThenNext) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

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
      ASSERT_EQ(1350, pkey.sequence);
      ASSERT_EQ(ValueType::kTypeValue, pkey.type);
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