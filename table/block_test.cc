//
// Created by harper on 12/5/20.
//

#include "block.h"

#include <gtest/gtest.h>

#include "byteutils.h"

using namespace leveldb;

TEST(VertBlockMetaTest, Read) {
  char buffer[897];
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

TEST(VertBlockMetaTest, Write) {
  VertBlockMeta meta;
  for (int i = 0; i < 100; ++i) {
    meta.AddSection(2 * i, i + 300);
  }
  meta.Finish();
  // 9 + 100*8 + 700bit = 809 + 88
  EXPECT_EQ(897, meta.EstimateSize());

  char buffer[897];
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

TEST(VertBlockMetaTest, Search) {
  char buffer[897];
  memset(buffer, 0, 897);

  // Fill in the buffer
  auto pointer = buffer;
  *(uint32_t*)pointer = 100;
  pointer += 4;
  for (int i = 0; i < 100; ++i) {
    *(uint64_t*)pointer = i*50;
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

TEST(VertSectionTest, Write) {
  VertSection section;
  char v[4];
  Slice value(v,4);  // Dummy
  section.StartValue(3);
  for (auto i = 0; i < 100; ++i) {
    section.Add(i * 2 + 3, value);
  }
  auto size = section.EstimateSize();
  // 113 data, 800 value
  EXPECT_EQ(913, size);
  EXPECT_EQ(100,section.NumEntry());
  char buffer[size];
  memset(buffer, 0, size);

  section.Write(buffer);

  auto pointer = buffer;
  EXPECT_EQ(100, *(uint32_t*)pointer);
  pointer += 4;

  EXPECT_EQ(3, *(int32_t*)pointer);
  pointer += 4;

  EXPECT_EQ(8, *(uint8_t*)pointer);
  pointer++;

  uint32_t plain_buffer[100];
  uint8_t packed_buffer[104];
  memset(packed_buffer, 0, 104);
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, packed_buffer);

  for (auto i = 0; i < 104; ++i) {
    EXPECT_EQ(packed_buffer[i], (uint8_t)(*pointer++));
  }
}

TEST(VertSectionTest, Read) {
  char buffer[113];
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
  EXPECT_EQ(234, section.StartValue());
  EXPECT_EQ(113, section.EstimateSize());
  EXPECT_EQ(8, section.BitWidth());
  EXPECT_EQ((const uint8_t*)pointer, section.KeysData());
}

TEST(VertSectionTest, Find) {
  char buffer[113];
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
  EXPECT_EQ(62,section.Find(358));

  EXPECT_EQ(-1, section.Find(329));

}

TEST(VertSectionTest, FindStart) {
  char buffer[113];
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
  EXPECT_EQ(62,section.FindStart(358));
  // Next is 330, diff is 96, location is 48
  EXPECT_EQ(48, section.FindStart(329));
}

TEST(VertBlockTest, Seek) {
  // Scenario: two blocks, first has data 0-100
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}