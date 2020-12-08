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

TEST(VertSectionTest, Read) {}

TEST(VertSectionTest, Write) {}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}