//
// Created by harper on 6/21/21.
//

#include "vert_block_builder.h"

#include <colsm/comparators.h>
#include <gtest/gtest.h>

using namespace colsm;

TEST(VertSectionBuilder, Write) {
  VertSectionBuilder section(EncodingType::PLAIN,3);
  char v[4];
  Slice value(v, 4);  // Dummy
  section.StartValue(3);
  for (auto i = 0; i < 100; ++i) {
    section.Add(i * 2 + 3, value);
  }
  auto size = section.EstimateSize();
  // 114 data, 800 value
  EXPECT_EQ(914, size);
  EXPECT_EQ(100, section.NumEntry());
  char buffer[size];
  memset(buffer, 0, size);

  section.Dump(buffer);

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

TEST(VertSectionBuilder, EstimateSize) {
  VertSectionBuilder section(EncodingType::PLAIN,0);
  std::string strvalue = "it is a good day";
  Slice value(strvalue.data(), strvalue.size());
  for (int i = 0; i < 1000; ++i) {
    section.Add(i, value);

    int bitwidth = 32 - _lzcnt_u32(i);
    int expected_value_size = (i + 1) * (4 + strvalue.size());
    int bitpack_size = (bitwidth * (i + 1) + 63) >> 6 << 3;

    EXPECT_EQ(10 + bitpack_size + expected_value_size, section.EstimateSize());
  }
}

TEST(VertBlockBuilder, Add) {
  Options options;
  auto comparator = intComparator();
  options.comparator = comparator.get();

  VertBlockBuilder builder(&options);
}

TEST(VertBlockBuilder, Build) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.encoding_ = EncodingType::LENGTH;

  int buffer = 0;
  Slice key((const char*)&buffer, 4);
  for (uint32_t i = 0; i < 1000; ++i) {
    buffer = i;
    builder.Add(key, key);
  }
  auto result = builder.Finish();
  // section_size = 128, 8 sections
  // meta = 9 + 9 * 8 + 4 * 9/8 = 86
  // section =
  EXPECT_EQ(9117, result.size());

  auto data = result.data();

  uint32_t mgc = *((uint32_t*)(result.data() + result.size() - 4));
  EXPECT_EQ(mgc, MAGIC);

  VertBlockMetaForTest meta;
  meta.Read(data);
  EXPECT_EQ(8, meta.NumSection());
  auto& offset = meta.Offset();
  EXPECT_EQ(8, offset.size());
  EXPECT_EQ(0, offset[0]);
  EXPECT_EQ(1154, offset[1]);
  EXPECT_EQ(1154 * 2, offset[2]);
  EXPECT_EQ(1154 * 3, offset[3]);
  EXPECT_EQ(1154 * 4, offset[4]);
  EXPECT_EQ(1154 * 5, offset[5]);
  EXPECT_EQ(1154 * 6, offset[6]);
  EXPECT_EQ(1154 * 7, offset[7]);

  EXPECT_EQ(10, meta.StartBitWidth());
}

TEST(VertBlockBuilder, Reset) {
  Options options;
  auto comparator = intComparator();
  options.comparator = comparator.get();

  VertBlockBuilder builder(&options);

  int intkey = 0;
  std::string strvalue = "value";
  Slice key((const char*)&intkey, 4);
  Slice value((const char*)strvalue.data(), strvalue.size());

  for (int repeat = 0; repeat < 5; ++repeat) {
    for (int i = 0; i < 100; ++i) {
      intkey = i;
      builder.Add(key, value);
    }
    builder.Finish();
    builder.Reset();
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}