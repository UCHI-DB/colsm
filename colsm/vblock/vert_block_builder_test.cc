//
// Created by harper on 6/21/21.
//

#include "vert_block_builder.h"

#include <byteutils.h>
#include <colsm/comparators.h>
#include <gtest/gtest.h>
#include <immintrin.h>

using namespace colsm;

TEST(VertSectionBuilder, Write) {
  VertSectionBuilder section(EncodingType::PLAIN, 3);
  int ik;
  Slice key((char*)&ik, 4);
  char v[4];
  Slice value(v, 4);  // Dummy
  for (auto i = 0; i < 100; ++i) {
    ik = i * 2 + 3;
    section.Add(ParsedInternalKey(key, 100, ValueType::kTypeValue), value);
  }
  section.Close();
  auto size = section.EstimateSize();
  // 137 data, 800 value, 800 seq, 4 type, 28 additional
  EXPECT_EQ(1769, size);
  EXPECT_EQ(100, section.NumEntry());
  uint8_t buffer[size];
  memset(buffer, 0, size);

  section.Dump(buffer);

  auto pointer = buffer;
  EXPECT_EQ(100, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(3, *(int32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(137, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(BITPACK, *(uint8_t*)pointer++);
  EXPECT_EQ(800, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(PLAIN, *(uint8_t*)pointer++);
  EXPECT_EQ(4, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(RUNLENGTH, *(uint8_t*)pointer++);
  EXPECT_EQ(800, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(PLAIN, *(uint8_t*)pointer++);
}

TEST(VertSectionBuilder, EstimateSize) {
  VertSectionBuilder section(EncodingType::PLAIN, 0);

  int ik;
  Slice key((char*)&ik, 4);
  std::string strvalue = "it is a good day";
  Slice value(strvalue.data(), strvalue.size());
  for (int i = 0; i < 1000; ++i) {
    ik = i;
    section.Add(ParsedInternalKey(key, 100, ValueType::kTypeValue), value);

    int bitwidth = 32 - _lzcnt_u32(i);
    int expected_value_size = (i + 1) * (4 + strvalue.size());
    int bitpack_size = 33 + ((i + 1 + 7) >> 3) * bitwidth;
    int seq_size = 8 * (i + 1);
    int type_size = 4;

    EXPECT_EQ(28 + bitpack_size + expected_value_size + seq_size + type_size,
              section.EstimateSize());
  }
}

class VertBlockMetaForTest : public VertBlockMeta {
 public:
  VertBlockMetaForTest() : VertBlockMeta() {}

  std::vector<uint64_t>& Offset() { return offsets_; }

  uint8_t StartBitWidth() { return start_bitwidth_; }

  uint8_t* Starts() { return starts_; }
};

TEST(VertBlockBuilder, Build) {
  Options option;
  VertBlockBuilder builder(&option);
  builder.value_encoding_ = EncodingType::LENGTH;

  char buffer[12];
  Slice key(buffer, 12);
  for (uint32_t i = 0; i < 1000; ++i) {
    *((uint32_t*)buffer) = i;
    *((uint64_t*)(buffer + 4)) = (100 << 8) + ValueType::kTypeValue;
    builder.Add(key, key);
  }
  auto result = builder.Finish();
  // section_size = 128, 8 sections
  // meta = 9 + 9 * 8 + 4 * 9/8 = 86
  // section =
  EXPECT_EQ(9117, result.size());

  uint8_t* data = (uint8_t*)result.data();

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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}