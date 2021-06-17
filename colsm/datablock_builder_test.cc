//
// Created by harper on 6/14/21.
//

#include "colsm/datablock_builder.h"

#include <gtest/gtest.h>

#include "colsm/comparators.h"

using namespace colsm;
TEST(DataBlockBuilder, Create) {
  leveldb::Options options;
  auto intCompare = intComparator();
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  DataBlockBuilder builder(&options, true);

  int intkey;
  Slice key((const char*)&intkey, 4);
  Slice value((const char*)&intkey, 4);
  for (int i = 0; i < 10000; ++i) {
    intkey = i;
    builder.Add(key, value);
  }
  Slice result = builder.Finish();
  auto magic = *((uint32_t*)(result.data()+result.size()-4));
  EXPECT_EQ(magic,colsm::MAGIC);
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}