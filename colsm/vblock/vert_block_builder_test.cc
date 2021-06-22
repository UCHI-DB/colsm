//
// Created by harper on 6/21/21.
//

#include "vert_block_builder.h"

#include <colsm/comparators.h>
#include <gtest/gtest.h>

using namespace colsm;

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