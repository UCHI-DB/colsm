//
// Created by harper on 7/8/21.
//

#include "colsm/comparators.h"

#include <gtest/gtest.h>
#include <leveldb/slice.h>

TEST(Comparators, IntComparator) {
  auto comparator = colsm::intComparator();

  uint32_t a;
  uint32_t b;

  leveldb::Slice akey((const char*)&a, 4);
  leveldb::Slice bkey((const char*)&b, 4);

  a = 65;
  b = 23;
  ASSERT_TRUE(comparator->Compare(akey, bkey) > 0);

  a = 37;
  b = 48;
  ASSERT_TRUE(comparator->Compare(akey, bkey) < 0);

  a = 119;
  b = 119;
  ASSERT_TRUE(comparator->Compare(akey, bkey) == 0);

  a = 0xFFFFFFFF;
  b = 0x37423424;
  ASSERT_TRUE(comparator->Compare(akey, bkey) > 0);

  b = 0xFFFFFFFF;
  a = 0x37423424;
  ASSERT_TRUE(comparator->Compare(akey, bkey) < 0);

  a = 0xFFFFFFFF;
  b = 0xF7423424;
  ASSERT_TRUE(comparator->Compare(akey, bkey) > 0);

  b = 0xFFFFFFFF;
  a = 0xF7423424;
  ASSERT_TRUE(comparator->Compare(akey, bkey) < 0);

  a = 0xF7423424;
  b = 0xF7423424;
  ASSERT_TRUE(comparator->Compare(akey, bkey) == 0);
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}