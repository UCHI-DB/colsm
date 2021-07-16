//
// Created by harper on 7/16/21.
//

#include "respool.h"

#include <gtest/gtest.h>

class ResPoolObj {
 public:
  ResPoolObj() {}

  virtual ~ResPoolObj() {}
};

TEST(ResPool, Get) {
  colsm::ResPool<ResPoolObj> pool(10, []() { return new ResPoolObj(); });

  pool.Get();
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}