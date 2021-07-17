//
// Created by harper on 7/16/21.
//

#include "respool.h"

#include <gtest/gtest.h>

class ResPoolObj {
 public:
    int value_;
  ResPoolObj() {
      static int counter = 0;
      counter++;
      value_ = counter;
  }

  virtual ~ResPoolObj() {}
};

TEST(ResPool, Get) {
  colsm::ResPool<ResPoolObj> pool(10, []() { return new ResPoolObj(); });

  auto val1 = pool.Get();
    auto val2 = pool.Get();
    auto val3 = pool.Get();
    auto val4 = pool.Get();
    auto val5 = pool.Get();
    auto val6 = pool.Get();

    EXPECT_EQ(val1->value_,1);
    EXPECT_EQ(val2->value_,2);
    EXPECT_EQ(val3->value_,3);
    EXPECT_EQ(val4->value_,4);
    EXPECT_EQ(val5->value_,5);
    EXPECT_EQ(val6->value_,6);

    {
        auto val7 = pool.Get();
        EXPECT_EQ(val7->value_,7);
    }

    auto val8 = pool.Get();
    EXPECT_EQ(val8->value_,7);
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}