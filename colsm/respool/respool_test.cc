//
// Created by harper on 7/16/21.
//

#include "respool.h"

#include <gtest/gtest.h>

class LockPoolObj {
 public:
    int value_;
  LockPoolObj() {
      static int counter = 0;
      counter++;
      value_ = counter;
  }

  virtual ~LockPoolObj() {}
};
class SimplePoolObj {
public:
    int value_;
    SimplePoolObj() {
        static int counter = 0;
        counter++;
        value_ = counter;
    }

    virtual ~SimplePoolObj() {}
};

class LockFreePoolObj {
public:
    int value_;
    LockFreePoolObj() {
        static int counter = 0;
        counter++;
        value_ = counter;
    }

    virtual ~LockFreePoolObj() {}
};
TEST(LockPool, Get) {
    colsm::lock_pool<LockPoolObj> pool(10, []() { return new LockPoolObj(); });

    auto val1 = pool.get();
    auto val2 = pool.get();
    auto val3 = pool.get();
    auto val4 = pool.get();
    auto val5 = pool.get();
    auto val6 = pool.get();

    EXPECT_EQ(val1->value_,10);
    EXPECT_EQ(val2->value_,9);
    EXPECT_EQ(val3->value_,8);
    EXPECT_EQ(val4->value_,7);
    EXPECT_EQ(val5->value_,6);
    EXPECT_EQ(val6->value_,5);

    {
        auto val7 = pool.get();
        EXPECT_EQ(val7->value_,4);
    }

    auto val8 = pool.get();
    EXPECT_EQ(val8->value_,4);
}

TEST(SimplePool, Get) {
    colsm::simple_pool<SimplePoolObj> pool(10, []() { return new SimplePoolObj(); });

    auto val1 = pool.get();
    auto val2 = pool.get();
    auto val3 = pool.get();
    auto val4 = pool.get();
    auto val5 = pool.get();
    auto val6 = pool.get();

    EXPECT_EQ(val1->value_,10);
    EXPECT_EQ(val2->value_,9);
    EXPECT_EQ(val3->value_,8);
    EXPECT_EQ(val4->value_,7);
    EXPECT_EQ(val5->value_,6);
    EXPECT_EQ(val6->value_,5);

    {
        auto val7 = pool.get();
        EXPECT_EQ(val7->value_,4);
    }

    auto val8 = pool.get();
    EXPECT_EQ(val8->value_,4);
}

TEST(LockFreePool, Get) {
  colsm::lockfree_pool<LockFreePoolObj> pool(10, []() { return new LockFreePoolObj(); });

  auto val1 = pool.get();
    auto val2 = pool.get();
    auto val3 = pool.get();
    auto val4 = pool.get();
    auto val5 = pool.get();
    auto val6 = pool.get();

    EXPECT_EQ(val1->value_,1);
    EXPECT_EQ(val2->value_,2);
    EXPECT_EQ(val3->value_,3);
    EXPECT_EQ(val4->value_,4);
    EXPECT_EQ(val5->value_,5);
    EXPECT_EQ(val6->value_,6);

    {
        auto val7 = pool.get();
        EXPECT_EQ(val7->value_,7);
    }

    auto val8 = pool.get();
    EXPECT_EQ(val8->value_,7);
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}