//
// Created by Hao Jiang on 12/2/20.
//
#include <benchmark/benchmark.h>

#include "leveldb/comparator.h"

#include "block.h"
#include "block_builder.h"
#include "format.h"

using namespace leveldb;

class IntComparator : public Comparator {
 public:
  const char* Name() const override { return "IntComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    int aint = *((int32_t*)a.data());
    int bint = *((int32_t*)b.data());
    return aint - bint;
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

class BlockBenchmark : public benchmark::Fixture {
 protected:
  uint32_t num_entry_;

  Block* block_;
  VertBlock* vblock_;
  Options option;
 public:
  // add members as needed

  BlockBenchmark() {
    num_entry_ = 1000000;
    {
      uint32_t intkey;
      uint32_t intvalue;
      Slice key((const char*)&intkey, 4);
      Slice value((const char*)&intvalue, 4);

      option.comparator = new IntComparator();
      BlockBuilder builder((const Options*)&option);

      for (uint32_t i = 0; i < num_entry_; ++i) {
        intkey = i;
        intvalue = i;
        builder.Add(key, value);
      }
      auto result = builder.Finish();
      char* copied = (char*)malloc(result.size());
      memcpy(copied, result.data(), result.size());
      Slice heap(copied, result.size());
      BlockContents content{heap, true, true};
      block_ = new Block(content);
    }
    {
      VertBlockBuilder vbb();

      uint32_t intkey;
      uint32_t intvalue;
      Slice key((const char*)&intkey, 4);
      Slice value((const char*)&intvalue, 4);

      VertBlockBuilder builder(NULL);

      for (uint32_t i = 0; i < num_entry_; ++i) {
        intkey = i;
        intvalue = i;
        builder.Add(key, value);
      }

      auto result = builder.Finish();

      BlockContents content{result, true, true};
      vblock_ =  new VertBlock(content);
    }
  }

  virtual ~BlockBenchmark() {
    delete block_;
    delete vblock_;
    delete option.comparator;
  }
};

BENCHMARK_F(BlockBenchmark, Normal)(benchmark::State& state) {
  for (auto _ : state) {
    auto ite = block_->NewIterator(option.comparator);

    int a = 39703;
    Slice target((const char*)&a,4);
    ite->Seek(target);
    delete ite;
  }
}

BENCHMARK_F(BlockBenchmark, Vert)(benchmark::State& state) {
  for (auto _ : state) {
    auto ite = vblock_->NewIterator(NULL);

    int a = 39703;
    Slice target((const char*)&a,4);
    ite->Seek(target);
    delete ite;
  }
}
