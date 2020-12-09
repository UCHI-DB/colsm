//
// Created by Hao Jiang on 12/2/20.
//
#include <benchmark/benchmark.h>
#include <iostream>

#include "leveldb/comparator.h"

#include "block.h"
#include "block_builder.h"
#include "format.h"

using namespace leveldb;

class IntComparator : public Comparator {
 public:
  const char* Name() const override { return "IntComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    if (b.size() < a.size()) {
      return 1;
    }
    int aint = *((int32_t*)a.data());
    int bint = *((int32_t*)b.data());
    return aint - bint;
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

class BlockBenchmark : public benchmark::Fixture {
 protected:
  uint32_t num_entry_ = 1000000;

  Block* block_;
  Block* blockWithInt_;
  VertBlock* vblock_;

  IntComparator* comparator_;

  Slice key_;
  char vbuffer_[16384];
  int value_len_ = 16384;

 public:
  // add members as needed

  BlockBenchmark() {
    comparator_ = new IntComparator();
    num_entry_ = 1000000;
    {
      std::vector<int32_t> buffer;
      for (auto i = 0; i < num_entry_; ++i) {
        buffer.push_back(i);
      }
      std::sort(buffer.begin(), buffer.end(), binary_sorter);

      uint32_t intkey;

      Options option;
      option.comparator = leveldb::BytewiseComparator();
      BlockBuilder builder((const Options*)&option);

      for (auto i = 0; i < num_entry_; ++i) {
        intkey = buffer[i];
        Slice key((const char*)&intkey, 4);
        Slice value(vbuffer_, value_len_);
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
      uint32_t intkey;

      Options option;
      option.comparator = new IntComparator();
      BlockBuilder builder((const Options*)&option);

      for (uint32_t i = 0; i < num_entry_; ++i) {
        intkey = i;
        Slice key((const char*)&intkey, 4);
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
      }
      auto result = builder.Finish();
      char* copied = (char*)malloc(result.size());
      memcpy(copied, result.data(), result.size());
      Slice heap(copied, result.size());
      BlockContents content{heap, true, true};
      blockWithInt_ = new Block(content);
    }
    {
      VertBlockBuilder vbb();

      uint32_t intkey;
      Slice key((const char*)&intkey, 4);

      VertBlockBuilder builder(NULL);

      for (uint32_t i = 0; i < num_entry_; ++i) {
        intkey = i;
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
      }

      auto result = builder.Finish();

      BlockContents content{result, true, true};
      vblock_ = new VertBlock(content);
    }
  }

  virtual ~BlockBenchmark() {
    delete block_;
    delete blockWithInt_;
    delete vblock_;
    delete comparator_;
  }
};

BENCHMARK_F(BlockBenchmark, Normal)(benchmark::State& state) {
  for (auto _ : state) {
    //    for (int i = 0; i < 10; ++i) {
    auto ite = block_->NewIterator(leveldb::BytewiseComparator());

    int a = 29942;
    Slice target((const char*)&a, 4);
    ite->Seek(target);
    key_ = ite->key();
    //    std::cout << *((int32_t*)key_.data()) << std::endl;
    delete ite;
    //    }
  }
}

// BENCHMARK_F(BlockBenchmark, NormalWithIntKey)(benchmark::State& state) {
//  for (auto _ : state) {
//    auto ite = blockWithInt_->NewIterator(comparator_);
//
//    int a = 39703;
//    Slice target((const char*)&a,4);
//    ite->Seek(target);
//    key_ = ite->key();
//    delete ite;
//  }
//}

BENCHMARK_F(BlockBenchmark, Vert)(benchmark::State& state) {
  auto ite = vblock_->NewIterator(NULL);

  for (auto _ : state) {
    int a = 39703;
    Slice target((const char*)&a, 4);
    ite->Seek(target);
    key_ = ite->key();
  }
  delete ite;
}
