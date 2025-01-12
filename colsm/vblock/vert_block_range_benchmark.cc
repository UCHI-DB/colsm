//
// Created by Hao Jiang on 12/2/20.
//
#include <benchmark/benchmark.h>
#include <iostream>
#include <memory>

#include "leveldb/comparator.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"

#include "colsm/comparators.h"
#include "vert_block.h"
#include "vert_block_builder.h"

using namespace std;
using namespace leveldb;
using namespace colsm;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

class BlockRangeBenchmark : public benchmark::Fixture {
 protected:
  uint32_t num_entry_ = 2000000;

  Block* block_;
  VertBlockCore* vblock_;

  unique_ptr<Comparator> comparator_;

  char kbuffer_[12];

  char vbuffer_[128];
  int value_len_ = 128;

  vector<uint32_t> target;

 public:
  // add members as needed

  BlockRangeBenchmark() {
    srand(time(nullptr));
    for (int i = 0; i < 10000; ++i) {
      target.push_back(rand() % 1000000);
    }
    comparator_ = intComparator();
    num_entry_ = 1000000;
    {
      std::vector<int32_t> buffer;
      for (auto i = 0; i < num_entry_; ++i) {
        buffer.push_back(i);
      }
      std::sort(buffer.begin(), buffer.end(), binary_sorter);

      Options option;
      option.comparator = leveldb::BytewiseComparator();
      BlockBuilder builder((const Options*)&option);

      for (auto i = 0; i < num_entry_; ++i) {
        *((uint32_t*)kbuffer_) = buffer[i];
        Slice key(kbuffer_, 12);
        *((uint32_t*)vbuffer_) = buffer[i];
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
      }
      auto result = builder.Finish();
      char* copied = new char[result.size()];
      memcpy(copied, result.data(), result.size());
      Slice heap(copied, result.size());
      BlockContents content{heap, true, true};
      block_ = new Block(content);
    }
    {
      Options options;
      VertBlockBuilder builder(&options,LENGTH);

      for (uint32_t i = 0; i < num_entry_; ++i) {
        *((uint32_t*)kbuffer_) = i;
        Slice key(kbuffer_, 12);
        *((uint32_t*)vbuffer_) = i;
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
      }

      auto result = builder.Finish();
      char* copied = new char[result.size()];
      memcpy(copied, result.data(), result.size());
      Slice heap(copied, result.size());
      BlockContents content{heap, true, true};
      vblock_ = new VertBlockCore(content);
    }
  }

  virtual ~BlockRangeBenchmark() {
    delete block_;
    delete vblock_;
  }
};

BENCHMARK_F(BlockRangeBenchmark, Normal)(benchmark::State& state) {
  for (auto _ : state) {
    //    for (int i = 0; i < 10; ++i) {
    char at[12];
    for (int i = 0; i < 1000; ++i) {
      auto ite = block_->NewIterator(leveldb::BytewiseComparator());
      *((uint32_t*)at) = target[i];
      Slice t(at, 12);
      ite->Seek(t);
      for (int j = 0; j < 100; ++j) {
        benchmark::DoNotOptimize(ite->key());
        if (!ite->Valid()) {
          break;
        }
        ite->Next();
      }
      delete ite;
    }
  }
}

BENCHMARK_F(BlockRangeBenchmark, Vert)(benchmark::State& state) {
  for (auto _ : state) {
    char at[12];
    for (int i = 0; i < 1000; ++i) {
      auto ite = vblock_->NewIterator(NULL);
      *((uint32_t*)at) = target[i];
      Slice t(at, 12);
      ite->Seek(t);
      for (int j = 0; j < 100; ++j) {
        benchmark::DoNotOptimize(ite->key());
        if (!ite->Valid()) {
          break;
        }
        ite->Next();
      }
      delete ite;
    }
  }
}