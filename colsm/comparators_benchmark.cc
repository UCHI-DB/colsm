//
// Created by harper on 7/9/21.
//

#include <benchmark/benchmark.h>
#include <cstdlib>
#include <leveldb/comparator.h>
#include <leveldb/slice.h>
#include <colsm/comparators.h>
#include <vector>

void prepare(std::vector<uint32_t>& a, std::vector<uint32_t>& b) {
  srand(time(NULL));

  for (int i = 0; i < 1000000; ++i) {
    a.push_back(rand());
    b.push_back(rand());
  }
}

using namespace leveldb;
class TrivialIntComparator : leveldb::Comparator {
 public:
  const char* Name() const override { return "TrivialIntComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    auto auint = *((uint32_t*)a.data());
    auto buint = *((uint32_t*)b.data());
    if (auint > buint) return 1;
    if (auint < buint) {
      return -1;
    } else {
      return 0;
    }
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

void TrivialCompare(benchmark::State& state) {
  auto comparator = new TrivialIntComparator();
  std::vector<uint32_t> a;
  std::vector<uint32_t> b;
  int akey;
  int bkey;
  Slice as((const char*)&akey, 4);
  Slice bs((const char*)&bkey, 4);
  prepare(a, b);
  for (auto _ : state) {
    for (int i = 0; i < 1000000; ++i) {
      akey = a[i];
      bkey = b[i];
      benchmark::DoNotOptimize(comparator->Compare(as, bs));
    }
  }
}

void IntCompare(benchmark::State& state) {
  auto comparator = colsm::intComparator();
  std::vector<uint32_t> a;
  std::vector<uint32_t> b;
  int akey;
  int bkey;
  Slice as((const char*)&akey, 4);
  Slice bs((const char*)&bkey, 4);
  prepare(a, b);
  for (auto _ : state) {
    for (int i = 0; i < 1000000; ++i) {
      akey = a[i];
      bkey = b[i];
      benchmark::DoNotOptimize(comparator->Compare(as, bs));
    }
  }
}

BENCHMARK(TrivialCompare);
BENCHMARK(IntCompare);