//
// Created by harper on 7/13/21.
//
#include <benchmark/benchmark.h>
#include <byteutils.h>
#include <cstring>
#include <vector>
using namespace std;

int eq_packed(const uint8_t* data, uint32_t num_entry, uint8_t bitwidth,
              uint32_t target) {
  uint32_t mask = (1 << bitwidth) - 1;
  uint32_t begin = 0;
  uint32_t end = num_entry - 1;
  while (begin <= end) {
    auto current = (begin + end + 1) / 2;

    auto bits = current * bitwidth;
    auto index = bits >> 3;
    auto offset = bits & 0x7;

    auto extracted = (*(uint32_t*)(data + index) >> offset) & mask;

    if (extracted == target) {
      return current;
    }
    if (extracted > target) {
      end = current - 1;
    } else {
      begin = current + 1;
    }
  }
  return -1;
}

template<int X>
void BinarySearch_X(benchmark::State& state) {
  vector<uint32_t> keys;
  for (int i = 0; i < 1000; ++i) {
    keys.push_back(i);
  }

  uint8_t output[4000];
  memset(output, 0, 4000);
  sboost::byteutils::bitpack(keys.data(), 1000, X, output);

  for (auto _ : state) {
    benchmark::DoNotOptimize(eq_packed(output, 1000, X, 881));
  }
}

const auto B12 = BinarySearch_X<15>;
const auto B20 = BinarySearch_X<25>;
const auto B30 = BinarySearch_X<31>;


BENCHMARK(B12);
BENCHMARK(B20);
BENCHMARK(B30);
