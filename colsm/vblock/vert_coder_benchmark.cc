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

#include "colsm/vblock/vert_coder.h"

using namespace std;
using namespace leveldb;
using namespace colsm;
using namespace colsm::encoding;

void prepareRLEData(std::vector<uint8_t>& buffer) {
  for (int i = 0; i < 100000; ++i) {
    buffer.push_back((i / 39) % 256);
  }
}

void RLE(benchmark::State& state) {
  std::vector<uint8_t> buffer;
  prepareRLEData(buffer);

  auto encoder = u8::EncodingFactory::Get(RUNLENGTH).encoder();

  //  encoder
  for (auto& item : buffer) {
    encoder->Encode(item);
  }
  encoder->Close();
  auto size = encoder->EstimateSize();

  uint8_t* byte_buffer = new uint8_t[size];
  encoder->Dump(byte_buffer);

  for (auto _ : state) {
    auto decoder = u8::EncodingFactory::Get(RUNLENGTH).decoder();
    decoder->Attach(byte_buffer);
    for (int i = 0; i < buffer.size(); ++i)
      benchmark::DoNotOptimize(decoder->DecodeU8());
  }

  delete[] byte_buffer;
}

void RLEVar(benchmark::State& state) {
  std::vector<uint8_t> buffer;
  prepareRLEData(buffer);

  auto encoder = u8::EncodingFactory::Get(BITPACK).encoder();

  //  encoder
  for (auto& item : buffer) {
    encoder->Encode(item);
  }
  encoder->Close();
  auto size = encoder->EstimateSize();

  uint8_t* byte_buffer = new uint8_t[size];
  encoder->Dump(byte_buffer);

  for (auto _ : state) {
    auto decoder = u8::EncodingFactory::Get(RUNLENGTH).decoder();
    decoder->Attach(byte_buffer);
    for (int i = 0; i < buffer.size(); ++i)
      benchmark::DoNotOptimize(decoder->DecodeU8());
  }

  delete[] byte_buffer;
}

void prepareDeltaData(std::vector<uint64_t>& buffer) {
  for (int i = 0; i < 100000; ++i) {
    uint64_t value = i;
    if (value % 117 == 0) {
      value *= 1000;
    }
    buffer.push_back(value);
  }
}

void PLAIN_64(benchmark::State& state) {
  std::vector<uint64_t> buffer;
  prepareDeltaData(buffer);

  auto encoder = u64::EncodingFactory::Get(PLAIN).encoder();

  //  encoder
  for (auto& item : buffer) {
    encoder->Encode(item);
  }
  encoder->Close();
  auto size = encoder->EstimateSize();

  uint8_t* byte_buffer = new uint8_t[size];
  encoder->Dump(byte_buffer);

  for (auto _ : state) {
    auto decoder = u8::EncodingFactory::Get(PLAIN).decoder();
    decoder->Attach(byte_buffer);
    for (int i = 0; i < buffer.size(); ++i)
      benchmark::DoNotOptimize(decoder->DecodeU64());
  }

  delete[] byte_buffer;
}

void DELTA_64(benchmark::State& state) {
  std::vector<uint64_t> buffer;
  prepareDeltaData(buffer);

  auto encoder = u64::EncodingFactory::Get(DELTA).encoder();

  //  encoder
  for (auto& item : buffer) {
    encoder->Encode(item);
  }
  encoder->Close();
  auto size = encoder->EstimateSize();

  uint8_t* byte_buffer = new uint8_t[size];
  encoder->Dump(byte_buffer);

  for (auto _ : state) {
    auto decoder = u8::EncodingFactory::Get(DELTA).decoder();
    decoder->Attach(byte_buffer);
    for (int i = 0; i < buffer.size(); ++i)
      benchmark::DoNotOptimize(decoder->DecodeU64());
  }

  delete[] byte_buffer;
}

BENCHMARK(PLAIN_64);
BENCHMARK(DELTA_64);
//BENCHMARK(RLE);
//BENCHMARK(RLEVar);
