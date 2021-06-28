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

void prepareData(std::vector<uint8_t>& buffer) {
  for (int i = 0; i < 100000; ++i) {
    buffer.push_back((i / 39) % 256);
  }
}

void RLE(benchmark::State& state) {
  std::vector<uint8_t> buffer;
  prepareData(buffer);

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
  prepareData(buffer);

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

BENCHMARK(RLE);
BENCHMARK(RLEVar);
