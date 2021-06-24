//
// Created by harper on 6/22/21.
//

#include "vert_coder.h"

#include <gtest/gtest.h>
#include <immintrin.h>
#include <sstream>

using namespace colsm;
using namespace colsm::encoding;

TEST(StrPlain, EncDec) {
  Encoding& plainEncoding = string::EncodingFactory::Get(PLAIN);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  std::stringstream ss;

  uint32_t size = 0;
  for (int i = 0; i < 10000; ++i) {
    ss.str(std::string());
    ss << "num" << i;
    auto string = ss.str();
    encoder->Encode(Slice(string.data(), string.length()));
    size += 4;
    size += string.length();
    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  uint8_t* buffer = new uint8_t[size];
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    ss.str(std::string());
    ss << "num" << i;
    auto expect = ss.str();
    auto slice = decoder->Decode();
    ASSERT_EQ(expect.size(), slice.size()) << i;
    ASSERT_TRUE(strncmp(slice.data(), expect.data(), expect.size()) == 0) << i;
  }

  delete[] buffer;
}

TEST(StrLength, EncDec) {
  Encoding& plainEncoding = string::EncodingFactory::Get(LENGTH);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  std::stringstream ss;

  uint32_t size = 4;
  for (int i = 0; i < 10000; ++i) {
    ss.str(std::string());
    ss << "num" << i;
    auto string = ss.str();
    encoder->Encode(Slice(string.data(), string.length()));
    size += 4;
    size += string.length();
    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  uint8_t* buffer = new uint8_t[encoder->EstimateSize()];
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    ss.str(std::string());
    ss << "num" << i;
    auto expect = ss.str();
    auto slice = decoder->Decode();
    ASSERT_EQ(expect.size(), slice.size()) << i;
    ASSERT_TRUE(strncmp(slice.data(), expect.data(), expect.size()) == 0) << i;
  }

  delete[] buffer;
}

TEST(U64Plain, EncDec) {
  Encoding& plainEncoding = u64::EncodingFactory::Get(PLAIN);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  uint32_t size = 0;
  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint64_t)i);
    size += 8;
    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  uint8_t* buffer = new uint8_t[size];
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint64_t expect = i;
    auto decoded = decoder->DecodeU64();
    ASSERT_EQ(expect, decoded);
  }

  delete[] buffer;
}

TEST(U32Plain, EncDec) {
  Encoding& plainEncoding = u32::EncodingFactory::Get(PLAIN);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  uint32_t size = 0;
  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint32_t)i);
    size += 4;
    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  uint8_t* buffer = new uint8_t[size];
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint32_t expect = i;
    auto decoded = decoder->DecodeU32();
    ASSERT_EQ(expect, decoded);
  }

  delete[] buffer;
}

TEST(U32Bitpack, EncDec) {
  Encoding& plainEncoding = u32::EncodingFactory::Get(BITPACK);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint32_t)i);

    uint32_t bitwidth = 32 - _lzcnt_u32(i);
    uint32_t size = 1 + ((bitwidth * (i + 1) + 63) >> 6 << 3);

    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  auto size = encoder->EstimateSize();
  uint8_t* buffer = new uint8_t[size];
  memset(buffer, 0, size);
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint32_t expect = i;
    auto decoded = decoder->DecodeU32();
    ASSERT_EQ(expect, decoded) << i;
  }

  delete[] buffer;
}

TEST(U8Plain, EncDec) {
  Encoding& plainEncoding = u8::EncodingFactory::Get(PLAIN);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  uint32_t size = 0;
  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint8_t)(i % 256));
    size++;
    ASSERT_EQ(size, encoder->EstimateSize());
  }
  encoder->Close();
  size = encoder->EstimateSize();
  uint8_t* buffer = new uint8_t[size];
  memset(buffer, 0, size);
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint32_t expect = i % 256;
    auto decoded = decoder->DecodeU8();
    ASSERT_EQ(expect, decoded) << i;
  }

  delete[] buffer;
}

TEST(U8Rle, EncDec) {
  Encoding& plainEncoding = u8::EncodingFactory::Get(RUNLENGTH);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint8_t)((i / 17) % 256));

    uint32_t size = ((i / 17) + 1) * 5;
    ASSERT_EQ(size, encoder->EstimateSize()) << i;
  }
  encoder->Close();
  auto size = encoder->EstimateSize();
  uint8_t* buffer = new uint8_t[size];
  memset(buffer, 0, size);
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint8_t expect = (i / 17) % 256;
    auto decoded = decoder->DecodeU8();
    ASSERT_EQ(expect, decoded) << i;
  }

  delete[] buffer;
}

TEST(U8RleVar, EncDec) {
  Encoding& plainEncoding = u8::EncodingFactory::Get(BITPACK);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  uint32_t size = 5;
  uint8_t prev = 0xFF;
  for (int i = 0; i < 10000; ++i) {
    uint8_t next = (uint8_t)((i / 17) % 256);
    encoder->Encode(next);

    // Estimate size
    if (next != prev) {
      if (i != 0) {
        size += 2;
      }
      prev = next;
    }

    ASSERT_EQ(size, encoder->EstimateSize()) << i;
  }
  encoder->Close();
  size = encoder->EstimateSize();
  uint8_t* buffer = new uint8_t[size];
  memset(buffer, 0, size);
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint8_t expect = (i / 17) % 256;
    auto decoded = decoder->DecodeU8();
    ASSERT_EQ(expect, decoded) << i;
  }

  delete[] buffer;
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}