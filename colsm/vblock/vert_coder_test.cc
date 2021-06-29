//
// Created by harper on 6/22/21.
//

#include "vert_coder.h"

#include <cstdlib>
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

  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);
  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    ss.str(std::string());
    ss << "num" << current;
    auto expect = ss.str();
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->Decode();
      ASSERT_EQ(expect, result);
      current++;
    }
  }

  delete[] buffer;
}

TEST(StrLength, EncDec) {
  Encoding& plainEncoding = string::EncodingFactory::Get(LENGTH);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  std::stringstream ss;

  uint32_t size = 8;
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
  ASSERT_EQ(size, encoder->EstimateSize());
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
  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);
  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    ss.str(std::string());
    ss << "num" << current;
    auto expect = ss.str();
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->Decode();
      ASSERT_EQ(expect, result);
      current++;
    }
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

  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);
  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU64();
      ASSERT_EQ(current, result);
      current++;
    }
  }

  delete[] buffer;
}

uint8_t Var32Size(uint32_t value) {
  if(value == 0) {
    return 1;
  }
  auto bits = 32-_lzcnt_u32(value);
  return (bits + 6) / 7;
}

uint8_t Var64Size(uint64_t value) {
  if(value == 0) {
    return 1;
  }
  auto bits = 64-_lzcnt_u64(value);
  return (bits + 6) / 7;
}

uint64_t zigzagEncForTest(int64_t value) {
  return (value << 1) ^ (value >> 63);
}

TEST(U64Delta, EncDec) {
  Encoding& plainEncoding = u64::EncodingFactory::Get(DELTA);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  uint32_t size = 0;
  int64_t prev = 0;
  uint64_t rle_value = 0;
  uint32_t rle_counter = 0;
  for (int i = 0; i < 10000; ++i) {
    uint64_t value = i;
    if (value % 15 == 0) {
      value *= 100;
    }
    uint64_t delta = zigzagEncForTest((int64_t)value - prev);
    if (delta != rle_value) {
      if (rle_counter > 0) {
        size += Var32Size(rle_counter);
        size += Var64Size(rle_value);
      }
      rle_value = delta;
      rle_counter = 1;
    } else {
      rle_counter++;
    }

    encoder->Encode((uint64_t)value);
    ASSERT_EQ(size+((rle_counter!=0)?12:0), encoder->EstimateSize()) << i;

    prev = value;
  }
  encoder->Close();
  size = encoder->EstimateSize();
  uint8_t* buffer = new uint8_t[size];
  encoder->Dump(buffer);

  decoder->Attach(buffer);
  for (int i = 0; i < 10000; ++i) {
    uint64_t expect = i;
    if (expect % 15 == 0) {
      expect *= 100;
    }
    auto decoded = decoder->DecodeU64();
    ASSERT_EQ(expect, decoded);
  }

  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);
  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU64();
      uint64_t value = current;
      if(value %15 == 0) {
        value *= 100;
      }
      ASSERT_EQ(value, result);
      current++;
    }
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

  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);
  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU32();
      ASSERT_EQ(current, result);
      current++;
    }
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
    uint32_t size = 33 + bitwidth * ((i + 8) >> 3);
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

  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);

  //    decoder2->Skip(10);
  //    ASSERT_EQ(10,decoder2->DecodeU32());
  //    decoder2->Skip(30);
  //    ASSERT_EQ(41,decoder2->DecodeU32());

  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU32();
      ASSERT_EQ(current, result);
      current++;
    }
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
  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);

  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU8();
      ASSERT_EQ(current % 256, result);
      current++;
    }
  }
  delete[] buffer;
}

TEST(U8Rle, EncDec) {
  Encoding& plainEncoding = u8::EncodingFactory::Get(RUNLENGTH);
  auto encoder = plainEncoding.encoder();
  auto decoder = plainEncoding.decoder();

  for (int i = 0; i < 10000; ++i) {
    encoder->Encode((uint8_t)((i / 17) % 256));

    uint32_t size = ((i / 17) + 1) * 4;
    ASSERT_EQ(size, encoder->EstimateSize()) << i;
  }
  encoder->Close();
  ASSERT_EQ(((10000 / 17) + 1) * 4, encoder->EstimateSize());
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
  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);

  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU8();
      ASSERT_EQ((current / 17) % 256, result);
      current++;
    }
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
  ASSERT_EQ(size - 3, encoder->EstimateSize());
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
  srand(time(0));

  int current = 0;
  auto decoder2 = plainEncoding.decoder();
  decoder2->Attach(buffer);

  while (current < 10000) {
    uint32_t skip = rand() % 100;
    current += skip;
    if (current < 10000) {
      decoder2->Skip(skip);
      auto result = decoder2->DecodeU8();
      ASSERT_EQ((current / 17) % 256, result);
      current++;
    }
  }
  delete[] buffer;
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}