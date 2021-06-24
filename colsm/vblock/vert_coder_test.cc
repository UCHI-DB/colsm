//
// Created by harper on 6/22/21.
//

#include "vert_coder.h"

#include <gtest/gtest.h>
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

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}