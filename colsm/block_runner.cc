//
// Created by Hao Jiang on 12/2/20.
//
#include <iostream>

#include "leveldb/comparator.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"

#include "vert_block.h"
#include "vert_block_builder.h"

using namespace leveldb;
using namespace leveldb::vert;

void runVert() {
  VertBlockBuilder vbb();

  uint32_t intkey;
  uint32_t intvalue;
  Slice key((const char*)&intkey, 4);
  Slice value((const char*)&intvalue, 4);

  VertBlockBuilder builder(NULL);

  for (uint32_t i = 0; i < 1000000; ++i) {
    intkey = i;
    intvalue = i;
    builder.Add(key, value);
  }

  auto result = builder.Finish();

  BlockContents content{result, true, true};
  VertBlock vertBlock(content);

  auto ite = vertBlock.NewIterator(NULL);
  int a = 39703;
  Slice target((const char*)&a, 4);
  ite->Seek(target);
  std::cout << *((const int32_t*)ite->key().data()) << std::endl;
  delete ite;
}
using namespace leveldb;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

void runNormal() {
  std::vector<int32_t> buffer;
  for (auto i = 0; i < 100000; ++i) {
    buffer.push_back(i);
  }
  std::sort(buffer.begin(), buffer.end(), binary_sorter);

  uint32_t intkey;
  uint32_t intvalue;

  Options option;
  option.comparator = leveldb::BytewiseComparator();
  BlockBuilder builder((const Options*)&option);

  for (auto i = 0; i < 100000; ++i) {
    intkey = buffer[i];
    intvalue = i;
    Slice key((const char*)&intkey, 4);
    Slice value((const char*)&intvalue, 4);
    builder.Add(key, value);
  }
  auto result = builder.Finish();
  char* copied = (char*)malloc(result.size());
  memcpy(copied, result.data(), result.size());
  Slice heap(copied, result.size());
  BlockContents content{heap, true, true};
  Block block(content);

  auto ite = block.NewIterator(option.comparator);

  int a = 39703;
  Slice target((const char*)&a, 4);
  ite->Seek(target);
  std::cout << *((const int32_t*)ite->key().data()) << std::endl;
  delete ite;
}

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

void runNormalWithIntKey() {
  uint32_t intkey;
  uint32_t intvalue;

  Options option;
  option.comparator = new IntComparator();
  BlockBuilder builder((const Options*)&option);

  for (uint32_t i = 0; i < 100000; ++i) {
    intkey = i;
    intvalue = i;
    Slice key((const char*)&intkey, 4);
    Slice value((const char*)&intvalue, 4);
    builder.Add(key, value);
  }
  auto result = builder.Finish();
  char* copied = (char*)malloc(result.size());
  memcpy(copied, result.data(), result.size());
  Slice heap(copied, result.size());
  BlockContents content{heap, true, true};
  Block block(content);

  auto ite = block.NewIterator(option.comparator);
  int a = 39703;
  Slice target((const char*)&a, 4);
  ite->Seek(target);
  std::cout << *((const int32_t*)ite->key().data()) << std::endl;
  delete ite;

  delete option.comparator;
}

int main() { runVert(); }
