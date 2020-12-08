//
// Created by Hao Jiang on 12/2/20.
//
#include <iostream>
#include "block.h"
#include "block_builder.h"
#include "format.h"
#include "leveldb/comparator.h"

using namespace leveldb;

void runVert() {
  VertBlockBuilder vbb();

  uint32_t intkey;
  uint32_t intvalue;
  Slice key((const char*)&intkey, 4);
  Slice value((const char*)&intvalue, 4);

  VertBlockBuilder builder(NULL);

  for (uint32_t i = 0; i < 100000; ++i) {
    intkey = i;
    intvalue = i;
    builder.Add(key, value);
  }

  auto result = builder.Finish();

  BlockContents content{result, true, true};
  VertBlock vertBlock(content);

  auto ite = vertBlock.NewIterator(NULL);

  int a = 39703;
  Slice target((const char*)&a,4);
  ite->Seek(target);
  delete ite;
}
using namespace leveldb;

class IntComparator : public Comparator {
 public:
  const char* Name() const override {
    return "IntComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    if(b.size() < a.size()) {
      return 1;
    }
    int aint = *((int32_t*)a.data());
    int bint = *((int32_t*)b.data());
    return aint - bint;
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
  }

  void FindShortSuccessor(std::string* key) const override {
  }
};


void runNormal() {
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
    builder.Add(key,value);
  }
  auto result = builder.Finish();
  char* copied = (char*)malloc(result.size());
  memcpy(copied,result.data(),result.size());
  Slice heap(copied,result.size());
  BlockContents content{heap,true,true};
  Block block(content);

  auto ite = block.NewIterator(option.comparator);

  int a = 39703;
  Slice target((const char*)&a,4);
  ite->Seek(target);
  auto key = ite->key();
  delete ite;

  delete option.comparator;
}

int main() {
  runVert();
}
