//
// Created by Hao Jiang on 12/2/20.
//
#include <iostream>
#include <vector>
#include <memory>

#include "leveldb/comparator.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"

#include "vert_block.h"
#include "vert_block_builder.h"
#include "comparators.h"
#include "sortmerge_iterator.h"

using namespace std;
using namespace leveldb;
using namespace leveldb::vert;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

bool int_sorter(int a, int b) { return a - b;}

BlockContents prepareVBlock(std::vector<int> &keys, int value_len, Encodings encoding) {
    auto comparator = leveldb::vert::intComparator();
    uint32_t num_entry = keys.size();
    uint32_t intkey;
    char value_buffer[value_len];
    Slice key((const char *) &intkey, 4);
    Slice value(value_buffer, value_len);

    Options option;
    option.comparator = comparator.get();
    VertBlockBuilder builder((const Options *) &option);
    builder.encoding_ = encoding;

    for (auto i = 0; i < num_entry; ++i) {
        intkey = keys[i];
        builder.Add(key, value);
    }
    auto result = builder.Finish();
    char *copied = (char *) malloc(result.size());
    memcpy(copied, result.data(), result.size());
    Slice heap(copied, result.size());
    return BlockContents{heap, true, false};
}

void runVert() {
    auto comparator = leveldb::vert::intComparator();
    vector<int32_t> key1;
    vector<int32_t> key2;
    for (int i = 0; i < 1000000; ++i) {
        key1.push_back(i * 2);
        key2.push_back(i * 2 + 1);
    }

    auto content1 = prepareVBlock(key1, 16, Encodings::LENGTH);
    auto content2 = prepareVBlock(key2, 16, Encodings::LENGTH);

    VertBlock block1(content1);
    VertBlock block2(content2);

    Options option;
    option.comparator = comparator.get();
    VertBlockBuilder builder((const Options *) &option);
    builder.encoding_ = Encodings::LENGTH;

    auto ite1 = block1.NewIterator(comparator.get());
    auto ite2 = block2.NewIterator(comparator.get());
    auto ite = leveldb::vert::sortMergeIterator(comparator.get(), ite1, ite2);
    while (ite->Valid()) {
        ite->Next();
        builder.Add(ite->key(), ite->value());
    }
    builder.Finish();

    delete content1.data.data();
    delete content2.data.data();
}

int main() {
    runVert();
}
