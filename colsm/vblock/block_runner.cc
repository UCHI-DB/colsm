//
// Created by Hao Jiang on 12/2/20.
//
#include <iostream>
#include <memory>
#include <vector>

#include "leveldb/comparator.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"

#include "colsm/comparators.h"
#include "sortmerge_iterator.h"
#include "vert_block.h"
#include "vert_block_builder.h"

using namespace std;
using namespace leveldb;
using namespace colsm;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

bool int_sorter(int a, int b) { return a - b; }

BlockContents prepareBlock(std::vector<int> &keys, int value_len) {
    sort(keys.begin(), keys.end(), binary_sorter);
    uint32_t num_entry = keys.size();
    uint32_t intkey;
    char value_buffer[value_len];
    Slice key((const char *) &intkey, 4);
    Slice value(value_buffer, value_len);

    Options option;
    option.comparator = leveldb::BytewiseComparator();
    BlockBuilder builder((const Options *) &option);

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

    auto content1 = prepareVBlock(key1, 64, Encodings::LENGTH);
    auto content2 = prepareVBlock(key2, 64, Encodings::LENGTH);

    VertBlockCore block1(content1);
    VertBlockCore block2(content2);

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
    auto result = builder.Finish();

    std::cout<< result.size() << endl;
    delete content1.data.data();
    delete content2.data.data();
}

void runHori() {
    vector<int32_t> key1;
    vector<int32_t> key2;
    for (int i = 0; i < 1000000; ++i) {
        key1.push_back(i * 2);
        key2.push_back(i * 2 + 1);
    }
    auto content1 = prepareBlock(key1, 64);
    auto content2 = prepareBlock(key2, 64);

    Block block1(content1);
    Block block2(content2);

    Options option;
    option.comparator = leveldb::BytewiseComparator();
    BlockBuilder builder((const Options *) &option);

    auto ite1 = block1.NewIterator(leveldb::BytewiseComparator());
    auto ite2 = block2.NewIterator(leveldb::BytewiseComparator());
    ite1->SeekToFirst();
    ite2->SeekToFirst();
    auto ite = leveldb::vert::sortMergeIterator(leveldb::BytewiseComparator(), ite1, ite2);

    while (ite->Valid()) {
        builder.Add(ite->key(), ite->value());
        ite->Next();
    }

    auto finished = builder.Finish();
    std::cout<< finished.size() << endl;

    delete content1.data.data();
    delete content2.data.data();
}

int main() {
    runHori();
    runVert();
}
