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
#include "vert_block.h"
#include "vert_block_builder.h"
#include "sortmerge_iterator.h"
#include "comparators.h"

using namespace std;
using namespace leveldb;
using namespace leveldb::vert;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

BlockContents prepareBlock(std::vector<int> &keys, int value_len) {
    std::sort(keys.begin(), keys.end(), binary_sorter);

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
    std::sort(keys.begin(), keys.end(), binary_sorter);
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

void BlockMergeWithNoOverlap(benchmark::State &state) {

    vector<int32_t> key1;
    vector<int32_t> key2;
    for(int i = 0 ; i < 1000000;++i) {
        key1.push_back(i*2);
        key2.push_back(i*2+1);
    }
    auto content1 = prepareBlock(key1, 16);
    auto content2 = prepareBlock(key2, 16);

    int counter = 0;
    for (auto _: state) {
        Block block1(content1);
        Block block2(content2);

        Options option;
        option.comparator = leveldb::BytewiseComparator();
        BlockBuilder builder((const Options *) &option);

        auto ite1 = block1.NewIterator(leveldb::BytewiseComparator());
        auto ite2 = block2.NewIterator(leveldb::BytewiseComparator());
        auto ite = leveldb::vert::sortMergeIterator(leveldb::BytewiseComparator(),ite1,ite2);
        while(ite->Valid()) {
            ite->Next();
            builder.Add(ite->key(),ite->value());
        }
        auto result = builder.Finish();
        counter += result.size();
    }

    delete content1.data.data();
    delete content2.data.data();
}

void VBlockMergeWithNoOverlap(benchmark::State &state) {
    vector<int32_t> key1;
    vector<int32_t> key2;
    for(int i = 0 ; i < 1000000;++i) {
        key1.push_back(i*2);
        key2.push_back(i*2+1);
    }

    auto content1 = prepareVBlock(key1, 16,Encodings::LENGTH);
    auto content2 = prepareVBlock(key2, 16, Encodings::LENGTH);

    for (auto _: state) {
        VertBlock block1(content1);
        VertBlock block2(content2);

        Options option;
        option.comparator = leveldb::BytewiseComparator();
       VertBlockBuilder builder((const Options *) &option);
       builder.encoding_ = Encodings::LENGTH;

        auto ite1 = block1.NewIterator(leveldb::BytewiseComparator());
        auto ite2 = block2.NewIterator(leveldb::BytewiseComparator());
        auto ite = leveldb::vert::sortMergeIterator(leveldb::BytewiseComparator(),ite1,ite2);
        while(ite->Valid()) {
            ite->Next();
            builder.Add(ite->key(),ite->value());
        }
        builder.Finish();
    }

    delete content1.data.data();
    delete content2.data.data();
}

BENCHMARK(BlockMergeWithNoOverlap);
BENCHMARK(VBlockMergeWithNoOverlap);
