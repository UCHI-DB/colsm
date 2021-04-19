//
// Created by Hao Jiang on 12/2/20.
//
#include <benchmark/benchmark.h>
#include <iostream>

#include "leveldb/comparator.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "vert_block.h"
#include "vert_block_builder.h"

using namespace leveldb;
using namespace leveldb::vert;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

void prepareBlock(std::vector<int> keys, int value_len) {
    std::sort(keys.begin(), keys.end(), binary_sorter);

    uint32_t num_entry = keys.size();
    uint32_t intkey;
    char value_buffer[value_len];

    Options option;
    option.comparator = leveldb::BytewiseComparator();
    BlockBuilder builder((const Options *) &option);

    for (auto i = 0; i < num_entry; ++i) {
        intkey = buffer[i];
        Slice key((const char *) &intkey, 4);
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
    }
    auto result = builder.Finish();
    char *copied = (char *) malloc(result.size());
    memcpy(copied, result.data(), result.size());
    Slice heap(copied, result.size());
    BlockContents content{heap, true, true};
    block_ = new Block(content);
}

void prepareVBlock(uint32_t num_entry, int value_len) {
    VertBlockBuilder vbb();

    uint32_t intkey;
    Slice key((const char *) &intkey, 4);

    VertBlockBuilder builder(NULL);

    for (uint32_t i = 0; i < num_entry_; ++i) {
        intkey = i;
        Slice value(vbuffer_, value_len_);
        builder.Add(key, value);
    }

    auto result = builder.Finish();

    BlockContents content{result, true, true};
    vblock_ = new VertBlock(content);
}

}

virtual ~

BlockWriteBenchmark() {
    delete block_;
    delete blockWithInt_;
    delete vblock_;
    delete comparator_;
}

};

BENCHMARK_F(BlockWriteBenchmark, Normal)(benchmark::State &state) {
    for (auto _ : state) {
        //    for (int i = 0; i < 10; ++i) {
        auto ite = block_->NewIterator(leveldb::BytewiseComparator());

        int a = 29942;
        Slice target((const char *) &a, 4);
        ite->Seek(target);
        key_ = ite->key();
        //    std::cout << *((int32_t*)key_.data()) << std::endl;
        delete ite;
        //    }
    }
}

// BENCHMARK_F(BlockBenchmark, NormalWithIntKey)(benchmark::State& state) {
//  for (auto _ : state) {
//    auto ite = blockWithInt_->NewIterator(comparator_);
//
//    int a = 39703;
//    Slice target((const char*)&a,4);
//    ite->Seek(target);
//    key_ = ite->key();
//    delete ite;
//  }
//}

BENCHMARK_F(BlockWriteBenchmark, Vert)(benchmark::State &state) {
    auto ite = vblock_->NewIterator(NULL);

    for (auto _ : state) {
        int a = 39703;
        Slice target((const char *) &a, 4);
        ite->Seek(target);
        key_ = ite->key();
    }
    delete ite;
}
