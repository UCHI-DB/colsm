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
#include "comparators.h"

using namespace std;
using namespace leveldb;
using namespace leveldb::vert;

bool binary_sorter(int a, int b) { return memcmp(&a, &b, 4) < 0; }

class BlockReadBenchmark : public benchmark::Fixture {
protected:
    uint32_t num_entry_ = 1000000;

    Block *block_;
    Block *blockWithInt_;
    VertBlock *vblock_;

    unique_ptr<Comparator> comparator_;

    Slice key_;
    char vbuffer_[16384];
    int value_len_ = 16384;

public:
    // add members as needed

    BlockReadBenchmark() {
        comparator_ = intComparator();
        num_entry_ = 1000000;
        {
            std::vector<int32_t> buffer;
            for (auto i = 0; i < num_entry_; ++i) {
                buffer.push_back(i);
            }
            std::sort(buffer.begin(), buffer.end(), binary_sorter);

            uint32_t intkey;

            Options option;
            option.comparator = leveldb::BytewiseComparator();
            BlockBuilder builder((const Options *) &option);

            for (auto i = 0; i < num_entry_; ++i) {
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
        {
            uint32_t intkey;

            Options option;
            option.comparator = comparator_.get();
            BlockBuilder builder((const Options *) &option);

            for (uint32_t i = 0; i < num_entry_; ++i) {
                intkey = i;
                Slice key((const char *) &intkey, 4);
                Slice value(vbuffer_, value_len_);
                builder.Add(key, value);
            }
            auto result = builder.Finish();
            char *copied = (char *) malloc(result.size());
            memcpy(copied, result.data(), result.size());
            Slice heap(copied, result.size());
            BlockContents content{heap, true, true};
            blockWithInt_ = new Block(content);
        }
        {
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

    virtual ~BlockReadBenchmark() {
        delete block_;
        delete blockWithInt_;
        delete vblock_;
    }
};

BENCHMARK_F(BlockReadBenchmark, Normal)(benchmark::State &state) {
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

BENCHMARK_F(BlockReadBenchmark, Vert)(benchmark::State &state) {
    auto ite = vblock_->NewIterator(NULL);

    for (auto _ : state) {
        int a = 39703;
        Slice target((const char *) &a, 4);
        ite->Seek(target);
        key_ = ite->key();
    }
    delete ite;
}
