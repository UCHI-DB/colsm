//
// Created by harper on 4/18/21.
//

#include <gtest/gtest.h>
#include <vector>
#include "leveldb/iterator.h"
#include "leveldb/slice.h"
#include "sortmerge_iterator.h"

using namespace std;
using namespace leveldb;

class VectorIterator : public Iterator {
protected:
    vector<pair<Slice, Slice>> &content_;
    uint32_t pointer_ = -1;
    Status status_;
public:

    VectorIterator(vector<pair<Slice, Slice>> content) : content_(content) {}

    bool Valid() const override {
        return pointer_ >= 0 && pointer_ < content_.size();
    };

    void SeekToFirst() override {
        pointer_ = 0;
    }

    void SeekToLast() override {
        pointer_ = content_.size() - 1;
    }

    void Seek(const Slice &target) override {
        status_ = Status::NotSupported(Slice("Seek not supported"));
    }

    void Next() override {
        pointer_++;
    }

    void Prev() override {
        pointer_--;
    }

    Slice key() const override {
        return content_[pointer_].first;
    }

    Slice value() const override {
        return content_[pointer_].second;
    }

    Status status() const override {
        return status_;
    }
};


using namespace leveldb;

TEST(TwoMergeIterator, Next) {

}

// LevelDB test did not use gtest_main
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}