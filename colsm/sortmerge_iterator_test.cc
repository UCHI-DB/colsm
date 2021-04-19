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

class IntIterator : public Iterator {
protected:
    vector <int32_t> &content_;
    uint32_t pointer_ = -1;
    Status status_;

    int32_t key_buffer_;
    Slice slice_;
public:

    IntIterator(vector <int32_t> content) : content_(content), slice_((const char *) &key_buffer_, 4) {}

    bool Valid() const override {
        return pointer_ >= 0 && pointer_ < content_.size();
    };

    void SeekToFirst() override {
        pointer_ = 0;
        key_buffer_ = content_[0];
    }

    void SeekToLast() override {
        pointer_ = content_.size() - 1;
        key_buffer_ = content_[pointer_];
    }

    void Seek(const Slice &target) override {
        status_ = Status::NotSupported(Slice("Seek not supported"));
    }

    void Next() override {
        pointer_++;
        if (pointer_ < content_.size())
            key_buffer_ = content_[pointer_];
    }

    void Prev() override {
        pointer_--;
        if (pointer_ >= 0)
            key_buffer_ = content_[pointer_];
    }

    Slice key() const override {
        return slice_;
    }

    Slice value() const override {
        return slice_;
    }

    Status status() const override {
        return status_;
    }
};


using namespace leveldb;

TEST(TwoMergeIterator, Next) {
    vector <int32_t> left_content{1, 3, 5, 7, 9, 10, 11};
    vector <int32_t> right_content{2, 4, 6, 8, 10, 12};
}

// LevelDB test did not use gtest_main
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}