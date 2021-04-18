//
// Created by harper on 4/13/21.
//

#include <memory>
#include "two_merge_iterator.h"

namespace leveldb {
    namespace vert {

        using namespace std;
        class TwoMergeIterator : public Iterator {

        protected:
            unique_ptr<Iterator> left_;
            unique_ptr<Iterator> right_;
        public:
            TwoMergeIterator(Iterator * left, Iterator *right) {
               left_ = unique_ptr<Iterator>(left);
               right_ = unique_ptr<Iterator>(right);
            }

            void Seek(const Slice &target) override;

            void SeekToFirst() override;

            void SeekToLast() override;

            void Next() override;

            void Prev() override;

            bool Valid() const override;

            Slice key() const override;

            Slice value() const override;

            Status status() const override;
        };
    }
}