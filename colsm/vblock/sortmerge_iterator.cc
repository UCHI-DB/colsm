//
// Created by harper on 4/13/21.
//

#include "sortmerge_iterator.h"

#include <memory>

using namespace leveldb;

namespace colsm {

using namespace std;

class TwoMergeIterator : public Iterator {
 protected:
  Status status_;
  const Comparator* comparator_;
  unique_ptr<Iterator> left_;
  unique_ptr<Iterator> right_;
  bool fill_left_ = false;

  Iterator* pointer_;

  bool both_valid_ = true;

 public:
  TwoMergeIterator(const Comparator* comparator, Iterator* left,
                   Iterator* right)
      : comparator_(comparator) {
    left_ = unique_ptr<Iterator>(left);
    right_ = unique_ptr<Iterator>(right);

    pointer_ = right;
    left_->Next();
  }

  void Seek(const Slice& target) override {
    // Not supported
    status_ = Status::NotSupported(Slice("Seek Not Supported"));
  }

  void SeekToFirst() override {
    // Not supported
    status_ = Status::NotSupported(Slice("SeekToFirst Not Supported"));
  }

  void SeekToLast() override {
    // Not supported
    status_ = Status::NotSupported(Slice("SeekToLast Not Supported"));
  }

  void Next() override {
    pointer_->Next();

    if (both_valid_) {
      if (pointer_->Valid()) {
        auto compared = comparator_->Compare(left_->key(), right_->key());
        if (compared < 0) {
          pointer_ = left_.get();
        } else if (compared == 0) {
          pointer_ = left_.get();
          // In the case of equal, left preempts
          right_->Next();
        } else {
          pointer_ = right_.get();
        }
      } else {
        both_valid_ = false;
        pointer_ =
            (Iterator*)(((uint64_t)left_.get() + (uint64_t)right_.get()) -
                        (uint64_t)pointer_);
      }
    }
  }

  void Prev() override {
    status_ = Status::NotSupported(Slice("Prev Not Supported"));
  }

  bool Valid() const override { return left_->Valid() || right_->Valid(); }

  Slice key() const override { return pointer_->key(); }

  Slice value() const override { return pointer_->value(); }

  Status status() const override {
    if (left_->status().ok()) {
      return right_->status();
    } else {
      return left_->status();
    }
  }
};

Iterator* sortMergeIterator(const Comparator* comparator, Iterator* left,
                            Iterator* right) {
  return new TwoMergeIterator(comparator, left, right);
}
}  // namespace colsm