//
// Created by harper on 4/19/21.
//

#include "comparators.h"

#include "leveldb/slice.h"

using namespace std;
using namespace leveldb;

namespace colsm {
class IntComparator : public Comparator {
 public:
  const char* Name() const override { return "IntComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    if (b.size() < a.size()) {
      return 1;
    }
    int aint = *((int32_t*)a.data());
    int bint = *((int32_t*)b.data());
    return aint - bint;
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

unique_ptr<Comparator> intComparator() {
  return unique_ptr<Comparator>(new IntComparator());
}
}  // namespace colsm