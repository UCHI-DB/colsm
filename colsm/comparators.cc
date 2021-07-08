//
// Created by harper on 4/19/21.
//

#include "colsm/comparators.h"

#include "leveldb/slice.h"

using namespace std;
using namespace leveldb;

namespace colsm {
class IntComparator : public Comparator {
 public:
  const char* Name() const override { return "IntComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    auto auint = *((uint32_t*)a.data());
    auto buint = *((uint32_t*)b.data());
    auto msb = ((auint & 0x80000000) ^ (buint & 0x80000000)) >> 31;

    return (int)(((msb - 1) & (auint & 0x7FFFFFFF) - (buint & 0x7FFFFFFF)) |
                 ((-msb) & buint));
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

unique_ptr<Comparator> intComparator() {
  return unique_ptr<Comparator>(new IntComparator());
}
}  // namespace colsm