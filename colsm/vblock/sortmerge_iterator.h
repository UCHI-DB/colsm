//
// Created by harper on 4/13/21.
//

#ifndef LEVELDB_SORTMERGE_ITERATOR_H
#define LEVELDB_SORTMERGE_ITERATOR_H

#include <leveldb/comparator.h>
#include <leveldb/iterator.h>

using namespace leveldb;
namespace colsm {

// Create a Sort-Merge iterator. Values from left will preempt the ones from
// right
Iterator* sortMergeIterator(const Comparator*, Iterator*, Iterator*);

}  // namespace colsm

#endif  // LEVELDB_SORTMERGE_ITERATOR_H
