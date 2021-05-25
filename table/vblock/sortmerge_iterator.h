//
// Created by harper on 4/13/21.
//

#ifndef LEVELDB_SORTMERGE_ITERATOR_H
#define LEVELDB_SORTMERGE_ITERATOR_H

#include <leveldb/iterator.h>
#include <leveldb/comparator.h>

namespace leveldb {

    namespace vert {

        // Create a Sort-Merge iterator. Values from left will preempt the ones from right
        Iterator* sortMergeIterator(const Comparator* ,Iterator*, Iterator*);

    }
}


#endif //LEVELDB_SORTMERGE_ITERATOR_H
