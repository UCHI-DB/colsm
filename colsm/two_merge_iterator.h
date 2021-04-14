//
// Created by harper on 4/13/21.
//

#ifndef LEVELDB_TWO_MERGE_ITERATOR_H
#define LEVELDB_TWO_MERGE_ITERATOR_H

#include <leveldb/iterator.h>

namespace leveldb {

    namespace vert {

        class TwoMergeIterator : public Iterator {

        public:
            TwoMergeIterator(Iterator *, Iterator *);
        };
    }
}


#endif //LEVELDB_TWO_MERGE_ITERATOR_H
