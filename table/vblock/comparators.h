//
// Created by harper on 4/19/21.
//

#ifndef LEVELDB_COMPARATORS_H
#define LEVELDB_COMPARATORS_H

#include <memory>
#include "leveldb/comparator.h"

namespace leveldb {
    namespace vert {
        std::unique_ptr<Comparator> intComparator();
    }
}

#endif //LEVELDB_COMPARATORS_H
