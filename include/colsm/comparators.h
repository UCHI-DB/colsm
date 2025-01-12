//
// Created by harper on 4/19/21.
//

#ifndef LEVELDB_COMPARATORS_H
#define LEVELDB_COMPARATORS_H

#include <memory>

#include "leveldb/comparator.h"

namespace colsm {
std::unique_ptr<leveldb::Comparator> intComparator();
}

#endif  // LEVELDB_COMPARATORS_H
