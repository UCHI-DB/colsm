//
// Created by Harper on 5/14/21.
//
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

#include "colsm/comparators.h"
int main() {
  auto intCompare = colsm::intComparator();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  int intkey;
  std::string strvalue = "demo value";

  leveldb::Slice key((const char*)&intkey, 4);
  leveldb::Slice value(strvalue.data(), strvalue.size());

  leveldb::Status s;

  for (int i = 0; i < 10000000; ++i) {
    intkey = i;
    if (s.ok()) {
      s = db->Put(leveldb::WriteOptions(), key, value);
    } else {
      std::cerr << "Write failure" << '\n';
      break;
    }
  }

  delete options.filter_policy;
}