//
// Created by Harper on 5/14/21.
//
#include <cassert>
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

  std::string key1;
  std::string key2;

  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
  if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

  delete options.filter_policy;
}