//
// Created by Harper on 5/14/21.
//
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <sstream>

#include "colsm/comparators.h"

void write() {
  auto intCompare = colsm::intComparator();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  int intkey;
  leveldb::Slice key((const char*)&intkey, 4);

  leveldb::Status s;

  for (int i = 0; i < 100000; ++i) {
    intkey = i * 2;
    std::stringstream ss;
    ss << "value " << (i * 2);
    if (s.ok()) {
      s = db->Put(leveldb::WriteOptions(), key, leveldb::Slice(ss.str()));
    } else {
      std::cerr << "Write failure" << '\n';
      break;
    }
  }

  delete db;
  delete options.filter_policy;
}

void read() {
  auto intCompare = colsm::intComparator();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  int intkey;
  leveldb::Slice key((const char*)&intkey, 4);

  leveldb::Status s;

  for (int i = 128; i < 100000; ++i) {
    intkey = i;
    std::string value;
    s = db->Get(leveldb::ReadOptions(), key, &value);
    if (s.ok()) {
      std::cout << value << '\n';
    } else {
      std::cerr << "Error:" << i << '\n';
    }
  }

  delete db;
  delete options.filter_policy;
}

void insert(leveldb::DB* db, int key, std::string value) {
  auto skey = leveldb::Slice((const char*)&key, 4);
  leveldb::Status s = db->Put(leveldb::WriteOptions(), skey, value);
  if (!s.ok()) {
    std::cerr << "Error Insert " << key << " => " << value << '\n';
  }
}

void scan(leveldb::DB* db, int keystart, int limit) {
  auto key = leveldb::Slice((const char*)&keystart, 4);
  auto iterator = db->NewIterator(leveldb::ReadOptions());
  iterator->Seek(key);
  for (auto i = 0; i < limit; ++i) {
    if (iterator->Valid()) {
      std::cout << iterator->value().ToString() << '\n';
      iterator->Next();
    } else {
      break;
    }
  }
  delete iterator;
}

void runexample() {
  auto intCompare = colsm::intComparator();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  insert(db, 5, "a=10");
  insert(db, 10, "b=20");
  insert(db, 15, "c=30");
  scan(db, 5, 4);

  delete db;
  delete options.filter_policy;
}

int main() {
  runexample();
}
