//
// Created by Harper on 5/14/21.
//
#include <cassert>
#include <fstream>
#include <iostream>
#include "include/leveldb/db.h"
#include "include/leveldb/filter_policy.h"
#include <regex>
#include <sstream>
#include <stdlib.h>

#include "colsm/comparators.h"

void preparetest() {
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

void loadtest() {
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

void insert(leveldb::DB* db, int key, std::string& value) {
  auto skey = leveldb::Slice((const char*)&key, 4);
  leveldb::Status s = db->Put(leveldb::WriteOptions(), skey, value);
  if (!s.ok()) {
    std::cerr << "Error Insert " << key << " => " << value << '\n';
  }
}

bool read(leveldb::DB* db, int key) {
  auto bkey = leveldb::Slice((const char*)&key, 4);
  std::string value;
  auto s = db->Get(leveldb::ReadOptions(), bkey, &value);
  if (s.ok()) {
    std::cout << "Read:" << key << " => " << value << '\n';
    return true;
  }

  std::cout << "Read:" << key << " not found\n";
  return false;
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

void scanall(leveldb::DB* db) {
  auto iterator = db->NewIterator(leveldb::ReadOptions());
  iterator->SeekToFirst();
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    auto key = *((uint32_t*)iterator->key().data());
    std::cout << key << " ==> " << iterator->value().ToString() << '\n';
  }
  delete iterator;
}

int main() {
  auto intCompare = colsm::intComparator();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare.get();
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/colsmtestdb", &db);

  std::ifstream insertFile("/home/cc/insert.log");

  int targetkey = 1805947546;

//  std::string line;
//  while (std::getline(insertFile, line)) {
//    std::regex regexp("-?\\d+$");
//    std::smatch m;
//
//    // regex_search that searches pattern regexp in the string mystr
//    regex_search(line, m, regexp);
//    std::string value = m[0];
//    char* pend;
//    int insertkey = strtol((const char*)value.data(), &pend, 10);
//    insert(db, insertkey, value);
//  }
  auto s = read(db, targetkey);

  delete db;
  delete options.filter_policy;
}
