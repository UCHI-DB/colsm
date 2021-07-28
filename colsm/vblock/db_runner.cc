//
// Created by Harper on 7/27/21.
//
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <colsm/comparators.h>
#include <iostream>

int main() {
  std::string folder_name = "/tmp/colsmtestdb";

  auto intCompare = colsm::intComparator().release();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare;
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, folder_name, &db);

  char keybuffer[4];
  char valuebuffer[2500];
  for(int i = 0 ; i < 1000000;++i) {
    *((uint32_t*)keybuffer) = i;
    auto status = db->Put(leveldb::WriteOptions(), leveldb::Slice(keybuffer,4),
                          leveldb::Slice(valuebuffer,128));
    if(!status.ok()) {
      std::cerr << "Error" << '\n';
      return 1;
    }
  }
delete db;
delete intCompare;
  return 0;
}