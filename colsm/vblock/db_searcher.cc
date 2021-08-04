//
// Created by Harper on 7/27/21.
//
#include <colsm/comparators.h>
#include <fstream>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

void search(leveldb::DB* db) {
  uint32_t key_length;
  std::string key_data;
  std::string value;
  std::ifstream search_log("/home/cc/search_log",
                           std::ifstream::in | std::ifstream::binary);
  leveldb::Status s;
  int counter = 0;
  while (!search_log.eof()) {
    search_log.read((char*)&key_length, 4);
    key_data.resize(key_length);
    search_log.read(key_data.data(), key_length);

    s = db->Get(leveldb::ReadOptions(), key_data, &value);
    std::cout << (counter++) << '\n';
    std::cout << key_data.size() << '\n';
    std::cout << value.size() << std::endl;
  }
}

int main() {
  std::string folder_name = "/tmp/colsmtestdb2";

  leveldb::DB* db;

  leveldb::Options* options = new leveldb::Options();
  auto intCompare = colsm::intComparator().release();
  options->compression = leveldb::kNoCompression;
  options->comparator = intCompare;
  options->create_if_missing = true;
  options->block_size = 2 * 1024 * 1024;
  options->filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(*options, folder_name, &db);

  search(db);

  delete db;
  delete intCompare;
  delete options->filter_policy;
  delete options;

  return 0;
}