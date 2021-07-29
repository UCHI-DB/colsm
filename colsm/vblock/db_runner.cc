//
// Created by Harper on 7/27/21.
//
#include <colsm/comparators.h>
#include <fstream>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

int main() {
  std::string folder_name = "/tmp/colsmtestdb";

  auto intCompare = colsm::intComparator().release();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare;
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, folder_name, &db);

  std::ifstream log_file("/home/cc/workload_log",
                         std::ifstream::in | std::ifstream::binary);

  uint32_t key_length;
  std::string key_data;
  uint32_t value_length;
  std::string value_data;
  while (!log_file.eof()) {
    log_file.read((char*)&key_length,4) ;
    key_data.resize(key_length);
    log_file.read(key_data.data(), key_length);

    log_file.read((char*)&value_length,4);
    value_data.resize(value_length);
    log_file.read(value_data.data(), value_length);

    auto status = db->Put(leveldb::WriteOptions(), leveldb::Slice(key_data),
                          leveldb::Slice(value_data));
    if (!status.ok()) {
      std::cerr << "Error" << '\n';
      return 1;
    }
  }

  delete db;
  delete intCompare;
  delete options.filter_policy;
  return 0;
}