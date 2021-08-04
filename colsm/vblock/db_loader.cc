//
// Created by Harper on 7/27/21.
//
#include <colsm/comparators.h>
#include <fstream>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

void load(leveldb::DB* db) {
  std::ifstream log_file("/home/cc/insert_log",
                         std::ifstream::in | std::ifstream::binary);

  uint32_t key_length;
  std::string key_data;
  uint32_t value_length;
  std::string value_data;
  while (!log_file.eof()) {
    log_file.read((char*)&key_length, 4);
    key_data.resize(key_length);
    log_file.read(key_data.data(), key_length);

    log_file.read((char*)&value_length, 4);
    value_data.resize(value_length);
    log_file.read(value_data.data(), value_length);

    auto status = db->Put(leveldb::WriteOptions(), leveldb::Slice(key_data),
                          leveldb::Slice(value_data));
    if (!status.ok()) {
      std::cerr << "Error" << '\n';
    }
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

  load(db);

  delete db;
  delete intCompare;
  delete options->filter_policy;
  delete options;

  return 0;
}