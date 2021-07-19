//
// Created by harper on 7/18/21.
//
// When testing Snappy, please be aware that table_builder.cc will disable snappy
// if the compression ratio is below 12.5%. To get the accurate number need to
// comment line 178
//
#include "db/dbformat.h"
#include <colsm/comparators.h>
#include <iostream>
#include <leveldb/env.h>
#include <leveldb/table_builder.h>

using namespace leveldb;

uint64_t num_entry = 4000000;
bool binary_sorter(uint64_t a, uint64_t b) { return memcmp(&a, &b, 8) < 0; }

void rand_string(char* buffer, int length) {
  for (int i = 0; i < length; ++i) {
    buffer[i] = rand() % 256;
  }
}

void build_vest() {
  auto intComparator = colsm::intComparator();
  auto env = Env::Default();
  WritableFile* file;
  env->NewWritableFile("/tmp/vesttable", &file);
  Options options;
  options.comparator = intComparator.get();
  options.compression = kSnappyCompression;
  TableBuilder builder(options, true, file);

  srand(time(NULL));

  char key_buffer[12];
  char value_buffer[128];

  for (uint64_t i = 0; i < num_entry; ++i) {
    *((uint32_t*)(key_buffer)) = i;
    *((uint64_t*)(key_buffer + 4)) = i;
    key_buffer[11] = ValueType::kTypeValue;

    rand_string(value_buffer, 128);
    builder.Add(Slice(key_buffer, 12), Slice(value_buffer, 128));
  }
  builder.Finish();
  std::cout << "VEST Size:" << num_entry << ',' << builder.FileSize() << '\n';
  delete file;
}

void build_sstable() {
  auto env = Env::Default();
  WritableFile* file;
  env->NewWritableFile("/tmp/sstable", &file);
  Options options;
  options.compression = kSnappyCompression;
  TableBuilder builder(options, false, file);

  srand(time(NULL));

  char key_buffer[12];
  char value_buffer[128];

  std::vector<uint64_t> buffer;
  for (uint64_t i = 0; i < num_entry; ++i) {
    buffer.push_back(i);
  }

  std::sort(buffer.begin(), buffer.end(), binary_sorter);

  for (uint64_t i = 0; i < num_entry; ++i) {
    *((uint32_t*)(key_buffer)) = buffer[i];
    *((uint64_t*)(key_buffer + 4)) = buffer[i];
    key_buffer[11] = ValueType::kTypeValue;

    rand_string(value_buffer, 128);
    builder.Add(Slice(key_buffer, 12), Slice(value_buffer, 128));
  }
  builder.Finish();
  std::cout << "SSTable Size:" << num_entry << ',' << builder.FileSize()
            << '\n';
  delete file;
}

int main() {
  build_vest();
  build_sstable();
  return 0;
}