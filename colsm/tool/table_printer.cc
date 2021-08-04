//
// Created by Harper on 6/18/21.
//

#include <cassert>
#include <fstream>
#include <iostream>
#include <unordered_map>

using namespace std;

int main(int argc, char** argv) {

  uint32_t key_length;
  std::string key_data;
  uint32_t value_length;
  std::string value_data;
  unordered_map<string, string> store;

  std::ifstream insert_log("/home/cc/insert_log", std::ifstream::in | std::ifstream::binary);
  while (!insert_log.eof()) {
    insert_log.read((char*)&key_length,4) ;
    key_data.resize(key_length);
    insert_log.read(key_data.data(), key_length);

    insert_log.read((char*)&value_length,4);
    value_data.resize(value_length);
    insert_log.read(value_data.data(), value_length);

    store[key_data] = value_data;
  }
  insert_log.close();

  cout << store.size() << '\n';

  std::ifstream search_log("/home/cc/search_log", std::ifstream::in | std::ifstream::binary);
  int counter = 0;
  while(!search_log.eof()) {
    search_log.read((char*)&key_length,4) ;
    key_data.resize(key_length);
    search_log.read(key_data.data(), key_length);

    assert(store.find(key_data) != store.end());
    cout << store[key_data].length() << '\n';
    counter++;
  }

  search_log.close();
  cout << counter << '\n';
}
