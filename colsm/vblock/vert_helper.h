//
// Helper functions for coLSM
//
// Created by harper on 7/5/21.
//

#ifndef LEVELDB_VERT_HELPER_H
#define LEVELDB_VERT_HELPER_H

#include <string>
#include <leveldb/env.h>
namespace colsm {

// Read meta data from table and determine if it is vertical
// Used primarily by RepairTable to know what format the table is using
bool IsVerticalTable(leveldb::Env* env, const std::string& filename);

}  // namespace colsm
#endif  // LEVELDB_VERT_HELPER_H
