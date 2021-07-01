#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

#include "colsm/comparators.h"
#include "site_ycsb_db_leveldb_LevelDB.h"

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_init(JNIEnv* env, jobject caller,
                                                    jbyteArray folder) {
  jint length = env->GetArrayLength(folder);
  std::string folder_name;
  folder_name.resize(length);
  env->GetByteArrayRegion(folder, 0, length, (jbyte*)folder_name.data());

  auto intCompare = colsm::intComparator().release();
  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = intCompare;
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, folder_name, &db);

  auto clazz = env->FindClass("site/ycsb/db/leveldb/LevelDB");
  auto db_inst_id = env->GetFieldID(clazz, "db_instance", "J");
  auto db_comp_id = env->GetFieldID(clazz, "db_comparator", "J");
  env->SetLongField(caller, db_inst_id, (uint64_t)db);
  env->SetLongField(caller, db_comp_id, (uint64_t)intCompare);
}

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_close(JNIEnv* env,
                                                     jobject caller) {
  auto clazz = env->FindClass("site/ycsb/db/leveldb/LevelDB");
  auto db_inst_id = env->GetFieldID(clazz, "db_instance", "J");
  auto db_comp_id = env->GetFieldID(clazz, "db_comparator", "J");
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, db_inst_id);
  leveldb::Comparator* comparator = (leveldb::Comparator*)env->GetLongField(caller, db_comp_id);
  delete db;
  delete comparator;
}

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_put
    (JNIEnv *, jobject, jbyteArray, jbyteArray);


void JNICALL Java_site_ycsb_db_leveldb_LevelDB_delete
    (JNIEnv *, jobject, jbyteArray);


jbyteArray JNICALL Java_site_ycsb_db_leveldb_LevelDB_get
    (JNIEnv *, jobject, jbyteArray);


void JNICALL Java_site_ycsb_db_leveldb_LevelDB_scanStart
    (JNIEnv *, jobject, jbyteArray);


jbyteArray JNICALL Java_site_ycsb_db_leveldb_LevelDB_scanNext
    (JNIEnv *, jobject);


void JNICALL Java_site_ycsb_db_leveldb_LevelDB_scanStop
    (JNIEnv *, jobject);
