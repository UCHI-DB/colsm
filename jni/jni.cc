#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

#include "colsm/comparators.h"
#include "site_ycsb_db_colsm_CoLSM.h"

std::string fromByteArray(JNIEnv* env, jbyteArray input) {
  jint length = env->GetArrayLength(input);
  std::string result;
  result.resize(length);
  env->GetByteArrayRegion(input, 0, length, (jbyte*)result.data());
  return result;
}

inline jint translate(leveldb::Status status) { return status.intcode(); }

static jclass levelDB_Class;
static jfieldID levelDB_db;
static jfieldID levelDB_comparator;

jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  // Obtain the JNIEnv from the VM and confirm JNI_VERSION
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_9) != JNI_OK) {
    return JNI_ERR;
  }

  jclass tempLocalClassRef = env->FindClass("site/ycsb/db/colsm/CoLSM");
  levelDB_Class = (jclass)env->NewGlobalRef(tempLocalClassRef);
  env->DeleteLocalRef(tempLocalClassRef);

  // Load the method id
  levelDB_db = env->GetFieldID(levelDB_Class, "db", "J");
  levelDB_comparator = env->GetFieldID(levelDB_Class, "comparator", "J");

  return JNI_VERSION_9;
}

void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
  // Obtain the JNIEnv from the VM
  // NOTE: some re-do the JNI Version check here, but I find that redundant
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_9);
  // Destroy the global references
  env->DeleteGlobalRef(levelDB_Class);
}

void JNICALL Java_site_ycsb_db_colsm_CoLSM_init(JNIEnv* env, jobject caller,
                                                jbyteArray folder) {
  std::string folder_name = fromByteArray(env, folder);

  leveldb::DB* db;

  leveldb::Options *options = new leveldb::Options();
  auto intCompare = colsm::intComparator().release();
  options->comparator = intCompare;
  options->create_if_missing = true;
  options->block_size=2*1024*1024;
  options->filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(*options, folder_name, &db);
  if (status.ok()) {
    env->SetLongField(caller, levelDB_db, (int64_t)db);
    env->SetLongField(caller, levelDB_comparator, (int64_t)options);
  }
}

void JNICALL Java_site_ycsb_db_colsm_CoLSM_close(JNIEnv* env, jobject caller) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  leveldb::Options* options =
      (leveldb::Options*)env->GetLongField(caller, levelDB_comparator);

  delete db;

  delete options->comparator;
  delete options->filter_policy;
  delete options;

}

jint JNICALL Java_site_ycsb_db_colsm_CoLSM_put(JNIEnv* env, jobject caller,
                                               jbyteArray jkey,
                                               jbyteArray jvalue) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto value = fromByteArray(env, jvalue);
  auto status = db->Put(leveldb::WriteOptions(), leveldb::Slice(key),
                        leveldb::Slice(value));
  return translate(status);
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_colsm_CoLSM_delete(JNIEnv* env,
                                                            jobject caller,
                                                            jbyteArray jkey) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto status = db->Delete(leveldb::WriteOptions(), leveldb::Slice(key));
  return translate(status);
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_colsm_CoLSM_get(JNIEnv* env,
                                                         jobject caller,
                                                         jbyteArray jkey,
                                                         jobjectArray jvalue) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  std::string value;
  auto status = db->Get(leveldb::ReadOptions(), key, &value);
  if (status.ok()) {
    jbyteArray jresult = env->NewByteArray(value.size());
    env->SetByteArrayRegion(jresult, 0, value.size(),
                            (const jbyte*)value.data());
    env->SetObjectArrayElement(jvalue, 0, jresult);
  }
  return translate(status);
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_colsm_CoLSM_scan(JNIEnv* env, jobject caller, jbyteArray jkey,
                                   jint limit, jobjectArray jvalues) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto iterator = db->NewIterator(leveldb::ReadOptions());
  iterator->Seek(leveldb::Slice(key));
  for (auto i = 0; i < limit; ++i) {
    if (iterator->Valid()) {
      auto value = iterator->value();
      auto jvalue = env->NewByteArray(value.size());
      env->SetByteArrayRegion(jvalue, 0, value.size(),
                              (const jbyte*)value.data());
      env->SetObjectArrayElement(jvalues, i, jvalue);
      iterator->Next();
    } else {
      break;
    }
  }
  delete iterator;
  return 0;
}
