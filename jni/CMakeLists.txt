find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})
add_library(colsmjni MODULE jni.cc site_ycsb_db_leveldb_LevelDB.h)
#set_target_properties(leveldbjni PROPERTIES SUFFIX ".jnilib")
target_link_libraries(colsmjni ${JNI_LIBRARIES} leveldb)
