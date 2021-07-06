find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})
add_library(colsmjni MODULE jni.cc)
#set_target_properties(leveldbjni PROPERTIES SUFFIX ".jnilib")
target_link_libraries(colsmjni PUBLIC ${JNI_LIBRARIES} leveldb)
