find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})

if(CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(CMAKE_DEBUG_EXTENSION "d")
else()
    set(CMAKE_DEBUG_EXTENSION "")
endif()

set(COLSM_JNI_SHARED_LIB "colsmjni${CMAKE_DEBUG_EXTENSION}")


add_library(${COLSM_JNI_SHARED_LIB} MODULE jni.cc)
#set_target_properties(leveldbjni PROPERTIES SUFFIX ".jnilib")
target_link_libraries(${COLSM_JNI_SHARED_LIB} PUBLIC ${JNI_LIBRARIES} leveldb)
