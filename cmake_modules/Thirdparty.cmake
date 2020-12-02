macro(build_sboost)

    set(SBOOST_REPO_URL "https://github.com/UCHI-DB/sboost.git")

    if(CMAKE_BUILD_TYPE MATCHES DEBUG)
        set(CMAKE_SBOOST_DEBUG_EXTENSION "d")
    else()
        set(CMAKE_SBOOST_DEBUG_EXTENSION "")
    endif()

    set(SBOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/sboost_ep-prefix/src/sboost_ep")
    set(_SBOOST_LIBRARY_SUFFIX "${CMAKE_SBOOST_DEBUG_EXTENSION}${CMAKE_STATIC_LIBRARY_SUFFIX}")


    set(SBOOST_STATIC_LIB ${SBOOST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}sboost${_SBOOST_LIBRARY_SUFFIX})
    set(SBOOST_INCLUDE_DIR ${SBOOST_PREFIX}/include)


    set(SBOOST_CMAKE_ARGS
        ${EP_COMMON_TOOLCHAIN}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_INSTALL_PREFIX=${SBOOST_PREFIX}"
        -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})


    externalproject_add(sboost_ep
        GIT_REPOSITORY ${SBOOST_REPO_URL}
        GIT_TAG main
        BUILD_BYPRODUCTS ${SBOOST_STATIC_LIB}
        CMAKE_ARGS ${SBOOST_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    add_library(sboost_static STATIC IMPORTED)
    set_target_properties(sboost_static
            PROPERTIES IMPORTED_LOCATION "${SBOOST_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${SBOOST_INCLUDE_DIR}")
    add_dependencies(sboost_static sboost_ep)
    file(MAKE_DIRECTORY ${SBOOST_INCLUDE_DIR})
    include_directories(${SBOOST_INCLUDE_DIR})
    set(SBOOST_SIMD_FLAGS -msse4.1 -mavx -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2)
endmacro()

macro(build_gbenchmark)
    message(STATUS "Building benchmark from source")
    set(GBENCHMARK_BUILD_VERSION 1.5.2)
    set(GBENCHMARK_SOURCE_URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_BUILD_VERSION}.tar.gz")

    set(GBENCHMARK_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -std=c++11")

    set(GBENCHMARK_PREFIX
            "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(
            GBENCHMARK_STATIC_LIB
            "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(
            GBENCHMARK_MAIN_STATIC_LIB
            "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(GBENCHMARK_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${GBENCHMARK_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib
            -DBENCHMARK_ENABLE_TESTING=OFF
            -DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS})

    externalproject_add(gbenchmark_ep
            URL ${GBENCHMARK_SOURCE_URL}
            BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
            "${GBENCHMARK_MAIN_STATIC_LIB}"
            CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${GBENCHMARK_INCLUDE_DIR}")

    add_library(benchmark::benchmark STATIC IMPORTED)
    set_target_properties(benchmark::benchmark
            PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES
            "${GBENCHMARK_INCLUDE_DIR}")

    add_library(benchmark::benchmark_main STATIC IMPORTED)
    set_target_properties(benchmark::benchmark_main
            PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_MAIN_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES
            "${GBENCHMARK_INCLUDE_DIR}")

    add_dependencies(benchmark::benchmark gbenchmark_ep)
    add_dependencies(benchmark::benchmark_main gbenchmark_ep)
    include_directories(${GBENCHMARK_INCLUDE_DIR})
endmacro()

include(ExternalProject)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")

set(EP_COMMON_TOOLCHAIN -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

if(CMAKE_AR)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_AR=${CMAKE_AR})
endif()

if(CMAKE_RANLIB)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_RANLIB=${CMAKE_RANLIB})
endif()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
        ${EP_COMMON_TOOLCHAIN}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_C_FLAGS=${EP_C_FLAGS}
        -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})

set(EP_LOG_OPTIONS
        LOG_CONFIGURE
        1
        LOG_BUILD
        1
        LOG_INSTALL
        1
        LOG_DOWNLOAD
        1)