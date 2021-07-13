# CMake files to build the solver which involves linking a ILOG CPlex library

# CPlex settings
set(CONCERT_ROOT "${CMAKE_SOURCE_DIR}/thirdparty/concert")
set(CPLEX_ROOT "${CMAKE_SOURCE_DIR}/thirdparty/cplex")
include_directories(${CONCERT_ROOT}/include)
include_directories(${CPLEX_ROOT}/include)

add_library(Concert::Concert STATIC IMPORTED)
set_target_properties(Concert::Concert PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CONCERT_ROOT}/include
        IMPORTED_LOCATION ${CONCERT_ROOT}/lib/x86-64_linux/static_pic/libconcert.a
        )

add_library(CPLEX::CPLEX STATIC IMPORTED)
set_target_properties(CPLEX::CPLEX PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CPLEX_ROOT}/include
        IMPORTED_LOCATION ${CPLEX_ROOT}/lib/x86-64_linux/libcplex.a
        )
add_library(CPLEX::ILOCPLEX STATIC IMPORTED)
set_target_properties(CPLEX::ILOCPLEX PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CPLEX_ROOT}/include
        IMPORTED_LOCATION ${CPLEX_ROOT}/lib/x86-64_linux/libilocplex.a
        )
#
add_executable(colsm_run_solver "")
target_sources(colsm_run_solver
        PRIVATE
            run_solver.cc
            solver.cc
        )

# Pay attention to the library sequence, ILO must be before CPLEX or there will be errors. Don't know why yet
target_link_libraries(colsm_run_solver PRIVATE Concert::Concert CPLEX::ILOCPLEX CPLEX::CPLEX Threads::Threads m dl)
target_compile_options(colsm_run_solver PRIVATE -fexceptions -fPIC -fno-strict-aliasing)