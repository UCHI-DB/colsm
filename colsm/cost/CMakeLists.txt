# CMake files to build the solver which involves linking a ILOG CPlex library

# CPlex settings
set(CPLEX_ROOT "${CMAKE_SOURCE_DIR}/thirdparty/cplex")
include_directories(${CPLEX_ROOT}/include)

add_library(CPLEX::CPLEX STATIC IMPORTED)
set_target_properties(CPLEX::CPLEX PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CPLEX_ROOT}/include
        IMPORTED_LOCATION ${CPLEX_ROOT}/lib/x86-64_linux/libcplex.a
        )
add_library(CPLEX::CPLEXdistmip STATIC IMPORTED)
set_target_properties(CPLEX::CPLEXdistmip PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CPLEX_ROOT}/include
        IMPORTED_LOCATION ${CPLEX_ROOT}/lib/x86-64_linux/libcplexdistmip.a
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
target_link_libraries(colsm_run_solver PRIVATE CPLEX::CPLEX)# CPlex::cplexdistmip CPlex::ilocplex)