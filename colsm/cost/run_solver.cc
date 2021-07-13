//
// Created by Harper on 7/10/21.
//
#include <fstream>

#include "solver.h"

using namespace std;
using namespace colsm;
int main() {
  CPlexSolver solver;
  // Get input parameters
  Parameter parameter;
  Workload workload;


  solver.Solve(parameter, workload);

  // Write model to output file
  ofstream model_file("colsm_model");
  model_file << parameter.l << '\n';
  for (auto i = 0; i < parameter.l; ++i) {
    model_file << parameter.level_results[i] << '\n';
  }
  model_file.close();
}