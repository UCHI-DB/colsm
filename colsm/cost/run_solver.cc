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
  CostModel model;


  solver.Solve(model);

  // Write model to output file
  ofstream model_file("colsm_model");
  model_file << model.l << '\n';
  for (auto i = 0; i < model.l; ++i) {
    model_file << model.level_results[i] << '\n';
  }
  model_file.close();
}