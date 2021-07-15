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
  Parameter param;
  Workload workload;

  param.l = 7;
  for (auto i = 0; i < 7; ++i) {
    param.t.push_back(10);
    param.fpr.push_back(0.01);
    if (param.b.empty()) {
      param.b.push_back(1024);
    } else {
      param.b.push_back(param.b.back() * 10);
    }
  }
  param.m = 8;
  param.pv = 1;
  param.ph = 2;
  param.rv = 10;
  param.rh = 10;
  param.uv = 5000000;
  param.uh = 5;

  workload.alpha = 0.5;
  workload.beta = 0;
  workload.gamma = 0.5;

  solver.Solve(param, workload);

  // Write model to output file
  ofstream model_file("colsm_model");
  model_file << param.l << '\n';
  for (auto i = 0; i < param.l; ++i) {
    model_file << param.level_results[i] << '\n';
  }
  model_file.close();
}