//
// Created by Harper on 7/10/21.
//
#include <fstream>
#include <iostream>

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
      param.b.push_back(15000);
    } else {
      param.b.push_back(param.b.back() * 10);
    }
  }
  param.m = 8;

  param.h_epsilon = 179.46;
  param.h_eta = -1740.2;

  param.rv = 10;
  param.rh = 10;

  param.v_mu = 409.41;
  param.v_xi = -7.99e6;
  param.h_mu = 429.87;
  param.h_xi = -5.01e6;

  workload.alpha = 0.5;
  workload.beta = 0;
  workload.gamma = 0.5;

  ofstream log_file("log");
  for (double factor = 0.2; factor <= 3; factor += 0.1) {
    param.v_epsilon = factor * param.h_epsilon;
    param.v_eta = factor * param.h_eta;
    solver.Solve(param, workload);

    log_file << factor << ',';
    for (auto lr : param.level_results) {
      log_file << lr << ',';
    }
    log_file << '\n';
  }
  log_file.close();

  // Write model to output file
  ofstream model_file("colsm_model");
  model_file << param.l << '\n';
  for (auto i = 0; i < param.l; ++i) {
    model_file << param.level_results[i] << '\n';
  }
  model_file.close();
}