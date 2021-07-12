//
// Created by Harper on 7/10/21.
//

#include "solver.h"

#include <ilcplex/ilocplex.h>

namespace colsm {

using namespace std;

void CPlexSolver::Solve(CostModel& cost_model) {
  IloEnv env;
  IloModel model(env);

  IloNumVarArray var(env);
  IloRangeArray con(env);

  IloCplex cplex(model);
  cplex.solve();

  env.out() << "Solution status = " << cplex.getStatus() << endl;
  env.out() << "Solution value  = " << cplex.getObjValue() << endl;

  IloNumArray vals(env);
  cplex.getValues(vals, var);
  env.out() << "Values        = " << vals << endl;
  cplex.getSlacks(vals, con);
  env.out() << "Slacks        = " << vals << endl;

  cplex.exportModel("mipex1.lp");

  env.end();
  return;

}  // END main

}
}  // namespace colsm