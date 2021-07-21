//
// Created by Harper on 7/10/21.
//

#include "solver.h"
#include <math.h>

#include <ilcplex/ilocplex.h>

namespace colsm {

    using namespace std;

    void CPlexSolver::Solve(Parameter &parameter, Workload &workload) {
        SolveLevelDB(parameter, workload);
    }

    void CPlexSolver::SolveLevelDB(Parameter &param, Workload &workload) {
        IloEnv env;
        IloModel model(env);

        IloNumVarArray var(env);

        // Define variables
        for (auto i = 0; i < param.l; ++i) {
            var.add(IloNumVar(env, 0, 1, ILOINT));
        }

        // sum 0.1 p(B_i)
        //

        // P = \sum T p P
        // R = P + R
        // U = \sum D/B
        // result = alpha P + beta R + gamma U
        auto p = param.t[0] * param.fpr[0] * (
                (var[0] * param.v_epsilon + (1 - var[0]) * param.h_epsilon) * std::log(param.b[0])
                + var[0] * param.v_eta + (1 - var[0]) * param.h_eta);
        auto r = param.t[0] * (param.fpr[0] * (
                (var[0] * param.v_epsilon + (1 - var[0]) * param.h_epsilon) * std::log(param.b[0])
                + var[0] * param.v_eta + (1 - var[0]) * param.h_eta)
                               + var[0] * param.rv + (1 - var[0]) * param.rh);
        auto u = (var[0] * param.v_mu + (1 - var[0]) * param.h_mu) +
                 (var[0] * param.v_xi + (1 - var[0]) * param.h_xi) / param.b[0];

        for (int i = 1; i < param.l; ++i) {
            p = p + param.t[i] * param.fpr[i] * (
                    (var[i] * param.v_epsilon + (1 - var[i]) * param.h_epsilon) * std::log(param.b[i])
                    + var[i] * param.v_eta + (1 - var[i]) * param.h_eta);
            r = r + param.t[i] * (param.fpr[i] * (
                    (var[i] * param.v_epsilon + (1 - var[i]) * param.h_epsilon) * std::log(param.b[i])
                    + var[i] * param.v_eta + (1 - var[i]) * param.h_eta)
                                  + var[i] * param.rv + (1 - var[i]) * param.rh);
            u = u + (var[i] * param.v_mu + (1 - var[i]) * param.h_mu) +
                    (var[i] * param.v_xi + (1 - var[i]) * param.h_xi) / param.b[i];
        }

        auto result = workload.alpha * p + workload.beta * r + workload.gamma * u;

        model.add(IloMinimize(env, result));

        IloCplex cplex(model);

        cplex.solve();

        env.out() << "Solution status = " << cplex.getStatus() << endl;
        env.out() << "Solution value  = " << cplex.getObjValue() << endl;

        IloNumArray vals(env);
        cplex.getValues(vals, var);
        env.out() << "Values        = " << vals << endl;

        auto intArray = vals.toIntArray();
        param.level_results.clear();
        for (int i = 0; i < param.l; ++i) {
            param.level_results.push_back(intArray[i] != 0);
        }

        env.end();

        return;
    }

}  // namespace colsm