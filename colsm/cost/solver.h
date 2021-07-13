//
// Created by Harper on 7/10/21.
//

#ifndef COLSM_COST_SOLVER_H
#define COLSM_COST_SOLVER_H

#include <vector>

#include "cost_model.h"

namespace colsm {
class CPlexSolver {
 protected:
  void SolveLevelDB(Parameter&, Workload&);

 public:
  void Solve(Parameter&, Workload&);
};

}  // namespace colsm
#endif  // COLSM_COST_SOLVER_H
