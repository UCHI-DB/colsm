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
    void SolveLevelDB(CostModel&);
 public:
  void Solve(CostModel& );
};

}  // namespace colsm
#endif  // COLSM_COST_SOLVER_H
