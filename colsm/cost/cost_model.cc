//
// Created by harper on 6/11/21.
//

#include "cost_model.h"

namespace colsm {

CostModel::CostModel() {}

std::unique_ptr<CostModel> CostModel::INSTANCE =
    std::unique_ptr<CostModel>(new CostModel());

bool CostModel::ShouldVertical(int level) { return true; }

}  // namespace colsm