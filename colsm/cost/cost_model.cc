//
// Created by harper on 6/11/21.
//

#include "cost_model.h"

#include <fstream>

namespace colsm {

using namespace std;

static int LEVEL_DEFAULT = 7;

CostOracle::CostOracle() {
  if (!ReadModel()) {
    for (auto i = 0; i <= LEVEL_DEFAULT; ++i) {
      level_vertical_.push_back(false);
    }
  }
}

bool CostOracle::ReadModel() {
  level_vertical_.clear();
  ifstream modelFile("colsm_model");
  if (modelFile.good()) {
    uint32_t num_level;
    modelFile >> num_level;
    bool level_assign;
    for (int i = 0; i < num_level; ++i) {
      modelFile >> level_assign;
      level_vertical_.push_back(level_assign);
    }
    return true;
  }
  return false;
}

std::unique_ptr<CostOracle> CostOracle::INSTANCE =
    std::unique_ptr<CostOracle>(new CostOracle());

bool CostOracle::ShouldVertical(int level) { return level_vertical_[level]; }

}  // namespace colsm