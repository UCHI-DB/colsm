//
// Created by harper on 6/11/21.
//

#ifndef COLSM_COST_MODEL_H
#define COLSM_COST_MODEL_H
#include <memory>

namespace colsm {

class CostModel {
 protected:
  CostModel();

 public:
  virtual ~CostModel() = default;

  static std::unique_ptr<CostModel> INSTANCE;

  bool ShouldVertical(int level);
};

}  // namespace colsm
#endif  // LEVELDB_COST_MODEL_H
