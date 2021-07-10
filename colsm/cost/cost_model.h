//
// Created by harper on 6/11/21.
//

#ifndef COLSM_COST_MODEL_H
#define COLSM_COST_MODEL_H
#include <memory>

namespace colsm {

struct CostModelEnv {
  int l; // Total number of levels
  int m; // Number of levels start using leveling
  int b; // Number of entries in a storage block;

  int t; // Compaction ratio
  double fpr; // False positive rate

  double pv; // Block Point lookup for Vertical
  double ph; // Block Point lookup for Horizontal

  double uv; // Update for Vertical
  double uh; // Update for Horizontal

  double rv; // Range Lookup for Vertical
  double rh; // Range Lookup for Horizontal
};

class CostModel {
 protected:
  CostModel();

 public:
  virtual ~CostModel() = default;

  static std::unique_ptr<CostModel> INSTANCE;

  void optimize()

  bool ShouldVertical(int level);
};

}  // namespace colsm
#endif  // LEVELDB_COST_MODEL_H
