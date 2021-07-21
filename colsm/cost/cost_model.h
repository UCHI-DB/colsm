//
// Created by harper on 6/11/21.
//

#ifndef COLSM_COST_MODEL_H
#define COLSM_COST_MODEL_H
#include <memory>
#include <vector>

namespace colsm {

struct Parameter {
  int l;  // Total number of levels
  int m;  // Number of levels start using leveling

  std::vector<int> b;       // Number of entries in a storage block;
  std::vector<int> t;       // Compaction ratio
  std::vector<double> fpr;  // False positive rate

  // p(x) parameters
  double v_epsilon;
    double v_eta;
  double h_epsilon;
  double h_eta;

  double rv;  // Range Lookup for Vertical
  double rh;  // Range Lookup for Horizontal

  double v_mu;  // Update for Vertical
  double v_xi;
  double h_mu;  // Update for Horizontal
  double h_xi;

  std::vector<bool> level_results;
};

struct Workload {
  double alpha;  // Percentage of point lookup
  double beta;   // Percentage of range lookup
  double gamma;  // Percentage of update
};

class CostModel {
 protected:
  CostModel();

  std::vector<bool> level_vertical_;

  bool ReadModel();

 public:
  virtual ~CostModel() = default;

  static std::unique_ptr<CostModel> INSTANCE;

  bool ShouldVertical(int level);
};

}  // namespace colsm
#endif  // COLSM_COST_MODEL_H
