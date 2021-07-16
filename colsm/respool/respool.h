//
// Created by Harper on 7/15/21.
//

#ifndef COLSM_RESPOOL_H
#define COLSM_RESPOOL_H

#include <atomic>
#include <forward_list>
#include <functional>
#include <memory>

namespace colsm {

template <typename R>
class ResPool {
 private:
  uint32_t size_;
  std::vector<uint64_t> resources_;
  uint64_t ZERO = 0;

 protected:
  void Add(R* resource) {
    while (true) {
      uint32_t index = 0;
      while (resources_[index] != 0) {
        index++;
        index %= size_;
      }
      if (__atomic_compare_exchange_n(resources_.data() + index, &ZERO,
                                      (uint64_t)resource, false,
                                      __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
        break;
      } else {
        index++;
        index %= size_;
      }
    }
  }

 public:
  ResPool(uint32_t max_size, std::function<R*()> creator) : size_(max_size) {
    for (auto i = 0; i < max_size; ++i) {
      resources_.push_back((uint64_t)creator());
    }
  }

  virtual ~ResPool() {
    for (auto& r : resources_) {
      delete (R*)r;
    }
    resources_.clear();
  }

  std::shared_ptr<R> Get() {
    uint64_t res;
    while (true) {
      uint32_t index = 0;
      while ((res = resources_[index]) == 0) {
        index++;
        index %= size_;
      }
      if (__atomic_compare_exchange_n(resources_.data() + index, &res, ZERO,
                                      false, __ATOMIC_ACQUIRE,
                                      __ATOMIC_ACQUIRE)) {
        break;
      } else {
        index++;
        index %= size_;
      }
    }
    return std::shared_ptr<R>((R*)res, [this](R* ptr) { this->Add(ptr); });
  }
};
}  // namespace colsm

#endif  // COLSM_RESPOOL_H
