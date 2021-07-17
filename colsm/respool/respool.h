//
// Created by Harper on 7/15/21.
//

#ifndef COLSM_RESPOOL_H
#define COLSM_RESPOOL_H

#include <forward_list>
#include <functional>
#include <memory>
#include <mutex>

namespace colsm {

 /**
  * Not thread safe simple pool. For performance evaluation
  * @tparam R
  */
template <typename R>
class simple_pool {
private:
   std::vector<R*> resources_;

    void add(R* resource) {
        resources_.push_back(resource);
    }
public:
    simple_pool(uint32_t max_size, std::function<R*()> creator) {
            for (auto i = 0; i < max_size; ++i) {
                resources_.push_back(creator());
            }
    }
    virtual ~simple_pool() {
        for(auto& r: resources_) {
            delete r;
        }
        resources_.clear();
    }

    std::shared_ptr<R> get() {
        auto res = resources_.back();
        resources_.pop_back();
        return std::shared_ptr<R>((R*)res, [this](R* ptr) { this->add(ptr); });
    }
};

/**
 * mutex based lock synchronized pool
 * @tparam R
 */
template <typename R>
class lock_pool {
private:
    std::mutex lock_;
    std::vector<R*> resources_;

    void add(R* resource) {
        std::lock_guard<std::mutex> guard(lock_);
        resources_.push_back(resource);
    }
public:
    lock_pool(uint32_t max_size, std::function<R*()> creator) {
        for (auto i = 0; i < max_size; ++i) {
            resources_.push_back(creator());
        }
    }
    virtual ~lock_pool() {
        for(auto& r: resources_) {
            delete r;
        }
        resources_.clear();
    }

    std::shared_ptr<R> get() {
        std::lock_guard<std::mutex> guard(lock_);
        auto res = resources_.back();
        resources_.pop_back();
        return std::shared_ptr<R>((R*)res, [this](R* ptr) { this->add(ptr); });
    }
};

template <typename R>
class lockfree_pool {
 private:
  uint32_t size_;
  std::vector<uint64_t> resources_;
  uint64_t ZERO = 0;

 protected:
  void add(R* resource) {
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
  lockfree_pool(uint32_t max_size, std::function<R*()> creator) : size_(max_size) {
    for (auto i = 0; i < max_size; ++i) {
      resources_.push_back((uint64_t)creator());
    }
  }

  virtual ~lockfree_pool() {
    for (auto& r : resources_) {
      delete (R*)r;
    }
    resources_.clear();
  }

  std::shared_ptr<R> get() {
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
    return std::shared_ptr<R>((R*)res, [this](R* ptr) { this->add(ptr); });
  }
};
}  // namespace colsm

#endif  // COLSM_RESPOOL_H
