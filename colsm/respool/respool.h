//
// Created by Harper on 7/15/21.
//

#ifndef COLSM_RESPOOL_H
#define COLSM_RESPOOL_H

#include <memory>
#include <forward_list>
#include <atomic>

namespace colsm {

    template<typename R>
    class ResPool {
        using ptr_type = std::unique_ptr<R, std::function<void(R*)>>;
    private:
        uint32_t size_;
        std::vector<std::atomic<R*>> resources_;
    protected:
        void Add(R* resource) {

        }
    public:
        ResPool(uint32_t max_size):size_(max_size) {
            for(auto i = 0 ; i < max_size;++i) {
                resources_.emplace_back(new R());
            }
        }

        virtual ~ResPool() {
            for(auto &r:resources_) {
                delete r.load();
            }
            resources_.clear();
        }

        std::unique_ptr<R> Get() {
                ptr_type tmp(resources_.top().release(),
                             [this](R *ptr) { this->Add(ptr); });
                resources_.pop();
                return std::move(tmp);
        }
    };
}

#endif //COLSM_RESPOOL_H
