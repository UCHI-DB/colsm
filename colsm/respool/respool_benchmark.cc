//
// Created by Harper on 7/17/21.
//

#include <benchmark/benchmark.h>
#include "respool.h"

class PoolItem {

};

void create(benchmark::State & state) {
   for(auto _ :state) {
       auto item = new PoolItem();
       benchmark::DoNotOptimize(item);
       delete item;
   }
}

void stack(benchmark::State & state) {
    for(auto _ :state) {
        PoolItem item;
        benchmark::DoNotOptimize(item);
    }
}

void simple(benchmark::State& state) {
    colsm::simple_pool<PoolItem> pool(10, [](){return new PoolItem();});
    for(auto _:state) {
       auto item = pool.get();
       benchmark::DoNotOptimize(item);
    }
}

void lock(benchmark::State& state) {
    colsm::lock_pool<PoolItem> pool(10, [](){return new PoolItem();});
    for(auto _:state) {
        auto item = pool.get();
        benchmark::DoNotOptimize(item);
    }
}

void lockfree(benchmark::State& state) {
    colsm::lockfree_pool<PoolItem> pool(10, [](){return new PoolItem();});
    for(auto _:state) {
        auto item = pool.get();
        benchmark::DoNotOptimize(item);
    }
}

BENCHMARK(create);
BENCHMARK(stack);
BENCHMARK(simple);
BENCHMARK(lock);
BENCHMARK(lockfree);