#include "RadixJoin.hpp"
#include <iostream>
#include <cmath>
#include <thread>

uint32_t Config::L3_CACHE_SIZE = 24 * 1024 * 1024;
uint32_t Config::NUM_CORES = 8;

RadixJoin::RadixJoin(relation_t &R, relation_t &S) : R_(R), S_(S){
    uint64_t dim = R_.number_tuples * sizeof(tuple_t);

    double N = (double)(dim * 4 * Config::NUM_CORES) / Config::L3_CACHE_SIZE;

    if (N <= 1) {
        bits = 0;
    } else {
        bits = std::ceil(std::log2(N));
    }

    partitions = 1 << bits;
    mask = partitions - 1;
}

RadixJoin::~RadixJoin(){
}




/*
*RADIX JOIN - Implement Radix join with the following requirements
* 1. Multithreaded
* 2. Use Radix Partitioning to create chunks fitting into the cache, but only one pass needed, i.e., figure out how
*    many bits you need to create N partitions
* 
* Input: Use Member Variables R and S
*
* Return: result_relation_t - defined in Types.hpp
*/
result_relation_t &RadixJoin::join(){
    relation_t partitioned_table_R;
    partitioned_table_R.number_tuples = R_.number_tuples;
    partitioned_table_R.data = new tuple_t[R_.number_tuples];

    std::vector<uint64_t> start_points_R;

    if (bits <= 8) {
        start_points_R = partition_helper(R_, partitioned_table_R);
    } else {
        //start_points_R = ...;
    }

    relation_t partitioned_table_S;
    partitioned_table_S.number_tuples = S_.number_tuples;
    partitioned_table_S.data = new tuple_t[S_.number_tuples];

    std::vector<uint64_t> start_points_S;

    if (bits <= 8) {
        start_points_S = partition_helper(S_, partitioned_table_S);
    } else {
        //start_points_S = ...;
    }

    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> results_lists(Config::NUM_CORES);
    uint64_t size = 0;
    std::vector<std::thread> threads;
    uint64_t chunk_size = partitions / Config::NUM_CORES;

    for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
        uint64_t start = core * chunk_size;
        uint64_t stop = start + chunk_size;

        if (core == Config::NUM_CORES - 1) {
            stop = partitions;
        }

        threads.emplace_back([&, core, start, stop](){
            for (uint64_t part = start; part < stop; part++) {
                uint64_t start_part = start_points_R[part];
                uint64_t end_part = start_points_R[part + 1];
                uint64_t dim = 4 * (end_part - start_part);

                if (dim == 0) {
                    continue;
                }

                std::vector<tuple_t> hash_map(dim, {UINT64_MAX, 0});

                for (uint64_t idx = start_part; idx < end_part; idx++) {
                    tuple_t elem = partitioned_table_R.data[idx];
                    uint64_t val = (elem.key >> bits) % dim;

                    while (hash_map[val].key != UINT64_MAX) {
                        val++;

                        if (val >= dim) {
                            val = 0;
                        }
                    }

                    hash_map[val] = elem;
                }


                start_part = start_points_S[part];
                end_part = start_points_S[part + 1];
                
                for (uint64_t idx = start_part; idx < end_part; idx++) {
                    tuple_t elem = partitioned_table_S.data[idx];
                    uint64_t pos = (elem.key >> bits) % dim;

                    while (hash_map[pos].key != UINT64_MAX && hash_map[pos].key != elem.key) {
                        pos++;

                        if (pos >= dim) {
                            pos = 0;
                        }
                    }

                    if (hash_map[pos].key == elem.key) {
                        results_lists[core].push_back({hash_map[pos].rid, elem.rid});
                    }
                }
            }
        });
    }

    for (auto& t:threads) {
        t.join();
    }

    for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
        size += results_lists[core].size();
    }

    result.data.reserve(size);

    for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
        result.data.insert(result.data.end(), results_lists[core].begin(), results_lists[core].end()); 
    }

    return result;
}

std::vector<uint64_t> RadixJoin::partition_helper(relation_t &table, relation_t &partitioned_table) {
    std::vector<std::vector<uint64_t>> local_counts(Config::NUM_CORES, std::vector<uint64_t>(partitions, 0));
    std::vector<std::vector<uint64_t>> offsets(Config::NUM_CORES, std::vector<uint64_t>(partitions, 0));
    std::vector<std::thread> threads;
    uint64_t chunk_size = table.number_tuples / Config::NUM_CORES;

    for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
        uint64_t start = core * chunk_size;
        uint64_t stop = start + chunk_size;

        if (core == Config::NUM_CORES - 1) {
            stop = table.number_tuples;
        }

        threads.emplace_back([&, core, start, stop](){
            for (uint64_t idx = start; idx < stop; idx++) {
                local_counts[core][mask&table.data[idx].key]++;
            }
        });
    }

    for (auto& t:threads) {
        t.join();
    }

    uint64_t current_off = 0;

    std::vector<uint64_t> start_points(partitions + 5);
    for (uint64_t idx = 0; idx < partitions; idx++) {
        for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
            offsets[core][idx] = current_off;
            current_off += local_counts[core][idx];
        }

        start_points[idx] = offsets[0][idx];
    }

    start_points[partitions] = table.number_tuples;

    threads.clear();

    for (uint64_t core = 0; core < Config::NUM_CORES; core++) {
        uint64_t start = core * chunk_size;
        uint64_t stop = start + chunk_size;

        if (core == Config::NUM_CORES - 1) {
            stop = table.number_tuples;
        }

        threads.emplace_back([&, core, start, stop](){
            for (uint64_t idx = start; idx < stop; idx++) {
                uint64_t key = mask&table.data[idx].key;
                uint64_t dest = offsets[core][key];

                partitioned_table.data[dest] = table.data[idx];

                offsets[core][key]++;
            }
        });
    }

    for (auto& t:threads) {
        t.join();
    }

    return start_points;
}



