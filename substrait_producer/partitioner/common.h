#pragma once
#include "metadata/boundary.h"
#include "produce_plan/scan_parameter.h"
#include <stdio.h>
#include <stdlib.h>
#include <string>

using namespace std;

const int BLOCK_MIN_ROW_NUM = 1 * 1024 * 1024;

struct PartitionParameter
{
    enum PartitionType
    {
        Horizontal,
        Hierarchical_Late,
        Hierarchical_Early
    };

    string schema_path;
    string table_range_path;
    string query_path;
    string validation_path;
    string test_query_path;
    string partition_path;
    PartitionType partition_type;

    static PartitionParameter parse(int argc, char const *argv[]);
};

bool stopByRowNum(shared_ptr<const BlockMeta> block);

double estimateCost(
    const vector<shared_ptr<const ScanParameter>> &recons_params,
    const vector<shared_ptr<const ScanParameter>> &direct_params,
    shared_ptr<const Schema> table_schema,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long),
    bool print_stats = false);

double estimateCost(
    const vector<shared_ptr<const BlockMeta>> &blocks,
    const unordered_set<shared_ptr<const Query>> &test_queries,
    shared_ptr<const Schema> table_schema,
    std::pair<vector<shared_ptr<const ScanParameter>>,
              vector<shared_ptr<const ScanParameter>>> (
        *produceParameter)(
        shared_ptr<const Query> query,
        shared_ptr<const Schema> table_schema,
        const vector<shared_ptr<const PartitionMeta>> &partitions),
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long));