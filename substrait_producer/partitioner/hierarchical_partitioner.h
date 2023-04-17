#pragma once
#include "metadata/boundary.h"
#include "metadata/query.h"
#include "produce_plan/scan_parameter.h"

typedef std::pair<vector<shared_ptr<const ScanParameter>>,
                  vector<shared_ptr<const ScanParameter>>> (
    *ParameterFunction)(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions);

vector<shared_ptr<const BlockMeta>> hierarchicalPartition(
    shared_ptr<BlockMeta> table,
    const unordered_set<shared_ptr<const Query>> &train_queries,
    const unordered_set<shared_ptr<const Query>> &validate_queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    ParameterFunction produceParameters,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long));