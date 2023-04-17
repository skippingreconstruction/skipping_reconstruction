#pragma once
#include "metadata/boundary.h"
#include "metadata/query.h"

vector<shared_ptr<const BlockMeta>> horizontalPartition(
    shared_ptr<const BlockMeta> table,
    const unordered_set<shared_ptr<const Query>> &queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    unordered_map<string, int> split_num);