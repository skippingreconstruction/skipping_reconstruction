#include "partitioner/horizontal_partitioner.h"
#include <algorithm>
#include <limits>
#include <random>

/**
 * @brief Identify the queries that read data from the block and compute
 * the I/O size
 *
 * @param queries all candiate queries
 * @param intersect_queries OUTPUT: queries that read data from the
 * block
 * @return size_t the I/O size
 */
size_t estimateIOSize(
    shared_ptr<const BlockMeta> block,
    const unordered_set<shared_ptr<const Query>> &queries,
    unordered_set<shared_ptr<const Query>> &intersect_queries)
{
    intersect_queries.clear();
    size_t size = 0;
    for (auto q : queries)
    {
        auto query_attributes = q->getAllReferredAttributes();
        SET_RELATION rel = block->relationship(q->getFilterBoundary(),
                                               query_attributes);
        if (rel == SET_RELATION::DISJOINT)
            continue;
        intersect_queries.insert(q);
        size += block->estimateIOSize(query_attributes);
    }
    return size;
}

struct SplitInfo
{
    shared_ptr<const BlockMeta> block;
    unordered_set<shared_ptr<const Query>> queries;
    unordered_map<string, int> split_num;
};

vector<shared_ptr<const BlockMeta>> resizeBlock(
    shared_ptr<const BlockMeta> block,
    const unordered_map<string, int> &ratio,
    bool (*stopCondition)(shared_ptr<const BlockMeta>))
{
    if (stopCondition(block))
        return {block};

    int sum = 0;
    for (auto it = ratio.begin(); it != ratio.end(); it++)
        sum += it->second;
    auto intervals = block->getBoundary()->getIntervals();
    unordered_set<string> checked_attr;

    vector<shared_ptr<BlockMeta>> candidates;
    bool has_produced = false;
    srand(time(NULL));
    while (checked_attr.size() < ratio.size() && candidates.size() == 0)
    {
        int r = rand() % sum;
        string attr;
        for (auto it = ratio.begin(); it != ratio.end() && r >= 0; it++)
        {
            attr = it->first;
            r -= it->second;
        }
        if (checked_attr.count(attr))
            continue;
        checked_attr.insert(attr);

        // split the block
        assert(intervals.count(attr) > 0);
        auto i = intervals[attr];
        auto p = shared_ptr<DataType>(
            i->getMin()->middle(i->getMax().get(), 0.5));
        candidates = block->split(attr, p, true);

        if (candidates.size() > 0)
            has_produced = true;
        // avoid to produce small blocks
        for (auto c : candidates)
            if (stopCondition(c))
            {
                candidates.clear();
                break;
            }
    }

    if (!has_produced && candidates.size() == 0)
    {
        auto min_max_attrs = getMinMaxAttributes();
        for (auto a : min_max_attrs)
        {
            shared_ptr<DataType> p;
            if (intervals.count(a))
            {
                auto i = intervals[a];
                p = shared_ptr<DataType>(
                    i->getMin()->middle(i->getMax().get(), 0.5));
            }
            else
            {
                auto min_v = getMinValue(a), max_v = getMaxValue(a);
                p = shared_ptr<DataType>(
                    min_v->middle(max_v.get(), 0.5));
            }

            candidates = block->split(a, p, true);
            // avoid to produce small blocks
            for (auto c : candidates)
                if (stopCondition(c))
                {
                    candidates.clear();
                    break;
                }
            if (candidates.size() > 0)
                break;
        }
    }

    if (candidates.size() > 0)
    {
        vector<shared_ptr<const BlockMeta>> ans;
        for (auto b : candidates)
        {
            auto t = resizeBlock(b, ratio, stopCondition);
            ans.insert(ans.end(), t.begin(), t.end());
        }
        return ans;
    }
    else
        return {block};
}

unordered_set<shared_ptr<const Query>> sampleQueries(
    const unordered_set<shared_ptr<const Query>> &queries, int max_num,
    shared_ptr<const BlockMeta> block)
{
    srand(time(NULL));
    // filter_queries reads data from the block
    unordered_set<shared_ptr<const Query>> filter_queries;
    estimateIOSize(block, queries, filter_queries);

    if (max_num >= filter_queries.size())
        return filter_queries;
    vector<shared_ptr<const Query>> v_queries(filter_queries.begin(),
                                              filter_queries.end());
    unordered_set<shared_ptr<const Query>> sample;
    while (sample.size() < max_num)
    {
        int id = rand() % filter_queries.size();
        sample.insert(v_queries[id]);
    }
    return sample;
}

vector<shared_ptr<const BlockMeta>> horizontalPartition(
    shared_ptr<const BlockMeta> block,
    const unordered_set<shared_ptr<const Query>> &queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    unordered_map<string, int> split_num)
{
    if (stopCondition(block))
        return {block};

    // try to partition the block by each endpoint
    vector<SplitInfo> children(2);
    size_t min_cost = std::numeric_limits<size_t>::max();

    // sample 30 queries
    auto sample_queries = sampleQueries(queries, 30, block);

    for (auto q : sample_queries)
    {
        auto intervals = q->getFilterBoundary()->getIntervals();
        for (auto it = intervals.begin(); it != intervals.end(); it++)
        {
            string attr = it->first;
            auto min_val = it->second->getMin(),
                 max_val = it->second->getMax();

            auto candidate = block->split(attr, min_val, false);
            if (candidate.size() > 0)
            {
                assert(candidate.size() == 2);
                unordered_set<shared_ptr<const Query>> q1, q2;
                size_t cost = 0;
                cost += estimateIOSize(candidate[0], queries, q1);
                cost += estimateIOSize(candidate[1], queries, q2);
                if (cost < min_cost)
                {
                    min_cost = cost;
                    auto snum = split_num;
                    if (snum.count(attr))
                        snum[attr]++;
                    else
                        snum[attr] = 1;

                    // avoid to produce small partitions
                    if (!stopCondition(candidate[0]) &&
                        !stopCondition(candidate[1]))
                    {
                        children[0] = {candidate[0], q1, snum};
                        children[1] = {candidate[1], q2, snum};
                    }
                }
            }

            candidate = block->split(attr, max_val, true);
            if (candidate.size() > 0)
            {
                assert(candidate.size() == 2);
                unordered_set<shared_ptr<const Query>> q1, q2;
                size_t cost = 0;
                cost += estimateIOSize(candidate[0], queries, q1);
                cost += estimateIOSize(candidate[1], queries, q2);
                if (cost < min_cost)
                {
                    min_cost = cost;
                    auto snum = split_num;
                    if (snum.count(attr))
                        snum[attr]++;
                    else
                        snum[attr] = 1;

                    if (!stopCondition(candidate[0]) &&
                        !stopCondition(candidate[1]))
                    {
                        children[0] = {candidate[0], q1, snum};
                        children[1] = {candidate[1], q2, snum};
                    }
                }
            }
        }
    }

    vector<shared_ptr<const BlockMeta>> ans;
    if (children[0].block)
    {
        for (int i = 0; i < 2; i++)
        {
            auto t = horizontalPartition(
                children[i].block, children[i].queries, stopCondition,
                children[i].split_num);
            ans.insert(ans.end(), t.begin(), t.end());
        }
    }
    else
        ans = resizeBlock(block, split_num, stopCondition);
    return ans;
}