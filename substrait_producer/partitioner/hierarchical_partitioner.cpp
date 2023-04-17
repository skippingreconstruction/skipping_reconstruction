#include "partitioner/hierarchical_partitioner.h"
#include "partitioner/common.h"
#include "partitioner/horizontal_partitioner.h"
#include "partitioner/model.h"
#include <boost/functional/hash.hpp>
#include <limits>
#include <mutex>
#include <queue>
#include <sched.h>
#include <sys/time.h>
#include <thread>
#include <tuple>

typedef pair<shared_ptr<BlockMeta>, boost::dynamic_bitset<>>
    Block_Pattern;
typedef unordered_map<shared_ptr<BlockMeta>,
                      vector<shared_ptr<const BlockMeta>>>
    Block_Cache;

/**
 * @brief Split the table into unit column groups. Columns in each unit
 * group are accessed by the same set of queries
 *
 * @param table
 * @param queries
 * @return vector<Block_Pattern> column groups and their access pattern
 * (bitmap)
 */
vector<Block_Pattern> columnBlocks(
    shared_ptr<const BlockMeta> table,
    const vector<shared_ptr<const Query>> &queries)
{
    vector<Block_Pattern> columns;
    auto table_schema = table->getSchema();
    auto attribute_names = table_schema->getAttributeNames();
    attribute_names.erase(tuple_id_name);
    auto tid = table_schema->get(tuple_id_name);

    // get attributes accessed by each query
    vector<unordered_set<string>> query_attributes[2];
    unordered_set<string> filter_attributes;
    for (auto q : queries)
    {
        auto filter_attr = q->getFilterBoundary()->getAttributes();
        auto project_attr = q->getAllReferredAttributes();
        for (const auto &a : filter_attr)
            project_attr.erase(a);

        query_attributes[0].push_back(filter_attr);
        query_attributes[1].push_back(project_attr);
        filter_attributes.insert(filter_attr.begin(),
                                 filter_attr.end());
    }

    vector<pair<unordered_set<string>, boost::dynamic_bitset<>>>
        patterns[2];

    for (auto a : attribute_names)
    {
        int idx = 1;
        if (filter_attributes.count(a))
            idx = 0;
        auto s = make_shared<Schema>();
        s->add(table_schema->get(a));
        boost::dynamic_bitset<> b(queries.size());
        for (int i = 0; i < queries.size(); i++)
        {
            auto r = s->relationship(query_attributes[idx][i]);
            if (r != SET_RELATION::DISJOINT)
                b.set(i);
        }

        auto it = patterns[idx].begin();
        for (; it != patterns[idx].end(); it++)
            if (it->second == b)
            {
                it->first.insert(a);
                break;
            }
        if (it == patterns[idx].end())
            patterns[idx].push_back(
                make_pair(unordered_set<string>{a}, b));
    }

    for (int i = 0; i < 2; i++)
    {
        for (const auto &p : patterns[i])
        {
            auto s = make_shared<Schema>();
            s->add(tid);
            for (auto a : p.first)
                s->add(table_schema->get(a));
            auto column =
                make_shared<BlockMeta>(0, table->getBoundary(), s,
                                       nullptr, table->getRowNum());
            columns.push_back(make_pair(column, p.second));
        }
    }

    return columns;
}

/**
 * @brief Horizontally partition a set of column groups and estimate the
 * cost after partitioning
 *
 * @param column_groups a set of column groups and their query access
 * patterns
 * @param train_queries all training queries
 * @param validate_queries all validation queries to estimate the cost
 * @param stopCondition
 * @param table_schema
 * @param produceParameters the function the query engines to evaluate a
 * query
 * @param cost OUTPUT estimated cost
 * @param cache optional
 * @param print_stats optional. Default is false
 * @return vector<shared_ptr<const BlockMeta>>
 */
vector<shared_ptr<const BlockMeta>> partitionColumnGroups(
    const vector<Block_Pattern> &column_groups,
    const vector<shared_ptr<const Query>> &train_queries,
    const unordered_set<shared_ptr<const Query>> &validate_queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    shared_ptr<const Schema> table_schema,
    ParameterFunction produceParameters,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long),
    double &cost, int thread_num, Block_Cache *cache = nullptr,
    bool print_stats = false)
{
    // partition each column group
    vector<shared_ptr<const PartitionMeta>> partitions;
    vector<shared_ptr<const BlockMeta>> all_blocks;
    int pid = 0;

    std::mutex lock;
    vector<std::thread *> threads;

    auto partition_one_group = [&](int thread_id) {
        for (int i = 0; i < column_groups.size(); i++)
        {
            if (i % thread_num != thread_id)
                continue;
            auto group = column_groups[i].first;
            auto pattern = column_groups[i].second;
            vector<shared_ptr<const BlockMeta>> blocks;

            lock.lock();
            bool in_cache = cache != nullptr && cache->count(group) > 0;
            lock.unlock();
            if (in_cache)
            {
                lock.lock();
                blocks = cache->at(group);
                lock.unlock();
            }
            else
            {
                unordered_set<shared_ptr<const Query>>
                    train_queries_set;
                for (auto k = pattern.find_first(); k != pattern.npos;
                     k = pattern.find_next(k))
                    train_queries_set.insert(train_queries[k]);

                blocks = horizontalPartition(group, train_queries_set,
                                             stopCondition, {});
                if (cache)
                {
                    lock.lock();
                    cache->insert({group, blocks});
                    lock.unlock();
                }
            }

            lock.lock();
            all_blocks.insert(all_blocks.end(), blocks.begin(),
                              blocks.end());
            for (auto b : blocks)
            {
                auto p = make_shared<PartitionMeta>(
                    std::to_string(pid++) + ".parquet");
                p->addBlock(shared_ptr<BlockMeta>(b->clone()));
                partitions.push_back(p);
            }
            lock.unlock();
        }
    };

    for (int thread_id = 0; thread_id < thread_num; thread_id++)
    {
        threads.push_back(
            new std::thread(partition_one_group, thread_id));
    }
    for (auto t : threads)
    {
        t->join();
        delete t;
    }
    threads.clear();

    // estimate the cost of each query
    // assume using agg reconstruction
    cost = 0;
    vector<shared_ptr<const Query>> v_validate_queries(
        validate_queries.begin(), validate_queries.end());
    auto estimate_cost = [&](int thread_id) {
        for (int i = 0; i < v_validate_queries.size(); i++)
        {
            if (i % thread_num != thread_id)
                continue;
            auto params = produceParameters(v_validate_queries[i],
                                            table_schema, partitions);
            double c =
                estimateCost(params.second, params.first, table_schema,
                             aggModel, print_stats);
            lock.lock();
            cost += c;
            lock.unlock();
        }
    };
    for (int thread_id = 0; thread_id < thread_num; thread_id++)
    {
        threads.push_back(new std::thread(estimate_cost, thread_id));
    }
    for (auto t : threads)
    {
        t->join();
        delete t;
    }
    threads.clear();
    return all_blocks;
}

/**
 * @brief Compute the similarity of the access pattern of each pair
 * column groups and return the top N pairs. The similarity of two
 * bitmaps is count(b1&b2) / count(b1|b2)
 *
 * @param column_groups
 * @param num the number of output pairs
 * @return vector<pair<int, int>>
 */
std::vector<std::pair<int, int>> pairColumnGroupsSample(
    const vector<Block_Pattern> &column_groups, int num,
    const vector<shared_ptr<const Query>> &train_queries)
{
    unordered_set<std::pair<int, int>, boost::hash<pair<int, int>>> ans;

    // find the location of each attribute
    unordered_map<string, int> group_attributes;
    for (int i = 0; i < column_groups.size(); i++)
    {
        auto attrs =
            column_groups[i].first->getSchema()->getAttributeNames();
        for (const string &a : attrs)
            group_attributes[a] = i;
    }
    for (auto q : train_queries)
    {
        auto filter_attr = q->getFilterBoundary()->getAttributes();
        auto proj_attr = q->getAllReferredAttributes();
        for (const auto &fa : filter_attr)
            for (const auto &pa : proj_attr)
            {
                int i = group_attributes[fa], j = group_attributes[pa];
                if (i == j)
                    continue;
                ans.insert(
                    std::make_pair(std::min(i, j), std::max(i, j)));
            }
    }

    priority_queue<tuple<double, int, int>,
                   vector<tuple<double, int, int>>,
                   greater<tuple<double, int, int>>>
        scores;
    for (int i = 0; i < column_groups.size(); i++)
        for (int j = i + 1; j < column_groups.size(); j++)
        {
            if (ans.count(make_pair(i, j)))
                continue;

            double t1 =
                (column_groups[i].second & column_groups[j].second)
                    .count();
            double t2 =
                (column_groups[i].second | column_groups[j].second)
                    .count();
            scores.push(std::make_tuple(t1 / t2, i, j));
            while (scores.size() > num)
                scores.pop();
        }

    while (scores.size() > 0)
    {
        ans.insert(std::make_pair(std::get<1>(scores.top()),
                                  std::get<2>(scores.top())));
        scores.pop();
    }

    return vector<pair<int, int>>(ans.begin(), ans.end());
}

std::vector<std::pair<int, int>> pairColumnGroups(
    const vector<Block_Pattern> &column_groups)
{
    vector<std::pair<int, int>> ans;
    for (int i = 0; i < column_groups.size(); i++)
        for (int j = i + 1; j < column_groups.size(); j++)
        {
            if ((column_groups[i].second & column_groups[j].second)
                    .count() == 0)
                continue;
            ans.push_back(make_pair(i, j));
        }
    return ans;
}

static int hierarchical_step = 0;

std::vector<std::pair<int, int>> pairColumnGroupsWOUnique(
    const vector<Block_Pattern> &column_groups,
    const vector<shared_ptr<const Query>> &queries)
{
    unordered_set<string> filter_attrs;
    vector<unordered_set<string>> query_attributes;
    for (auto q : queries)
    {
        auto a = q->getFilterBoundary()->getAttributes();
        filter_attrs.insert(a.begin(), a.end());
        a = q->getAllReferredAttributes();
        auto it = query_attributes.begin();
        for (; it != query_attributes.end(); it++)
            if (*it == a)
                break;
        if (it == query_attributes.end())
            query_attributes.push_back(a);
    }

    vector<Block_Pattern> groups;
    unordered_map<shared_ptr<BlockMeta>, int> group_idx;
    for (int i = 0; i < column_groups.size(); i++)
    {
        group_idx[column_groups[i].first] = i;
        auto schema = column_groups[i].first->getSchema();
        int count = 0;
        for (const auto &attrs : query_attributes)
            if (schema->relationship(attrs) != SET_RELATION::DISJOINT)
                count++;
        // the block is accessed by more than one query template
        if (count > 1)
        {
            groups.push_back(column_groups[i]);
            continue;
        }

        auto b = column_groups[i].first;
        auto relation = b->getSchema()->relationship(filter_attrs);
        if (relation != SET_RELATION::DISJOINT)
            groups.push_back(column_groups[i]);
    }

    auto make_group = [&](const vector<Block_Pattern> &groups) {
        vector<std::pair<int, int>> ans;
        for (int i = 0; i < groups.size(); i++)
            for (int j = i + 1; j < groups.size(); j++)
            {
                if ((groups[i].second & groups[j].second).count() == 0)
                    continue;
                ans.push_back(make_pair(group_idx.at(groups[i].first),
                                        group_idx.at(groups[j].first)));
            }
        return ans;
    };

    vector<std::pair<int, int>> ans = make_group(groups);
    if (ans.size() == 0)
        ans = make_group(column_groups);
    return ans;
}

vector<shared_ptr<const BlockMeta>> hierarchicalPartition(
    const vector<Block_Pattern> &column_groups,
    const vector<shared_ptr<const Query>> &train_queries,
    const unordered_set<shared_ptr<const Query>> &validate_queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    ParameterFunction produceParameters,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long),
    shared_ptr<const Schema> table_schema, double &cost)
{
    int local_hierarchical_step = hierarchical_step++;
    printf("Step %d\n", local_hierarchical_step);
    for (int i = 0; i < column_groups.size(); i++)
    {
        printf("Schema %d %s\t", i,
               column_groups[i].first->getSchema()->toString().c_str());
    }
    printf("\n");

    int thread_num =
        std::min(12, (int)(std::thread::hardware_concurrency() * 0.8));
    if (thread_num == 0)
        thread_num = 1;

    cost = 0;
    Block_Cache *cache = new Block_Cache();
    auto blocks = partitionColumnGroups(
        column_groups, train_queries, validate_queries, stopCondition,
        table_schema, produceParameters, aggModel, cost, thread_num,
        cache);
    printf("Step %d validation cost %.2f seconds\n",
           local_hierarchical_step, cost);

    auto pairs = pairColumnGroupsWOUnique(column_groups, train_queries);
    if (pairs.size() > 1000)
        pairs =
            pairColumnGroupsSample(column_groups, 1000, train_queries);

    if (pairs.size() == 0)
        return blocks;

    double min_cost = std::numeric_limits<double>::max();
    vector<Block_Pattern> min_groups;

    // partition the pairs and estimate the cost on each layout
    std::mutex lock;
    auto partition_pairs_parallel = [&](int thread_id) {
        for (int pid = 0; pid < pairs.size(); pid++)
        {
            if (pid % thread_num != thread_id)
                continue;
            printf("Step %d Pair %d of %zu\n", local_hierarchical_step,
                   pid, pairs.size());
            int i = pairs[pid].first, j = pairs[pid].second;
            std::vector<Block_Pattern> merged_groups;
            for (int k = 0; k < column_groups.size(); k++)
                if (k != i && k != j)
                    merged_groups.push_back(column_groups[k]);

            BlockMeta *m = column_groups[i].first->clone();
            shared_ptr<Schema> m_schema = make_shared<Schema>();
            m_schema->append(column_groups[i].first->getSchema());
            m_schema->append(column_groups[j].first->getSchema());
            m->setSchema(m_schema);
            auto qbits =
                column_groups[i].second | column_groups[j].second;
            merged_groups.push_back(
                make_pair(shared_ptr<BlockMeta>(m), qbits));

            double c = 0;
            // use one thread because the cache contains all groups
            // except for the merged one
            partitionColumnGroups(merged_groups, train_queries,
                                  validate_queries, stopCondition,
                                  table_schema, produceParameters,
                                  aggModel, c, 1, cache);
            lock.lock();
            if (c < min_cost)
            {
                min_cost = c;
                min_groups = merged_groups;
            }
            lock.unlock();
        }
    };

    vector<thread *> threads;
    for (int i = 0; i < thread_num; i++)
    {
        threads.push_back(new thread(partition_pairs_parallel, i));
    }
    for (auto t : threads)
    {
        t->join();
        delete t;
    }

    // delete the cache before the next function call
    cache->clear();
    delete cache;

    double merged_cost = std::numeric_limits<double>::max();
    auto merged_blocks = hierarchicalPartition(
        min_groups, train_queries, validate_queries, stopCondition,
        produceParameters, aggModel, table_schema, merged_cost);

    if (merged_cost < cost)
    {
        cost = merged_cost;
        return merged_blocks;
    }
    else
        return blocks;
}

vector<shared_ptr<const BlockMeta>> hierarchicalPartition(
    shared_ptr<BlockMeta> table,
    const unordered_set<shared_ptr<const Query>> &train_queries,
    const unordered_set<shared_ptr<const Query>> &validate_queries,
    bool (*stopCondition)(shared_ptr<const BlockMeta> block),
    ParameterFunction produceParameters,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long))
{
    // find the unit column groups
    vector<shared_ptr<const Query>> v_train_queries(
        train_queries.begin(), train_queries.end());
    auto column_group = columnBlocks(table, v_train_queries);

    double cost = 0;
    auto blocks = hierarchicalPartition(
        column_group, v_train_queries, validate_queries, stopCondition,
        produceParameters, aggModel, table->getSchema(), cost);
    return blocks;
}