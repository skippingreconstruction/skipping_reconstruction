#include "produce_plan/join_sequence.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include <list>

shared_ptr<Schema> readMiniTable(
    ::substrait::Rel *rel,
    const unordered_set<shared_ptr<const ScanParameter>> &blocks,
    shared_ptr<const Schema> table_schema)
{
    assert(blocks.size() > 0);
    boost::dynamic_bitset<> project_attributes(table_schema->size());
    for (auto b : blocks)
        project_attributes |= b->project_attributes;
    vector<substrait::Rel *> read_rel;
    shared_ptr<Schema> schema_after_read;
    for (auto b : blocks)
    {
        auto rel = new substrait::Rel();
        shared_ptr<Schema> s = readForReconstruction(
            rel, b, table_schema, project_attributes);
        read_rel.push_back(rel);
        if (!schema_after_read)
            schema_after_read = s;
        else if (!schema_after_read->equal(s))
            throw Exception(
                "readMiniTable:inputs of union must have same schema");
    }

    if (blocks.size() == 1)
    {
        *rel = *read_rel[0];
        delete read_rel[0];
        return schema_after_read;
    }

    ::substrait::Rel *union_rel, *exchange_rel;
    if (InputParameter::get()->engine == InputParameter::Engine::Arrow)
    {
        union_rel = rel;
        exchange_rel = nullptr;
    }
    else
    {
        exchange_rel = rel;
        union_rel = exchange_rel->mutable_exchange()->mutable_input();
    }

    auto schema_after_union = unionAll(union_rel, schema_after_read);
    for (auto r : read_rel)
    {
        *union_rel->mutable_set()->add_inputs() = *r;
        delete r;
    }

    if (exchange_rel)
        schema_after_union =
            exchange(exchange_rel, schema_after_union,
                     {schema_after_union->get(tuple_id_name)});

    return schema_after_union;
}

shared_ptr<Schema> makeJoinRel(::substrait::Rel *rel,
                               shared_ptr<const Schema> left_schema,
                               ::substrait::Rel *left_rel,
                               shared_ptr<const Schema> right_schema,
                               ::substrait::Rel *right_rel,
                               shared_ptr<const Schema> table_schema)
{
    ::substrait::Rel *project_rel, *join_rel;
    project_rel = rel;
    join_rel = project_rel->mutable_project()->mutable_input();

    *join_rel->mutable_join()->mutable_left() = *left_rel;
    *join_rel->mutable_join()->mutable_right() = *right_rel;

    vector<int> left_map, right_map;
    auto join_out_schema = equalJoin(
        join_rel, left_schema, right_schema, tuple_id_name,
        tuple_id_name, ::substrait::JoinRel_JoinType_JOIN_TYPE_OUTER,
        left_map, right_map);

    // project rel to merge attributes after join
    unordered_map<string, pair<int, int>> left_right_idx;
    for (int i = 0; i < left_schema->size(); i++)
    {
        string name = left_schema->get(i)->getName();
        if (left_right_idx.count(name) == 0)
            left_right_idx[name] = make_pair(-1, -1);
        left_right_idx[name].first = i;
    }
    for (int i = 0; i < right_schema->size(); i++)
    {
        string name = right_schema->get(i)->getName();
        if (left_right_idx.count(name) == 0)
            left_right_idx[name] = make_pair(-1, -1);
        left_right_idx[name].second = i;
    }

    vector<shared_ptr<const Expression>> project_expression;
    for (auto it = left_right_idx.begin(); it != left_right_idx.end();
         it++)
    {
        const auto &idx = it->second;
        if (idx.first == -1)
        {
            project_expression.push_back(
                join_out_schema->get(right_map[idx.second]));
        }
        else if (idx.second == -1)
        {
            project_expression.push_back(
                join_out_schema->get(left_map[idx.first]));
        }
        else
        {
            // both left and right table has the attribute but only one
            // is valid
            auto left_attr = join_out_schema->get(left_map[idx.first]);
            auto right_attr =
                join_out_schema->get(right_map[idx.second]);
            if (it->first == valid_attribute_name ||
                it->first == passed_pred_name ||
                it->first == direct_measures_name ||
                it->first == possible_measures_name)
                project_expression.push_back(
                    make_shared<FunctionExpression>(
                        "left_" + it->first, "bitmap_or_scalar",
                        vector<shared_ptr<const Expression>>{
                            left_attr, right_attr},
                        DATA_TYPE::FIXEDBINARY, false));
            else
            {
                auto bitmaptGetExp = makeBitmapGet(
                    "left_" + valid_attribute_name, join_out_schema,
                    table_schema->getOffset(it->first));

                auto isValidExp =
                    checkValid("is_valid_exp",
                               join_out_schema->get(
                                   "left_" + valid_attribute_name));
                auto checkExp = FunctionExpression::connectExpression(
                    "if_check",
                    vector<shared_ptr<const Expression>>{isValidExp,
                                                         bitmaptGetExp},
                    false, true);

                project_expression.push_back(
                    make_shared<IfFunctionExpression>(
                        "left_" + it->first, checkExp, left_attr,
                        right_attr));
            }
        }
    }
    auto project_out_schema =
        project(project_rel, project_expression, join_out_schema);
    // rename the attributes to remove prefix
    for (int i = 0; i < project_out_schema->size(); i++)
    {
        string name = project_out_schema->get(i)->getName();
        string dest_name = name.substr(name.find('_') + 1);
        project_out_schema->rename(name, dest_name);
    }
    return project_out_schema;
}

int findLargestAttribute(
    const list<pair<shared_ptr<const ScanParameter>, int64_t>> &active,
    shared_ptr<const Schema> table_schema)
{
    unordered_map<int, int64_t> tnum;
    for (auto it = active.begin(); it != active.end(); it++)
    {
        const auto &attr = it->first->read_attributes;
        int64_t t = it->second;
        for (auto i = attr.find_first(); i != attr.npos;
             i = attr.find_next(i))
        {
            const string &name = table_schema->get(i)->getName();
            if (name != tuple_id_name && name != block_id_name)
                tnum[i] += t;
        }
    }

    auto max = tnum.begin();
    for (auto it = tnum.begin(); it != tnum.end(); it++)
        max = it->second > max->second ? it : max;
    return max->first;
}

unordered_set<shared_ptr<const ScanParameter>> extractBlocks(
    list<pair<shared_ptr<const ScanParameter>, int64_t>> &active,
    int attribute_id)
{
    unordered_set<shared_ptr<const ScanParameter>> ans;
    for (auto it = active.begin(); it != active.end();)
    {
        if (it->first->read_attributes.test(attribute_id))
        {
            ans.insert(it->first);
            it = active.erase(it);
        }
        else
            it++;
    }
    return ans;
}

shared_ptr<Expression> checkAttributes(
    const unordered_set<shared_ptr<const ScanParameter>> &future_blocks,
    shared_ptr<const Query> query,
    shared_ptr<const Schema> input_schema,
    shared_ptr<const Schema> table_schema)
{
    static unordered_set<int> checked_measures;

    unordered_set<string> future_attribtues;
    for (const auto b : future_blocks)
        for (auto i = b->project_attributes.find_first();
             i != b->project_attributes.npos;
             i = b->project_attributes.find_next(i))
            future_attribtues.insert(table_schema->get(i)->getName());

    vector<shared_ptr<const Expression>> filter_exp;
    for (int i = 0; i < query->numOfMeasures(); i++)
    {
        auto attr = query->attributesInMeasure(i);
        boost::dynamic_bitset<> attr_bitmap(table_schema->size(),
                                            false);
        bool in_future = false;
        for (string a : *attr)
        {
            attr_bitmap.set(table_schema->getOffset(a), true);
            if (future_attribtues.count(a) != 0)
                in_future = true;
        }
        if (in_future || checked_measures.count(i))
            break;

        // the future blocks does not have attributes referred in the
        // measure. We should check the validity of the measure
        checked_measures.insert(i);

        // the measure can be in the direct path
        auto check_direct =
            makeBitmapGet(direct_measures_name, input_schema, i);

        // all attributes or non attributes in the measure are valid
        auto bitmap_and_exp = make_shared<FunctionExpression>(
            "bitmap_and_exp", "bitmap_and_scalar",
            vector<shared_ptr<const Expression>>{
                make_shared<Literal>(
                    "expect_same",
                    make_shared<FixedBinary>(attr_bitmap)),
                input_schema->get(valid_attribute_name)},
            DATA_TYPE::FIXEDBINARY, false);
        auto bitmap_count_exp = make_shared<FunctionExpression>(
            "bitmap_count_exp", "bitmap_count",
            vector<shared_ptr<const Expression>>{bitmap_and_exp},
            DATA_TYPE::INTEGER, false);

        auto zero_exp = make_shared<FunctionExpression>(
            "equal_zero", "equal",
            vector<shared_ptr<const Expression>>{
                bitmap_count_exp,
                make_shared<Literal>("zero",
                                     make_shared<Integer>(0, 32))},
            DATA_TYPE::BOOLEAN, false);
        auto all_valid_exp = make_shared<FunctionExpression>(
            "equal_all_valid", "equal",
            vector<shared_ptr<const Expression>>{
                bitmap_count_exp,
                make_shared<Literal>(
                    "all", make_shared<Integer>(attr->size(), 32))},
            DATA_TYPE::BOOLEAN, false);
        filter_exp.push_back(FunctionExpression::connectExpression(
            "check_measure_" + to_string(i),
            vector<shared_ptr<const Expression>>{check_direct, zero_exp,
                                                 all_valid_exp},
            false, false));
    }
    if (filter_exp.size() == 0)
        return nullptr;
    return FunctionExpression::connectExpression(
        "filter_by_valid", filter_exp, false, true);
}

shared_ptr<Schema> makeJoinSequenceRecursive(
    ::substrait::Rel *rel,
    list<pair<shared_ptr<const ScanParameter>, int64_t>> active,
    unordered_set<shared_ptr<const ScanParameter>> finished,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query, bool filter_tuples)
{
    int attribute = findLargestAttribute(active, table_schema);
    auto blocks = extractBlocks(active, attribute);

    auto finished_next = finished;
    finished_next.insert(blocks.begin(), blocks.end());

    ::substrait::Rel join_left_rel;
    auto join_left_schema =
        readMiniTable(&join_left_rel, blocks, table_schema);

    ::substrait::Rel join_rel;
    shared_ptr<Schema> schema_after_join;

    if (active.size() == 0)
    {
        join_rel = join_left_rel;
        schema_after_join = join_left_schema;
    }
    else
    {
        ::substrait::Rel join_right_rel;
        auto join_right_schema = makeJoinSequenceRecursive(
            &join_right_rel, active, finished_next, table_schema, query,
            filter_tuples);
        schema_after_join = makeJoinRel(
            &join_rel, join_left_schema, &join_left_rel,
            join_right_schema, &join_right_rel, table_schema);
    }

    if (filter_tuples)
    {
        shared_ptr<Expression> filter_exp = checkAttributes(
            finished, query, schema_after_join, table_schema);
        if (filter_exp)
        {
            *rel->mutable_filter()->mutable_input() = join_rel;
            return filter(rel, filter_exp, schema_after_join);
        }
    }
    *rel = join_rel;
    return schema_after_join;
}

shared_ptr<Schema> makeJoinSequence(
    ::substrait::Rel *rel,
    const vector<shared_ptr<const ScanParameter>> &blocks,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query, bool filter_tuples)
{
    list<pair<shared_ptr<const ScanParameter>, int64_t>> active;
    unordered_set<shared_ptr<const ScanParameter>> finished;

    for (auto b : blocks)
    {
        int64_t tnum = 0;
        for (auto i : b->blocks)
        {
            int64_t t = i->estimateRowNum(*b->filter_boundary);
            if (t == -1)
                throw Exception("makeJoinSequence: unknown block size");
            tnum += t;
        }
        active.push_back(make_pair(b, tnum));
    }

    return makeJoinSequenceRecursive(
        rel, active, finished, table_schema, query, filter_tuples);
}