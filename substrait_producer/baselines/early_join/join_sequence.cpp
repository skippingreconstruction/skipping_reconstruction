#include "baselines/early_join/join_sequence.h"
#include "baselines/make_plan_base.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include <list>

MiniTable::MiniTable(
    shared_ptr<const Schema> table_schema,
    const unordered_set<shared_ptr<const ScanParameter>> &blocks)
    : table_schema(table_schema)
{
    mini_schema = make_shared<Schema>();
    for (auto b : blocks)
        addDataBlock(std::move(b));
}

void MiniTable::addDataBlock(shared_ptr<const ScanParameter> block)
{
    data_blocks.push_back(block);
    for (auto i = block->project_attributes.find_first();
         i != block->project_attributes.npos;
         i = block->project_attributes.find_next(i))
    {
        if (!mini_schema->contains(table_schema->get(i)->getName()))
            mini_schema->add(table_schema->get(i));
    }
}

shared_ptr<Schema> MiniTable::makeSubstraitRel(
    ::substrait::Rel *rel) const
{
    boost::dynamic_bitset<> requested_attributes =
        table_schema->getOffsets(mini_schema);
    if (data_blocks.size() == 0)
        throw Exception("MiniTable::makeSubstraitRel: mini table must "
                        "have at least one data block");
    else if (data_blocks.size() == 1)
        return readBlocks(rel, data_blocks[0], table_schema,
                          requested_attributes);
    else
    {
        ::substrait::Rel *union_rel, *exchange_rel;
        if (InputParameter::get()->engine ==
            InputParameter::Engine::Arrow)
        {
            union_rel = rel;
            exchange_rel = nullptr;
        }
        else
        {
            exchange_rel = rel;
            union_rel =
                exchange_rel->mutable_exchange()->mutable_input();
        }

        shared_ptr<Schema> union_schema;
        for (auto b : data_blocks)
            union_schema =
                readBlocks(union_rel->mutable_set()->add_inputs(), b,
                           table_schema, requested_attributes);
        auto schema_after_uion = unionAll(union_rel, union_schema);

        if (exchange_rel)
            schema_after_uion =
                exchange(exchange_rel, schema_after_uion,
                         {schema_after_uion->get(tuple_id_name)});

        return schema_after_uion;
    }
}

string MiniTable::toString() const
{
    string ans = "MiniTable{\n" + mini_schema->toString() + "\n";
    for (auto block : data_blocks)
    {
        ans += block->toString() + "\n";
    }
    ans += "}";
    return ans;
}

shared_ptr<Schema> JoinParameter::makeSubstraitRel(
    ::substrait::Rel *rel, shared_ptr<const Schema> table_schema) const
{
    auto project_rel = rel;
    auto join_rel = project_rel->mutable_project()->mutable_input();

    shared_ptr<Schema> right_schema;
    if (std::get_if<shared_ptr<MiniTable>>(&child_right))
        right_schema =
            std::get<shared_ptr<MiniTable>>(child_right)
                ->makeSubstraitRel(
                    join_rel->mutable_join()->mutable_right());
    else
        right_schema =
            std::get<shared_ptr<FilterParameter>>(child_right)
                ->makeSubstraitRel(
                    join_rel->mutable_join()->mutable_right(),
                    table_schema);

    auto left_schema = child_left->makeSubstraitRel(
        join_rel->mutable_join()->mutable_left());

    vector<int> left_map, right_map;
    auto join_out_schema = equalJoin(
        join_rel, left_schema, right_schema, tuple_id_name,
        tuple_id_name,
        ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER,
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
            // if (it->first == tuple_id_name)
            //     project_expression.push_back(
            //         join_out_schema->get(left_map[idx.first]));
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

string JoinParameter::toString() const
{
    string ans = "Join:{\nbuild->";
    if (std::get_if<shared_ptr<MiniTable>>(&child_right))
        ans +=
            std::get<shared_ptr<MiniTable>>(child_right)->toString() +
            "\n";
    else
        ans += std::get<shared_ptr<FilterParameter>>(child_right)
                   ->toString() +
               "\n";
    ans += "prob->" + child_left->toString() + "}\\END JOIN";
    return ans;
}

shared_ptr<Schema> FilterParameter::makeSubstraitRel(
    ::substrait::Rel *rel, shared_ptr<const Schema> table_schema) const
{
    if (!needFilter())
        return child->makeSubstraitRel(rel, table_schema);
    auto filter_rel = rel;
    auto join_rel = filter_rel->mutable_filter()->mutable_input();

    auto schema_after_join =
        child->makeSubstraitRel(join_rel, table_schema);

    vector<shared_ptr<const Expression>> filter_exp;
    for (auto i = expect_valid.find_first(); i != expect_valid.npos;
         i = expect_valid.find_next(i))
        filter_exp.push_back(
            makeBitmapGet(valid_attribute_name, schema_after_join, i));

    // expect_same
    for (auto it = expect_same.begin(); it != expect_same.end(); it++)
    {
        auto check_valid = makeBitmapGet(valid_attribute_name,
                                         schema_after_join, it->first);

        // count(bitmap_and(expect_same, valid_attribute)) == 0
        auto expect_same_value = make_shared<Literal>(
            "expect_same", make_shared<FixedBinary>(it->second));
        auto bitmap_and_exp = make_shared<FunctionExpression>(
            "bitmap_and_exp", "bitmap_and_scalar",
            vector<shared_ptr<const Expression>>{
                expect_same_value,
                schema_after_join->get(valid_attribute_name)},
            DATA_TYPE::FIXEDBINARY, false);
        auto bitmap_count_exp = make_shared<FunctionExpression>(
            "bitmap_count_exp", "bitmap_count",
            vector<shared_ptr<const Expression>>{bitmap_and_exp},
            DATA_TYPE::INTEGER, false);
        auto equal_exp = make_shared<FunctionExpression>(
            "equal_exp", "equal",
            vector<shared_ptr<const Expression>>{
                bitmap_count_exp,
                make_shared<Literal>("zero",
                                     make_shared<Integer>(0, 32))},
            DATA_TYPE::BOOLEAN, false);

        filter_exp.push_back(make_shared<FunctionExpression>(
            "expect_same_check_" + std::to_string(it->first), "or",
            vector<shared_ptr<const Expression>>{check_valid,
                                                 equal_exp},
            DATA_TYPE::BOOLEAN, false));
    }

    auto connect_filter_exp = FunctionExpression::connectExpression(
        "filter_by_valid", filter_exp, false, true);

    return filter(filter_rel, connect_filter_exp, schema_after_join);
}

string FilterParameter::toString() const
{
    string ans = "Filter:{\n" + child->toString() + "\n";

    auto bitset_to_string =
        [&](const boost::dynamic_bitset<> &bitset) -> string {
        size_t size = bitset.size();
        char c_str[size + 1] = {0};
        for (size_t i = 0; i < size; i++)
            if (bitset.test(i))
                c_str[i] = '1';
            else
                c_str[i] = '0';
        return string(c_str);
    };
    ans += "expect_valid: " + bitset_to_string(expect_valid) + "\n";
    ans += "expect_same:{\n";
    for (auto it = expect_same.begin(); it != expect_same.end(); it++)
        ans += std::to_string(it->first) + ": " +
               bitset_to_string(it->second) + "\n";
    ans += "}\n}\\END FILTER";
    return ans;
}

/**
 * @brief Find the attribute that has the most tuples in the active set
 *
 * @param active the set of data blocks that has not been processed
 * @param query_attributes all attributes in the query, except for the
 * auxilary attribute
 * @return int the attribute id
 */
int findLargestAttribute(
    const list<pair<shared_ptr<const ScanParameter>, int64_t>> &active,
    const boost::dynamic_bitset<> &query_attributes)
{
    unordered_map<int, int64_t> tnum;
    for (auto it = active.begin(); it != active.end(); it++)
    {
        const auto &proj_attr = it->first->project_attributes;
        int64_t t = it->second;
        for (auto i = proj_attr.find_first(); i != proj_attr.npos;
             i = proj_attr.find_next(i))
        {
            if (query_attributes.test(i))
                tnum[i] += t;
        }
    }

    auto max = tnum.begin();
    for (auto it = tnum.begin(); it != tnum.end(); it++)
        if (max->second < it->second)
            max = it;
    return max->first;
}

/**
 * @brief Extract the data blocks that contain an attribute and remove
 * these blocks from the active set
 *
 * @param active : IN/OUT. The set of blocks that have not been
 * processed. Remove blocks from the set after the function
 * @param attribute_id the attribute to extract
 * @return unordered_set<shared_ptr<const ScanParamter>>
 */
unordered_set<shared_ptr<const ScanParameter>> extractBlocks(
    list<pair<shared_ptr<const ScanParameter>, int64_t>> &active,
    int attribute_id)
{
    unordered_set<shared_ptr<const ScanParameter>> ans;
    for (auto it = active.begin(); it != active.end();)
    {
        if (it->first->project_attributes.test(attribute_id))
        {
            ans.insert(it->first);
            it = active.erase(it);
        }
        else
            it++;
    }
    return ans;
}

/**
 * @brief check attributes if null after join
 *
 * @param finished the data blocks that has been processed in the
 * ancestor nodes
 * @param query_attributes the attributes in the query, except for
 * auxilary attributes
 * @param expect_valid OUT: attributes cannot be null after the join
 * @param expect_same OUT: attributes that should all be null or valid
 */
void checkAttributes(
    const unordered_set<shared_ptr<const ScanParameter>> &finished,
    const boost::dynamic_bitset<> &query_attributes,
    boost::dynamic_bitset<> &expect_valid,
    unordered_map<int, boost::dynamic_bitset<>> &expect_same)
{
    unordered_map<int, boost::dynamic_bitset<>> occurence;
    for (auto i = query_attributes.find_first();
         i != query_attributes.npos; i = query_attributes.find_next(i))
        occurence[i].resize(finished.size(), false);

    int idx = 0;
    for (auto it = finished.begin(); it != finished.end(); it++, idx++)
    {
        const auto &proj = (*it)->project_attributes;
        for (auto i = proj.find_first(); i != proj.npos;
             i = proj.find_next(i))
        {
            if (query_attributes.test(i))
                occurence[i].set(idx);
        }
    }

    expect_valid.resize(query_attributes.size());
    expect_valid.reset();
    expect_same.clear();
    for (auto it = occurence.begin(); it != occurence.end(); it++)
    {
        if (it->second.none())
            expect_valid.set(it->first);
        else
            for (auto it1 = occurence.begin(); it1 != occurence.end();
                 it1++)
            {
                if (it == it1)
                    continue;
                if (it->second.is_subset_of(it1->second))
                {
                    if (expect_same.count(it->first) == 0)
                        expect_same[it->first] =
                            boost::dynamic_bitset<>(
                                query_attributes.size(), false);
                    expect_same[it->first].set(it1->first);
                }
            }
    }
}

variant<shared_ptr<MiniTable>, shared_ptr<FilterParameter>>
makeJoinSequenceRecursive(
    list<pair<shared_ptr<const ScanParameter>, int64_t>> active,
    unordered_set<shared_ptr<const ScanParameter>> finished,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &query_attributes)
{
    int attribute = findLargestAttribute(active, query_attributes);
    const auto &mini_table_blocks = extractBlocks(active, attribute);
    auto finished_next = finished;
    finished_next.insert(mini_table_blocks.begin(),
                         mini_table_blocks.end());

    shared_ptr<MiniTable> mini_table =
        make_shared<MiniTable>(table_schema, mini_table_blocks);
    if (active.size() == 0)
        return mini_table;
    shared_ptr<JoinParameter> join_node = make_shared<JoinParameter>();
    join_node->child_left = mini_table;
    join_node->child_right = makeJoinSequenceRecursive(
        active, finished_next, table_schema, query_attributes);

    shared_ptr<FilterParameter> filter_node =
        make_shared<FilterParameter>();
    filter_node->child = join_node;
    checkAttributes(finished, query_attributes,
                    filter_node->expect_valid,
                    filter_node->expect_same);

    return filter_node;
}

variant<shared_ptr<MiniTable>, shared_ptr<FilterParameter>>
makeJoinSequence(
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const ScanParameter>> &scan_parameters)
{
    boost::dynamic_bitset<> query_attributes(table_schema->size());
    list<pair<shared_ptr<const ScanParameter>, int64_t>> active;
    unordered_set<shared_ptr<const ScanParameter>> finished;
    for (auto p : scan_parameters)
    {
        int64_t tnum = 0;
        for (auto b : p->blocks)
        {
            int64_t i = b->estimateRowNum(*p->filter_boundary);
            if (i == -1)
                throw Exception("makeJoinSequence: unknown block size");
            tnum += i;
        }
        active.push_back(make_pair(p, tnum));
        query_attributes |= p->project_attributes;
    }

    for (auto i = query_attributes.find_first();
         i != query_attributes.npos; i = query_attributes.find_next(i))
        if (table_schema->get(i)->getName() == block_id_name ||
            table_schema->get(i)->getName() == tuple_id_name)
            query_attributes.reset(i);

    return makeJoinSequenceRecursive(
        active, unordered_set<shared_ptr<const ScanParameter>>{},
        table_schema, query_attributes);
}
