#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include "produce_plan/join_sequence.h"
#include "produce_plan/make_plan.h"

shared_ptr<Schema> unionReconsMeasure(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<vector<shared_ptr<const ScanParameter>>>
        &recons_measure_params)
{
    assert(recons_measure_params.size() > 1);
    substrait::Rel *exchange_rel = nullptr, *union_rel = rel;
    if (InputParameter::get()->engine == InputParameter::Engine::Velox)
    {
        exchange_rel = rel;
        union_rel = exchange_rel->mutable_exchange()->mutable_input();
    }

    unordered_set<string> union_attributes = {
        valid_attribute_name, tuple_id_name, passed_pred_name,
        direct_measures_name};
    shared_ptr<Schema> schema_before_union;
    for (const auto &p : recons_measure_params)
        for (auto i : p)
        {
            for (int a = i->project_attributes.find_first();
                 a != i->project_attributes.npos;
                 a = i->project_attributes.find_next(a))
                union_attributes.insert(
                    table_schema->get(a)->getName());
        }
    for (const auto &params : recons_measure_params)
    {
        auto project_rel = union_rel->mutable_set()->add_inputs();
        auto schema_after_join = makeJoinSequence(
            project_rel->mutable_project()->mutable_input(), params,
            table_schema, query, true);

        vector<shared_ptr<const Expression>> project_expression;
        for (auto a : union_attributes)
        {
            if (schema_after_join->contains(a))
                project_expression.push_back(schema_after_join->get(a));
            else
            {
                // the missing attribute cannot be auxilary
                // attribute
                project_expression.push_back(make_shared<Literal>(
                    table_schema->get(a)->getName(),
                    table_schema->get(a)->getType()));
            }
        }
        auto t_schema =
            project(project_rel, project_expression, schema_after_join);
        if (!schema_before_union)
            schema_before_union = t_schema;
        else
            assert(schema_before_union->equal(t_schema));
    }
    auto schema_after_union = unionAll(union_rel, schema_before_union);

    if (!exchange_rel)
        return schema_after_union;

    return exchange(exchange_rel, schema_after_union,
                    {schema_after_union->get(tuple_id_name)});
}

shared_ptr<Schema> reconstructByJoin(
    ::substrait::Rel *rel, ::substrait::Rel *left_rel,
    shared_ptr<const Schema> left_schema, ::substrait::Rel *right_rel,
    shared_ptr<const Schema> right_schema,
    shared_ptr<const Query> query)
{
    ::substrait::Rel *filter_rel = rel;
    ::substrait::Rel *project_rel =
        filter_rel->mutable_filter()->mutable_input();
    ::substrait::Rel *join_rel =
        project_rel->mutable_project()->mutable_input();
    *join_rel->mutable_join()->mutable_left() = *left_rel;
    *join_rel->mutable_join()->mutable_right() = *right_rel;

    vector<int> left_map, right_map;
    auto join_out_schema = equalJoin(
        join_rel, left_schema, right_schema, tuple_id_name,
        tuple_id_name, ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT,
        left_map, right_map);

    vector<shared_ptr<const Expression>> project_expression;
    for (int i = 0; i < left_map.size(); i++)
    {
        string name = left_schema->get(i)->getName();
        auto left_attr = join_out_schema->get(left_map[i]);

        if (name == passed_pred_name || name == direct_measures_name ||
            name == possible_measures_name)
        {
            auto right_attr = join_out_schema->get(
                right_map[right_schema->getOffset(name)]);
            project_expression.push_back(
                make_shared<FunctionExpression>(
                    left_attr->getName(), "bitmap_or_scalar",
                    vector<shared_ptr<const Expression>>{left_attr,
                                                         right_attr},
                    DATA_TYPE::FIXEDBINARY, false));
        }
        else
            // the attribtue could be tuple_id, valid_attribute_id, or
            // attributes in the probe table
            project_expression.push_back(left_attr);
    }
    auto project_out_schema =
        project(project_rel, project_expression, join_out_schema);
    for (int i = 0; i < project_out_schema->size(); i++)
    {
        string name = project_out_schema->get(i)->getName();
        string dest_name = name.substr(name.find('_') + 1);
        project_out_schema->rename(name, dest_name);
    }

    // filter tuples that do not pass all predicates
    int pred_count =
        query->getFilter()->getSubExpressions("and").size();
    auto bitmap_count_exp = make_shared<FunctionExpression>(
        "bitmap_count_exp", "bitmap_count",
        vector<shared_ptr<const Expression>>{
            project_out_schema->get(passed_pred_name)},
        DATA_TYPE::INTEGER, false);
    auto equal_exp = make_shared<FunctionExpression>(
        "equal_exp", "equal",
        vector<shared_ptr<const Expression>>{
            bitmap_count_exp,
            make_shared<Literal>("expect_count",
                                 make_shared<Integer>(pred_count, 32))},
        DATA_TYPE::BOOLEAN, false);
    return filter(filter_rel, equal_exp, project_out_schema);
}

shared_ptr<Schema> evaluateMeasure(
    ::substrait::Rel *rel, ::substrait::Rel *input_rel,
    shared_ptr<const Schema> input_schema,
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema)
{
    *rel->mutable_project()->mutable_input() = *input_rel;

    vector<shared_ptr<const Expression>> project_expression;
    const auto &all_measures = query->getMeasures();
    for (int i = 0; i < all_measures.size(); i++)
    {
        auto default_value = make_shared<Literal>(
            all_measures[i]->getName(), all_measures[i]->getType());

        auto attributes_in_measures = all_measures[i]->getAttributes();

        // check if all referred attributes are valid
        bool all_exist = true;
        boost::dynamic_bitset<> attributes_offset(table_schema->size(),
                                                  false);
        for (const string &a : attributes_in_measures)
        {
            all_exist &= input_schema->contains(a);
            attributes_offset.set(table_schema->getOffset(a));
        }
        if (!all_exist)
        {
            project_expression.push_back(default_value);
            continue;
        }
        auto bitmap_and_exp = make_shared<FunctionExpression>(
            "bitmap_and_" + to_string(i), "bitmap_and_scalar",
            vector<shared_ptr<const Expression>>{
                input_schema->get(valid_attribute_name),
                make_shared<Literal>(
                    "expect_valid",
                    make_shared<FixedBinary>(attributes_offset))},
            DATA_TYPE::FIXEDBINARY, false);
        auto bitmap_count_exp = make_shared<FunctionExpression>(
            "bitmap_count_" + to_string(i), "bitmap_count",
            vector<shared_ptr<const Expression>>{bitmap_and_exp},
            DATA_TYPE::INTEGER, false);
        shared_ptr<Expression> check_valid_exp =
            make_shared<FunctionExpression>(
                "check_valid_" + to_string(i), "equal",
                vector<shared_ptr<const Expression>>{
                    bitmap_count_exp,
                    make_shared<Literal>(
                        "expect_count",
                        make_shared<Integer>(
                            attributes_in_measures.size(), 32))},
                DATA_TYPE::BOOLEAN, false);

        // check if the measure is evaluated directly
        shared_ptr<Expression> check_direct_exp =
            make_shared<FunctionExpression>(
                "check_direct_" + to_string(i), "not",
                vector<shared_ptr<const Expression>>{makeBitmapGet(
                    direct_measures_name, input_schema, i)},
                DATA_TYPE::BOOLEAN, false);

        auto eval_exp = make_shared<IfFunctionExpression>(
            all_measures[i]->getName(),
            FunctionExpression::connectExpression(
                "check_" + to_string(i),
                {check_valid_exp, check_direct_exp}, false, true),
            all_measures[i]->getChildren()[0], default_value);
        project_expression.push_back(eval_exp);
    }
    return project(rel, project_expression, input_schema);
}

shared_ptr<Schema> reconstructPath(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &recons_filter_params,
    const vector<vector<shared_ptr<const ScanParameter>>>
        &recons_measure_params)
{
    auto project_rel = rel;
    auto join_rel = project_rel->mutable_project()->mutable_input();
    shared_ptr<Schema> schema_after_join;

    // union the probe table
    assert(recons_measure_params.size() > 0);
    ::substrait::Rel join_left_rel;
    shared_ptr<Schema> join_left_schema;
    if (recons_measure_params.size() == 1)
        join_left_schema =
            makeJoinSequence(&join_left_rel, recons_measure_params[0],
                             table_schema, query, true);
    else
        join_left_schema = unionReconsMeasure(
            &join_left_rel, table_schema, query, recons_measure_params);

    // join the probe table with the filter table
    if (recons_filter_params.size() == 0)
    {
        *join_rel = join_left_rel;
        schema_after_join = join_left_schema;
    }
    else
    {
        // build the filter table
        ::substrait::Rel join_right_rel;
        auto join_right_schema =
            makeJoinSequence(&join_right_rel, recons_filter_params,
                             table_schema, query, false);
        schema_after_join = reconstructByJoin(
            join_rel, &join_left_rel, join_left_schema, &join_right_rel,
            join_right_schema, query);
    }

    // evaluate the measures
    return evaluateMeasure(project_rel, join_rel, schema_after_join,
                           query, table_schema);
}

shared_ptr<Schema> evalauteJoinPlan(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>>
        &unmerged_direct_params,
    const vector<shared_ptr<const ScanParameter>>
        &unmerged_recons_filter_params,
    const vector<vector<shared_ptr<const ScanParameter>>>
        &unmerged_recons_measure_params)
{
    // merge params
    auto direct_params = mergeBeforeRead(unmerged_direct_params);
    auto recons_filter_params =
        mergeBeforeRead(unmerged_recons_filter_params);
    vector<vector<shared_ptr<const ScanParameter>>>
        recons_measure_params;
    for (const auto &p : unmerged_recons_measure_params)
        recons_measure_params.push_back(mergeBeforeRead(p));

    // print input plan
    cout << table_schema->toString() << endl;
    cout << query->toString() << endl;
    printf("*************Direct parameters (%zu params)*************\n",
           direct_params.size());
    for (auto p : direct_params)
        cout << p->toString() << endl;
    printf("*************Reconstruct filter parameters (%zu "
           "params)*************\n",
           recons_filter_params.size());
    for (auto p : recons_filter_params)
        cout << p->toString() << endl;
    printf("*************Reconstruct measure parameters (%zu "
           "params)*************\n",
           recons_measure_params.size());
    for (int i = 0; i < recons_measure_params.size(); i++)
    {
        cout << "*************Group " << i << "*************" << endl;
        for (auto p : recons_measure_params[i])
            cout << p->toString() << endl;
    }

    shared_ptr<Schema> schema_out_path;
    substrait::Rel reconstruct_rel;
    shared_ptr<Schema> schema_after_reconstruct;
    if (recons_measure_params.size() > 0)
    {
        schema_after_reconstruct = reconstructPath(
            &reconstruct_rel, table_schema, query, recons_filter_params,
            recons_measure_params);
        assert(schema_after_reconstruct->size() ==
               query->getMeasures().size());
        schema_out_path = schema_after_reconstruct;
    }

    substrait::Rel direct_rel;
    shared_ptr<Schema> schema_after_direct;
    if (direct_params.size() > 0)
    {
        schema_after_direct = directEvalPath(&direct_rel, table_schema,
                                             direct_params, query);
        assert(schema_after_direct->size() ==
               query->getMeasures().size());
        schema_out_path = schema_after_direct;
    }

    if (schema_after_direct && schema_after_reconstruct)
        assert(schema_after_direct->equal(schema_after_reconstruct));

    if (!schema_out_path)
        return nullptr;

    auto agg_rel = rel;
    auto union_rel = agg_rel->mutable_aggregate()->mutable_input();
    ::substrait::Rel *exchange_rel = nullptr;
    if (InputParameter::get()->engine == InputParameter::Engine::Velox)
    {
        exchange_rel = agg_rel->mutable_aggregate()->mutable_input();
        union_rel = exchange_rel->mutable_exchange()->mutable_input();
    }

    auto schema_after_union = unionAll(union_rel, schema_out_path);
    if (schema_after_reconstruct)
        *union_rel->mutable_set()->add_inputs() = reconstruct_rel;
    if (schema_after_direct)
        *union_rel->mutable_set()->add_inputs() = direct_rel;

    if (exchange_rel)
        schema_after_union =
            exchange(exchange_rel, schema_after_union, {});

    // evaluate aggregation function
    vector<shared_ptr<const AggregateExpression>>
        measures_after_projection;
    for (int i = 0; i < query->getMeasures().size(); i++)
    {
        auto m = query->getMeasures()[i];
        measures_after_projection.push_back(
            make_shared<const AggregateExpression>(
                m->getName(), m->getFunction(),
                vector<shared_ptr<const Expression>>{
                    schema_after_union->get(i)},
                m->getType(), m->getNullable()));
    }
    auto schema_after_agg =
        aggregate(agg_rel, schema_after_union,
                  measures_after_projection, nullptr);
    return schema_after_agg;
}