#include "configuration.h"
#include "metadata/expression.h"
#include "metadata/query.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include "produce_plan/make_plan.h"
#include "substrait/plan.pb.h"
#include <boost/dynamic_bitset.hpp>
#include <iostream>

/**
 * @brief Reconstruct tuples. Filter tuples out if the tuple does not
 * pass all predicates. Evaluate expression in all measures. Attributes
 * that a measure uses should be valid in the tuple. The measure should
 * not be directly evaluated in the fast path.
 *
 * @param rel the input algebra
 * @param input_rel (out parameter) the output algebra
 * @param input_schema the schema before tuple reconstruction
 * @param table_schema the schema of the source table
 * @param filters filters in the query
 * @param all_measures all aggregation functions in the query
 * @return shared_ptr<Schema> the schema after reconstruction, filter
 * and projection
 */
shared_ptr<Schema> reconstruct(
    substrait::Rel *rel, substrait::Rel *input_rel,
    shared_ptr<const Schema> input_schema,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const Expression>> &filters,
    const vector<shared_ptr<const AggregateExpression>> &all_measures)
{
    substrait::Rel *exchange_rel = nullptr, *agg_rel = nullptr,
                   *filter_rel = nullptr, *project_rel = nullptr,
                   *union_rel = nullptr;

    if (InputParameter::get()->parallel ==
        InputParameter::AggParallelMethod::Partition)
    {
        union_rel = rel;
        project_rel = union_rel->mutable_set()->add_inputs();
        filter_rel = project_rel->mutable_project()->mutable_input();
        agg_rel = filter_rel->mutable_filter()->mutable_input();
        exchange_rel = agg_rel->mutable_aggregate()->mutable_input();

        exchange_rel->mutable_exchange()->set_allocated_input(
            input_rel);
        vector<shared_ptr<const Attribute>> scatter_attributes = {
            input_schema->get(tuple_id_name)};
        exchange(exchange_rel, input_schema, scatter_attributes);
    }
    else
    {
        project_rel = rel;
        filter_rel = project_rel->mutable_project()->mutable_input();
        agg_rel = filter_rel->mutable_filter()->mutable_input();
        agg_rel->mutable_aggregate()->set_allocated_input(input_rel);
    }

    // reconstruct tuples by aggregation
    vector<shared_ptr<const AggregateExpression>> measures;
    for (int i = 0; i < input_schema->size(); i++)
    {
        auto attr = input_schema->get(i);
        string name = attr->getName();
        vector<shared_ptr<const Expression>> expression{attr};
        DATA_TYPE type = attr->getType();
        string op;
        if (name == tuple_id_name || name == block_id_name)
            continue;
        else if (name == passed_pred_name ||
                 name == direct_measures_name ||
                 //  name == possible_measures_name ||
                 name == valid_attribute_name)
            op = "bitmap_or";
        else
            op = "reconstruct";

        measures.push_back(make_shared<const AggregateExpression>(
            name, op, expression, type, true));
    }
    shared_ptr<Expression> group = input_schema->get(tuple_id_name);
    auto schema_after_agg =
        aggregate(agg_rel, input_schema, measures, group);

    // filter tuples
    vector<shared_ptr<const Expression>> new_filters;
    for (int i = 0; i < filters.size(); i++)
    {
        auto attribute_names = filters[i]->getAttributes();
        if (attribute_names.size() == 0)
            // the filter only contains literals
            new_filters.push_back(filters[i]);
        else if (attribute_names.size() == 1)
        {
            auto passed_pred_exp =
                makeBitmapGet(passed_pred_name, schema_after_agg, i);

            string name = *attribute_names.begin();
            if (!schema_after_agg->contains(name))
                continue;

            int attr_in_table = table_schema->getOffset(name);
            auto valid_attr_exp = makeBitmapGet(
                valid_attribute_name, schema_after_agg, attr_in_table);
            shared_ptr<IfFunctionExpression> valid_check_if =
                make_shared<IfFunctionExpression>(
                    filters[i]->getName(), valid_attr_exp, filters[i],
                    make_shared<Literal>("error",
                                         make_shared<Boolean>(false)));
            new_filters.push_back(make_shared<IfFunctionExpression>(
                filters[i]->getName(), passed_pred_exp,
                make_shared<Literal>("passed",
                                     make_shared<Boolean>(true)),
                valid_check_if));
        }
        else
            throw Exception("Each atom filter can not contain two or "
                            "more attributes");
    }

    auto schema_after_filter = schema_after_agg;
    if (new_filters.size() > 0)
    {
        auto new_filter = FunctionExpression::connectExpression(
            "filter_expression", new_filters, false, "and");
        schema_after_filter =
            filter(filter_rel, new_filter, schema_after_agg);
    }
    else
    {
        // skip the filter operator
        project_rel->mutable_project()->set_allocated_input(agg_rel);
    }

    // project and evaluate measures
    auto check_attributes = [&](const vector<int> &offsets,
                                shared_ptr<const Schema> current_schema)
        -> shared_ptr<FunctionExpression> {
        vector<shared_ptr<const Expression>> exps;
        for (auto i : offsets)
            exps.push_back(
                makeBitmapGet(valid_attribute_name, current_schema, i));
        return FunctionExpression::connectExpression(
            "check_valid_attributs", exps, false, "and");
    };
    vector<shared_ptr<const Expression>> project_expression;
    for (int i = 0; i < all_measures.size(); i++)
    {
        auto check_measure_exp =
            makeBitmapGet(direct_measures_name, schema_after_filter, i);
        auto default_value = make_shared<Literal>(
            all_measures[i]->getName(), all_measures[i]->getType());

        auto attributes_in_measure = all_measures[i]->getAttributes();
        vector<int> attribute_offset;
        bool all_exist = true;
        for (auto &name : attributes_in_measure)
        {
            attribute_offset.push_back(table_schema->getOffset(name));
            all_exist &= schema_after_filter->contains(name);
        }
        if (!all_exist)
        {
            // the reconstruction path does contain all attributes in
            // the measure. The measure must cannot be evaluated in the
            // path so that we fill default nulls
            project_expression.push_back(default_value);
            continue;
        }

        auto check_valid_attributes =
            check_attributes(attribute_offset, schema_after_filter);

        if (all_measures[i]->getChildren().size() != 1)
            throw Exception("Measures must be unary");

        shared_ptr<IfFunctionExpression> inner_if =
            make_shared<IfFunctionExpression>(
                "evaluate_exp_in_measure_inner", check_valid_attributes,
                all_measures[i]->getChildren()[0], default_value);
        shared_ptr<IfFunctionExpression> outter_if =
            make_shared<IfFunctionExpression>(
                all_measures[i]->getName(), check_measure_exp,
                default_value, inner_if);
        project_expression.push_back(outter_if);
    }
    auto schema_after_project =
        project(project_rel, project_expression, schema_after_filter);

    if (InputParameter::get()->parallel ==
        InputParameter::AggParallelMethod::Partition)
        return unionAll(union_rel, schema_after_project);
    else
        return schema_after_project;
}

shared_ptr<Schema> reconstructPath(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const ScanParameter>> &scan_parameters,
    shared_ptr<const Query> query)
{
    // read data
    boost::dynamic_bitset<> reconstruct_attributes(
        table_schema->size());
    for (auto p : scan_parameters)
        reconstruct_attributes |= p->project_attributes;
    vector<substrait::Rel *> read_rel;
    shared_ptr<Schema> schema_after_read;
    for (auto p : scan_parameters)
    {
        auto rel = new substrait::Rel();
        auto s = readForReconstruction(rel, p, table_schema,
                                       reconstruct_attributes);
        if (!schema_after_read)
            schema_after_read = s;
        else if (!schema_after_read->equal(s))
            throw Exception("reconstructPath: inputs of union have "
                            "different schemas");
        read_rel.push_back(rel);
    }

    // union all
    substrait::Rel *union_rel = new substrait::Rel();
    auto schema_after_union = unionAll(union_rel, schema_after_read);
    for (auto r : read_rel)
    {
        *union_rel->mutable_set()->add_inputs() = *r;
        delete r;
    }

    // reconstruct
    vector<shared_ptr<const Expression>> query_filter_sub_exps;
    for (auto e : query->getFilter()->getSubExpressions("and"))
        query_filter_sub_exps.push_back(e);
    auto schema_after_reconstruct =
        reconstruct(rel, union_rel, schema_after_union, table_schema,
                    query_filter_sub_exps, query->getMeasures());
    return schema_after_reconstruct;
}

/**
 * @brief evaluate expressions in all aggregation measures after reading
 * data in direct_evaluation path.
 *
 * @param rel
 * @param input_rel
 * @param input_schema the schema after the union operator
 * @param all_measures all aggregation measures in the query
 * @return shared_ptr<Schema>
 */
shared_ptr<Schema> directEvaluation(
    substrait::Rel *rel, substrait::Rel *&input_rel,
    shared_ptr<Schema> input_schema,
    const vector<shared_ptr<const AggregateExpression>> &all_measures)
{
    input_rel = rel;
    vector<shared_ptr<const Expression>> project_expression;
    for (int i = 0; i < all_measures.size(); i++)
    {
        auto default_value = make_shared<Literal>(
            all_measures[i]->getName(), all_measures[i]->getType());

        auto attributes_in_measure = all_measures[i]->getAttributes();
        bool all_exists = true;
        for (auto &a : attributes_in_measure)
            all_exists &= input_schema->contains(a);
        if (!all_exists)
        {
            // fill null if the measure requires attributes not in the
            // direct path
            project_expression.push_back(default_value);
            continue;
        }

        if (all_measures[i]->getChildren().size() != 1)
            throw Exception("Measures must be unary");
        auto check_measure_exp =
            makeBitmapGet(direct_measures_name, input_schema, i);
        project_expression.push_back(make_shared<IfFunctionExpression>(
            all_measures[i]->getName(), check_measure_exp,
            all_measures[i]->getChildren()[0], default_value));
    }
    auto schema_after_project =
        project(rel, project_expression, input_schema);
    return schema_after_project;
}

shared_ptr<Schema> directEvalPath(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const ScanParameter>> &scan_parameters,
    shared_ptr<const Query> query)
{
    boost::dynamic_bitset<> direct_attributes(table_schema->size());
    int measure_num = query->numOfMeasures();
    for (int i = 0; i < measure_num; i++)
    {
        auto attrs = query->attributesInMeasure(i);
        for (auto a : *attrs)
            direct_attributes.set(table_schema->getOffset(a));
    }
    vector<substrait::Rel *> read_rel;
    shared_ptr<Schema> schema_after_read;
    for (auto p : scan_parameters)
    {
        auto rel = new substrait::Rel();
        auto s =
            readForDirectEval(rel, p, table_schema, direct_attributes);
        if (!schema_after_read)
            schema_after_read = std::move(s);
        else if (schema_after_read && !schema_after_read->equal(s))
            throw Exception("directEvalPath: inputs of union have "
                            "different schemas");
        read_rel.push_back(rel);
    }

    // union all
    substrait::Rel *union_rel = new substrait::Rel();
    auto schema_after_union = unionAll(union_rel, schema_after_read);
    for (auto r : read_rel)
    {
        *union_rel->mutable_set()->add_inputs() = *r;
        delete r;
    }

    // project
    substrait::Rel *project_input_rel;
    auto schema_after_project =
        directEvaluation(rel, project_input_rel, schema_after_union,
                         query->getMeasures());
    project_input_rel->mutable_project()->set_allocated_input(
        union_rel);

    return schema_after_project;
}

shared_ptr<Schema> evaluateAggregatePlan(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>>
        &unmerged_reconstruct_params,
    const vector<shared_ptr<const ScanParameter>>
        &unmerged_direct_params)
{
    // merge params
    auto reconstruct_params =
        mergeBeforeRead(unmerged_reconstruct_params);
    auto direct_params = mergeBeforeRead(unmerged_direct_params);

    // print input plan
    cout << table_schema->toString() << endl;
    cout << query->toString() << endl;
    printf("*************Reconstruct parameters (%zu "
           "params)*************\n",
           reconstruct_params.size());
    for (auto p : reconstruct_params)
        cout << p->toString() << endl;
    printf(
        "*************Direct parameters  (%zu params)*************\n",
        direct_params.size());
    for (auto p : direct_params)
        cout << p->toString() << endl;

    auto all_measures = query->getMeasures();
    shared_ptr<Schema> schema_out_path;

    substrait::Rel *reconstruct_rel = new substrait::Rel();
    shared_ptr<Schema> schema_after_reconstruct;
    if (reconstruct_params.size() > 0)
    {
        schema_after_reconstruct = reconstructPath(
            reconstruct_rel, table_schema, reconstruct_params, query);
        if (schema_after_reconstruct->size() != all_measures.size())
            throw Exception(
                "evaluate: out schema of reconstruction path must have "
                "the same number of attributes as the query measures");
        schema_out_path = schema_after_reconstruct;
    }

    substrait::Rel *direct_rel = new substrait::Rel();
    shared_ptr<Schema> schema_after_direct;
    if (direct_params.size() > 0)
    {
        schema_after_direct = directEvalPath(direct_rel, table_schema,
                                             direct_params, query);
        if (schema_after_direct->size() != all_measures.size())
            throw Exception(
                "evaluate: out schema of direct path must have "
                "the same number of attributes as the query measures");
        schema_out_path = schema_after_direct;
    }

    if (schema_after_direct && schema_after_reconstruct &&
        !schema_after_direct->equal(schema_after_reconstruct))
        throw Exception("evaluate: direct path and reconstruction path "
                        "must have the same out schema");

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
        *union_rel->mutable_set()->add_inputs() = *reconstruct_rel;
    if (schema_after_direct)
        *union_rel->mutable_set()->add_inputs() = *direct_rel;
    delete reconstruct_rel;
    delete direct_rel;

    if (exchange_rel)
        schema_after_union =
            exchange(exchange_rel, schema_after_union, {});

    // evaluate aggregation function
    vector<shared_ptr<const AggregateExpression>>
        measures_after_projection;
    for (int i = 0; i < all_measures.size(); i++)
    {
        auto m = all_measures[i];
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