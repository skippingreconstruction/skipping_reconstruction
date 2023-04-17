#include "baselines/early_agg/make_plan.h"
#include "baselines/make_plan_base.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include <boost/dynamic_bitset.hpp>
#include <iostream>

using namespace std;

shared_ptr<Schema> reconstructByAgg(
    substrait::Rel *rel, substrait::Rel *input_rel,
    shared_ptr<const Schema> input_schema,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query)
{
    substrait::Rel *exchange_rel = nullptr, *agg_rel = nullptr,
                   *filter_rel = nullptr, *union_rel = nullptr;

    if (InputParameter::get()->parallel ==
        InputParameter::AggParallelMethod::Partition)
    {
        union_rel = rel;
        filter_rel = union_rel->mutable_set()->add_inputs();
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
        filter_rel = rel;
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
        else if (name == valid_attribute_name)
            op = "bitmap_or";
        else
            op = "reconstruct";
        measures.push_back(make_shared<const AggregateExpression>(
            name, op, expression, type, true));
    }
    shared_ptr<Expression> group = input_schema->get(tuple_id_name);
    auto schema_after_agg =
        aggregate(agg_rel, input_schema, measures, group);

    // filter tuples. Check if all requested attributes are valid and
    // then evaluate the query predicate
    vector<shared_ptr<const Expression>> new_filters;
    auto requested_attributes = query->getAllReferredAttributes();
    for (string attribute : requested_attributes)
    {
        if (!schema_after_agg->contains(attribute))
            throw Exception(
                "reconstruct: the schema after reconstruction does "
                "contain the requested attribute " +
                attribute);
        int off_in_table = table_schema->getOffset(attribute);
        auto valid_attr_exp = makeBitmapGet(
            valid_attribute_name, schema_after_agg, off_in_table);
        new_filters.push_back(std::move(valid_attr_exp));
    }
    new_filters.push_back(query->getFilter());
    auto new_filter = FunctionExpression::connectExpression(
        "filter_expression", new_filters, false, "and");
    auto schema_after_filter =
        filter(filter_rel, new_filter, schema_after_agg);

    if (InputParameter::get()->parallel ==
        InputParameter::AggParallelMethod::Partition)
        return unionAll(union_rel, schema_after_filter);
    else
        return schema_after_filter;
}

shared_ptr<Schema> evaluateEarlyAgg(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &unmerged_params,
    bool is_reconstruct)
{
    auto params = mergeBeforeRead(unmerged_params);
    cout << table_schema->toString() << endl;
    cout << query->toString() << endl;
    if (is_reconstruct)
        printf("**************Early agg (%zu params)***************\n",
               params.size());
    else
        printf(
            "**************Early direct (%zu params)***************\n",
            params.size());
    for (auto p : params)
        cout << p->toString() << endl;

    boost::dynamic_bitset<> requested_attributes(table_schema->size());
    for (auto p : params)
        requested_attributes |= p->project_attributes;
    vector<substrait::Rel *> read_rel;
    shared_ptr<Schema> schema_after_read;
    for (auto p : params)
    {
        auto rel = new substrait::Rel();
        auto s = readBlocks(rel, p, table_schema, requested_attributes);
        if (!schema_after_read)
            schema_after_read = s;
        else if (!schema_after_read->equal(s))
            throw Exception(
                "evaluate: inputs of union have different schemas");
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
    substrait::Rel *reconstruct_rel = union_rel;
    auto schema_after_reconstruct = schema_after_union;
    if (is_reconstruct)
    {
        reconstruct_rel = new substrait::Rel();
        schema_after_reconstruct =
            reconstructByAgg(reconstruct_rel, union_rel,
                             schema_after_union, table_schema, query);
    }

    // evaluate query measures
    auto schema_after_agg = aggregate(rel, reconstruct_rel,
                                      schema_after_reconstruct, query);
    return schema_after_agg;
}