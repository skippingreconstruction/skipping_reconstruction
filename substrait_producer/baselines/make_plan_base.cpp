#include "baselines/make_plan_base.h"
#include "baselines/early_agg/make_plan.h"
#include "baselines/early_join/make_plan.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"

using namespace std;

shared_ptr<Schema> readBlocks(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &requested_attributes)
{
    substrait::Rel *project_rel, *filter_rel, *read_rel;
    project_rel = rel;
    if (parameter->filter)
    {
        filter_rel = project_rel->mutable_project()->mutable_input();
        read_rel = filter_rel->mutable_filter()->mutable_input();
    }
    else
        read_rel = project_rel->mutable_project()->mutable_input();

    // read data blocks
    auto schema_after_read = readBlocks(
        read_rel, parameter->read_attributes, parameter->block_id,
        table_schema, parameter->file_path);

    // filter tuples
    auto schema_after_filter = schema_after_read;
    if (parameter->filter)
        schema_after_filter =
            filter(filter_rel, parameter->filter, schema_after_read);

    // project attributes
    vector<shared_ptr<const Expression>> project_expression;
    shared_ptr<FixedBinary> valid_attribute =
        make_shared<FixedBinary>(parameter->project_attributes);
    project_expression.push_back(
        make_shared<Literal>(valid_attribute_name, valid_attribute));

    for (auto i = requested_attributes.find_first();
         i != requested_attributes.npos;
         i = requested_attributes.find_next(i))
    {
        if (parameter->project_attributes.test(i))
            project_expression.push_back(table_schema->get(i));
        else
            project_expression.push_back(
                make_shared<Literal>(table_schema->get(i)->getName(),
                                     table_schema->get(i)->getType()));
    }
    auto schema_after_project =
        project(project_rel, project_expression, schema_after_filter);
    return schema_after_project;
}

shared_ptr<Schema> aggregate(substrait::Rel *rel,
                             substrait::Rel *input_rel,
                             shared_ptr<const Schema> input_schema,
                             shared_ptr<const Query> query)
{
    auto aggregate_rel = rel;
    auto project_rel =
        aggregate_rel->mutable_aggregate()->mutable_input();

    if (InputParameter::get()->engine == InputParameter::Engine::Velox)
    {
        auto exchange_rel =
            project_rel->mutable_project()->mutable_input();
        exchange_rel->mutable_exchange()->set_allocated_input(
            input_rel);
        input_schema = exchange(exchange_rel, input_schema, {});
    }
    else
    {
        project_rel->mutable_project()->set_allocated_input(input_rel);
    }

    auto all_measures = query->getMeasures();
    vector<shared_ptr<const Expression>> project_expression;
    for (auto m : all_measures)
    {
        if (m->getChildren().size() != 1)
            throw Exception("aggregate: an aggregate measure function "
                            "must exactly have one arguments");
        // auto child =
        //     shared_ptr<Expression>(m->getChildren()[0]->clone());
        // child->setName(m->getName());
        project_expression.push_back(m->getChildren()[0]);
    }
    auto schema_after_project =
        project(project_rel, project_expression, input_schema);

    // evaluate agg measures
    vector<shared_ptr<const AggregateExpression>>
        measures_after_projection;
    for (auto m : all_measures)
    {
        int off = schema_after_project->getOffset(
            m->getChildren()[0]->getName());
        measures_after_projection.push_back(
            make_shared<AggregateExpression>(
                m->getName(), m->getFunction(),
                vector<shared_ptr<const Expression>>{
                    schema_after_project->get(off)},
                m->getType(), m->getNullable()));
    }
    auto schema_after_agg =
        aggregate(aggregate_rel, schema_after_project,
                  measures_after_projection, nullptr);
    return schema_after_agg;
}

shared_ptr<Schema> evaluate(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &reconstruct_params,
    const vector<shared_ptr<const ScanParameter>> &direct_params)
{
    // either reconstruct everything or nothing
    assert(direct_params.size() == 0 || reconstruct_params.size() == 0);
    assert(direct_params.size() + reconstruct_params.size() > 0);

    if (reconstruct_params.size())
    {
        if (InputParameter::get()->reconstruct ==
            InputParameter::ReconstructType::Join)
            return evaluateEarlyJoin(rel, table_schema, query,
                                     reconstruct_params);
        else
            return evaluateEarlyAgg(rel, table_schema, query,
                                    reconstruct_params, true);
    }
    else
    {
        for (int i = 1; i < direct_params.size(); i++)
            assert(direct_params[i]->project_attributes ==
                   direct_params[0]->project_attributes);
        return evaluateEarlyAgg(rel, table_schema, query, direct_params,
                                false);
    }
}
