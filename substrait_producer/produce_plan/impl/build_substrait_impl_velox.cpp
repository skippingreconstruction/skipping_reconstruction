#include "produce_plan/impl/build_substrait_impl_velox.h"
#include "configuration.h"
#include "produce_plan/build_substrait.h"
#include <memory.h>
#include <unordered_map>

shared_ptr<Schema> readVelox(st::Rel *rel, const std::string &path,
                             const std::vector<int> &block_id,
                             shared_ptr<const Schema> base_schema)
{
    if (block_id.size() != 1)
        throw Exception(
            "read: Velox consumer only reads one block each time");
    auto filter_rel = rel->mutable_filter();

    // read the blocks
    auto read_rel = filter_rel->mutable_input()->mutable_read();
    auto schema = read_rel->mutable_base_schema();
    auto types = schema->mutable_struct_();
    for (size_t i = 0; i < base_schema->size(); i++)
    {
        string name = base_schema->get(i)->getName();
        bool nullable = true;
        if (name == block_id_name)
            nullable = false;
        schema->add_names(name);
        makeSubstraitType(types->add_types(),
                          base_schema->get(i)->getType(), nullable,
                          false);
    }

    auto file = read_rel->mutable_local_files()->add_items();
    *file->mutable_uri_file() = path;
    file->mutable_parquet();

    auto block_id_att = base_schema->get(block_id_name);
    shared_ptr<const Expression> bid_value = make_shared<const Literal>(
        "bid_value", make_shared<Integer>(block_id[0], 32));
    shared_ptr<FunctionExpression> check_block_exp =
        make_shared<FunctionExpression>(
            "bid_check_one_element", "equal",
            vector<shared_ptr<const Expression>>{block_id_att,
                                                 bid_value},
            DATA_TYPE::BOOLEAN, false);
    check_block_exp->makeSubstraitExpression(read_rel->mutable_filter(),
                                             base_schema);

    // check the block id after read
    check_block_exp->makeSubstraitExpression(
        filter_rel->mutable_condition(), base_schema);
    return make_shared<Schema>(*base_schema);
}

shared_ptr<Schema> filterVelox(st::Rel *rel,
                               shared_ptr<const Expression> expression,
                               shared_ptr<const Schema> in_schema)
{
    auto filter_rel = rel->mutable_filter();
    expression->makeSubstraitExpression(filter_rel->mutable_condition(),
                                        in_schema);
    return make_shared<Schema>(*in_schema);
}

shared_ptr<Schema> projectVelox(
    st::Rel *rel,
    const vector<shared_ptr<const Expression>> &expression,
    shared_ptr<const Schema> in_schema)
{
    auto project_rel = rel->mutable_project();
    auto out_schema = make_shared<Schema>();
    for (int i = 0; i < expression.size(); i++)
    {
        expression[i]->makeSubstraitExpression(
            project_rel->add_expressions(), in_schema);
        DATA_TYPE type = expression[i]->getType();
        shared_ptr<Attribute> a =
            make_shared<Attribute>(expression[i]->getName(), type);
        out_schema->add(std::move(a));
    }
    return out_schema;
}

shared_ptr<Schema> unionAllVelox(st::Rel *rel,
                                 shared_ptr<const Schema> in_schema)
{
    auto union_rel = rel->mutable_set();
    union_rel->set_op(
        substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL);
    return make_shared<Schema>(*in_schema);
}

shared_ptr<Schema> aggregateVelox(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group)
{
    auto agg_rel = rel->mutable_aggregate();

    shared_ptr<Schema> out_schema = make_shared<Schema>();
    if (group)
    {
        group->makeSubstraitExpression(
            agg_rel->add_groupings()->add_grouping_expressions(),
            in_schema);
        shared_ptr<Attribute> group_attr =
            make_shared<Attribute>(group->getName(), group->getType());
        out_schema->add(group_attr);
    }

    for (int i = 0; i < measures.size(); i++)
    {
        auto m = agg_rel->add_measures()->mutable_measure();
        int anchor = getFunctionAnchor(measures[i]->getFunction());
        m->set_function_reference(anchor);

        DATA_TYPE type = measures[i]->getType();
        bool upper = true;
        if (measures[i]->getFunction() == "reconstruct")
            upper = false;
        makeSubstraitType(m->mutable_output_type(), type,
                          measures[i]->getNullable(), upper);

        for (shared_ptr<const Expression> e :
             measures[i]->getChildren())
            e->makeSubstraitExpression(
                m->add_arguments()->mutable_value(), in_schema);
        m->set_invocation(
            substrait::AggregateFunction_AggregationInvocation::
                AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_ALL);
        m->set_phase(substrait::AggregationPhase::
                         AGGREGATION_PHASE_INITIAL_TO_RESULT);

        shared_ptr<Attribute> a =
            make_shared<Attribute>(measures[i]->getName(), type);
        out_schema->add(std::move(a));
    }
    return out_schema;
}

shared_ptr<Schema> exchangeVelox(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    const vector<shared_ptr<const Attribute>> &scatter_attributes)
{
    auto exchange_rel = rel->mutable_exchange();
    if (scatter_attributes.size() > 0)
    {
        for (const auto &attr : scatter_attributes)
        {
            auto field =
                exchange_rel->mutable_scatter_by_fields()->add_fields();

            ::substrait::Expression e;
            attr->makeSubstraitExpression(&e, in_schema);
            field->CopyFrom(e.selection());
        }
    }
    else
    {
        exchange_rel->mutable_round_robin()->set_exact(true);
    }
    return make_shared<Schema>(*in_schema);
}