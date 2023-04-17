#include "produce_plan/impl/build_substrait_impl_arrow.h"
#include "configuration.h"
#include "produce_plan/build_substrait.h"
#include <memory.h>
#include <unordered_map>

/**
 * @brief Read a parquet file
 *
 * @param rel
 * @param path
 * @param block_id
 * @param base_schema
 */
shared_ptr<Schema> readArrow(st::Rel *rel, const std::string &path,
                             const std::vector<int> &block_id,
                             shared_ptr<const Schema> base_schema)
{
    if (block_id.size() == 0)
        throw Exception("read: read at least one block from " + path);

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
    *file->mutable_uri_path() = path;
    file->mutable_parquet();

    shared_ptr<FunctionExpression> check_block_exp;
    auto block_id_att = base_schema->get(block_id_name);

    auto make_precise_filter = [&]() -> shared_ptr<FunctionExpression> {
        vector<shared_ptr<const Expression>> subExps;
        for (auto b : block_id)
        {
            shared_ptr<Literal> l = make_shared<Literal>(
                "bid_value", make_shared<Integer>(b, 32));
            subExps.push_back(make_shared<FunctionExpression>(
                "bid_check_one_element", "equal",
                vector<shared_ptr<const Expression>>{block_id_att, l},
                DATA_TYPE::BOOLEAN, false));
        }
        return FunctionExpression::connectExpression(
            "check_block_exp", subExps, false, false);
    };

    if (block_id.size() < 10)
    {
        check_block_exp = make_precise_filter();
    }
    else
    {
        shared_ptr<Integer> min_block_id = make_shared<Integer>(
            *min_element(block_id.begin(), block_id.end()), 32);
        shared_ptr<Integer> max_block_id = make_shared<Integer>(
            *max_element(block_id.begin(), block_id.end()), 32);
        shared_ptr<Literal> min_l =
            make_shared<Literal>("min_block_id", min_block_id);
        shared_ptr<Literal> max_l =
            make_shared<Literal>("max_block_id", max_block_id);
        shared_ptr<Attribute> block_id_attr =
            base_schema->get(block_id_name);

        shared_ptr<FunctionExpression> min_f =
            make_shared<FunctionExpression>(
                "f1", "gte",
                vector<shared_ptr<const Expression>>{block_id_attr,
                                                     min_l},
                DATA_TYPE::BOOLEAN, false);
        shared_ptr<FunctionExpression> max_f =
            make_shared<FunctionExpression>(
                "f2", "lte",
                vector<shared_ptr<const Expression>>{block_id_attr,
                                                     max_l},
                DATA_TYPE::BOOLEAN, false);
        check_block_exp = make_shared<FunctionExpression>(
            "check_block_exp", "and",
            vector<shared_ptr<const Expression>>{min_f, max_f},
            DATA_TYPE::BOOLEAN, false);
    }
    // check block id in read to skip data pages in file
    check_block_exp->makeSubstraitExpression(read_rel->mutable_filter(),
                                             base_schema);

    // check the block id after read
    check_block_exp = make_precise_filter();
    check_block_exp->makeSubstraitExpression(
        filter_rel->mutable_condition(), base_schema);
    return make_shared<Schema>(*base_schema);
}

shared_ptr<Schema> filterArrow(st::Rel *rel,
                               shared_ptr<const Expression> expression,
                               shared_ptr<const Schema> inSchema)
{

    auto filter_rel = rel->mutable_filter();
    expression->makeSubstraitExpression(filter_rel->mutable_condition(),
                                        inSchema);
    return make_shared<Schema>(*inSchema);
}

shared_ptr<Schema> projectArrow(
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

    // delete the original attributes
    auto emit = project_rel->mutable_common()->mutable_emit();
    for (int i = in_schema->size();
         i < in_schema->size() + out_schema->size(); i++)
        emit->add_output_mapping(i);
    return out_schema;
}

shared_ptr<Schema> unionAllArrow(st::Rel *rel,
                                 shared_ptr<const Schema> in_schema)
{
    auto union_rel = rel->mutable_set();
    union_rel->set_op(
        substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL);
    return make_shared<Schema>(*in_schema);
}

shared_ptr<Schema> aggregateArrow(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group)
{
    auto agg_rel = rel->mutable_aggregate();

    shared_ptr<Schema> out_schema = make_shared<Schema>();
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

    // the group attribute is at the end of the tuple in Substriat/Arrow
    if (group)
    {
        group->makeSubstraitExpression(
            agg_rel->add_groupings()->add_grouping_expressions(),
            in_schema);
        shared_ptr<Attribute> group_attr =
            make_shared<Attribute>(group->getName(), group->getType());
        out_schema->add(group_attr);
    }

    return out_schema;
}