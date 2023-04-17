#include "baselines/early_join/make_plan.h"
#include "baselines/early_join/join_sequence.h"
#include "baselines/make_plan_base.h"
#include "produce_plan/build_substrait.h"
#include "produce_plan/helper.h"
#include "substrait/plan.pb.h"
#include <iostream>
#include <variant>

using namespace std;

shared_ptr<Schema> reconstructByJoin(
    substrait::Rel *rel,
    std::variant<shared_ptr<MiniTable>, shared_ptr<FilterParameter>>
        join_sequence,
    shared_ptr<const Schema> table_schema)
{
    substrait::Rel *union_rel(nullptr), *reconstruct_rel(rel);
    if (InputParameter::get()->engine == InputParameter::Engine::Velox)
    {
        union_rel = rel;
        reconstruct_rel = union_rel->mutable_set()->add_inputs();
    }

    shared_ptr<Schema> schema_after_reconstruct;
    auto filter_param =
        std::get_if<shared_ptr<FilterParameter>>(&join_sequence);
    if (filter_param)
    {
        auto filter_param =
            std::get<shared_ptr<FilterParameter>>(join_sequence);
        schema_after_reconstruct = filter_param->makeSubstraitRel(
            reconstruct_rel, table_schema);
    }
    else
    {
        auto mini_table =
            std::get<shared_ptr<MiniTable>>(join_sequence);
        schema_after_reconstruct =
            mini_table->makeSubstraitRel(reconstruct_rel);
    }

    auto schema_after_union = schema_after_reconstruct;
    if (union_rel)
        schema_after_union =
            unionAll(union_rel, schema_after_reconstruct);
    return schema_after_union;
}

shared_ptr<Schema> evaluateEarlyJoin(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>>
        &unmerged_reconstruct_params)
{
    auto reconstruct_params =
        mergeBeforeRead(unmerged_reconstruct_params);
    cout << table_schema->toString() << endl;
    cout << query->toString() << endl;
    printf("**************Early join (%zu params)***************\n",
           reconstruct_params.size());
    for (auto p : reconstruct_params)
        cout << p->toString() << endl;

    auto join_sequence =
        makeJoinSequence(table_schema, reconstruct_params);
    if (std::get_if<shared_ptr<MiniTable>>(&join_sequence))
        cout << std::get<shared_ptr<MiniTable>>(join_sequence)
                    ->toString()
             << endl;
    else
        cout << std::get<shared_ptr<FilterParameter>>(join_sequence)
                    ->toString()
             << endl;

    substrait::Rel *reconstruct_rel = new substrait::Rel();
    auto schema_after_reconstruct =
        reconstructByJoin(reconstruct_rel, join_sequence, table_schema);

    auto schema_after_agg = aggregate(rel, reconstruct_rel,
                                      schema_after_reconstruct, query);
    return schema_after_agg;
}