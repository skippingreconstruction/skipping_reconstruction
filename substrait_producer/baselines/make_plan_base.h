#pragma once

#include "metadata/query.h"
#include "metadata/schema.h"
#include "produce_plan/scan_parameter.h"
#include "substrait/plan.pb.h"

shared_ptr<Schema> readBlocks(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &requested_attributes);

shared_ptr<Schema> aggregate(substrait::Rel *rel,
                             substrait::Rel *input_rel,
                             shared_ptr<const Schema> input_schema,
                             shared_ptr<const Query> query);

shared_ptr<Schema> evaluate(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &reconstruct_params,
    const vector<shared_ptr<const ScanParameter>> &direct_params);