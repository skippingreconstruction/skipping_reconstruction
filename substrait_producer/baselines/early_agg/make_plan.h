#pragma once

#include "metadata/query.h"
#include "metadata/schema.h"
#include "produce_plan/scan_parameter.h"
#include "substrait/plan.pb.h"

shared_ptr<Schema> evaluateEarlyAgg(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &params,
    bool is_reconstruct = true);