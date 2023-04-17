#pragma once

#include "metadata/query.h"
#include "metadata/schema.h"
#include "produce_plan/scan_parameter.h"
#include "substrait/plan.pb.h"
#include <boost/dynamic_bitset.hpp>

shared_ptr<Schema> evaluateAggregatePlan(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &reconstruct_params,
    const vector<shared_ptr<const ScanParameter>> &direct_params);

shared_ptr<Schema> evalauteJoinPlan(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query,
    const vector<shared_ptr<const ScanParameter>> &direct_params,
    const vector<shared_ptr<const ScanParameter>> &recons_filter_params,
    const vector<vector<shared_ptr<const ScanParameter>>>
        &recons_measure_params);