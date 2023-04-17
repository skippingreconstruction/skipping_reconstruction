#pragma once
#include "produce_plan/scan_parameter.h"

shared_ptr<Schema> makeJoinSequence(
    ::substrait::Rel *rel,
    const vector<shared_ptr<const ScanParameter>> &blocks,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const Query> query, bool filter_tuples);