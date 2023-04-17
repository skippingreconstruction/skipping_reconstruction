#pragma once

#include "metadata/expression.h"
#include "metadata/schema.h"
#include "substrait/plan.pb.h"

namespace st = substrait;
using namespace std;

shared_ptr<Schema> readArrow(st::Rel *rel, const std::string &path,
                             const std::vector<int> &block_id,
                             shared_ptr<const Schema> project_schema);
shared_ptr<Schema> filterArrow(st::Rel *rel,
                               shared_ptr<const Expression> expression,
                               shared_ptr<const Schema> in_schema);

shared_ptr<Schema> projectArrow(
    st::Rel *rel,
    const vector<shared_ptr<const Expression>> &expression,
    shared_ptr<const Schema> in_schema);

shared_ptr<Schema> unionAllArrow(st::Rel *rel,
                                 shared_ptr<const Schema> in_schema);

shared_ptr<Schema> aggregateArrow(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group);