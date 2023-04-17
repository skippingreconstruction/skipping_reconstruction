#pragma once

#include "metadata/expression.h"
#include "metadata/schema.h"
#include "substrait/plan.pb.h"

namespace st = substrait;
using namespace std;

/**
 * @brief Read just one block (Substrait consumer in Velox only support
 * conjunctive ReadRel.condition)
 *
 * @param rel
 * @param path
 * @param block_id
 * @param project_schema
 * @return shared_ptr<Schema>
 */
shared_ptr<Schema> readVelox(st::Rel *rel, const std::string &path,
                             const std::vector<int> &block_id,
                             shared_ptr<const Schema> project_schema);
shared_ptr<Schema> filterVelox(st::Rel *rel,
                               shared_ptr<const Expression> expression,
                               shared_ptr<const Schema> in_schema);

shared_ptr<Schema> projectVelox(
    st::Rel *rel,
    const vector<shared_ptr<const Expression>> &expression,
    shared_ptr<const Schema> in_schema);

shared_ptr<Schema> unionAllVelox(st::Rel *rel,
                                 shared_ptr<const Schema> in_schema);

/**
 * @brief Compute the aggregation. The groupby key is the first
 * attribute in the output table
 *
 * @param rel
 * @param in_schema
 * @param measures
 * @param group
 * @return shared_ptr<Schema>
 */
shared_ptr<Schema> aggregateVelox(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group);

shared_ptr<Schema> exchangeVelox(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    const vector<shared_ptr<const Attribute>> &scatter_attributes);