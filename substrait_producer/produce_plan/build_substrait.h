#pragma once

#include <string>
#include <vector>

#include "metadata/expression.h"
#include "metadata/schema.h"
#include "substrait/plan.pb.h"

namespace st = substrait;
using namespace std;

void registFunctions(st::Plan *plan);
int getFunctionAnchor(const string &name);
/**
 * @brief Get the Function Overflow Option object
 *
 * @param name
 * @return string An empty string means that the function does not have
 * any overflow option
 */
string getFunctionOverflowOption(const string &name);

void makeBinaryScalarFunction(
    substrait::Expression_ScalarFunction *function,
    const string &function_name, int offset, const DataType *value,
    DATA_TYPE out_type, bool nullable);

shared_ptr<FunctionExpression> checkValid(
    const string &name, shared_ptr<const Attribute> attribute);

shared_ptr<Schema> read(st::Rel *rel, const std::string &path,
                        const std::vector<int> &block_id,
                        shared_ptr<const Schema> project_schema);
shared_ptr<Schema> filter(st::Rel *rel,
                          shared_ptr<const Expression> expression,
                          shared_ptr<const Schema> in_schema);

shared_ptr<Schema> project(
    st::Rel *rel,
    const vector<shared_ptr<const Expression>> &expression,
    shared_ptr<const Schema> in_schema);

shared_ptr<Schema> unionAll(st::Rel *rel,
                            shared_ptr<const Schema> in_schema);

shared_ptr<Schema> aggregate(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group);

shared_ptr<Schema> exchange(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    const vector<shared_ptr<const Attribute>> &scatter_attributes);

/**
 * @brief Build an equal join between the left and right table
 *
 * @param rel the Join rel
 * @param left_schema schema of the left table
 * @param right_schema schema of the right table
 * @param left_key name of the join key
 * @param right_key name of the join key
 * @param type join type
 * @param left_map OUT: the offset of the left attributes in the output
 * schema
 * @param right_map OUT: the offset of the right attributes in the
 * output schema
 * @return shared_ptr<Schema> merge the left_schema and the
 * right_schema. Attributes prefix with "left_" or "right_".
 */
shared_ptr<Schema> equalJoin(
    st::Rel *rel, shared_ptr<const Schema> left_schema,
    shared_ptr<const Schema> right_schema, const string &left_key,
    const string &right_key, ::substrait::JoinRel_JoinType type,
    vector<int> &left_map, vector<int> &right_map);