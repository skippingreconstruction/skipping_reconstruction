#pragma once
#include "metadata/expression.h"
#include "metadata/schema.h"
#include "produce_plan/scan_parameter.h"
#include "substrait/algebra.pb.h"
#include <boost/dynamic_bitset.hpp>

shared_ptr<FunctionExpression> makeBitmapGet(
    const string &bitmap_attribute_name,
    shared_ptr<const Schema> schema, int offset);

vector<shared_ptr<const ScanParameter>> mergeBeforeRead(
    const vector<shared_ptr<const ScanParameter>> &parameters);

shared_ptr<Schema> readBlocks(substrait::Rel *read_rel,
                              const boost::dynamic_bitset<> &attributes,
                              const unordered_set<int> &block_id,
                              shared_ptr<const Schema> table_schema,
                              const string &path);

/**
 * read, filter, and project data blocks in one partiton before
 *reconstruction.
 * @param rel: the output substrait relation
 * @param paramter: the configuration for read, filter and project
 * @param table_schema: the schema of the source table. The schema must
 *contain the tuple id
 * @param reconstruct_attributes: all attributes that need to
 *reconstruct for any partition. Must set the tuple_id attribute to be
 *true
 * @return the schema after projection
 **/
shared_ptr<Schema> readForReconstruction(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &reconstruct_attributes);

/**
 * @brief read and project data blocks in one partition before direct
 * evaluation
 *
 * @param rel the output substrait relation
 * @param parameter the configuration for read and project
 * @param table_schema the schema of the source table. The schema
 * contains the tuple id
 * @param measure_attribtues attributes in all measures of the query
 * @return shared_ptr<Schema> the schema after projection
 */
shared_ptr<Schema> readForDirectEval(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &measure_attribtues);

/**
 * @brief Process a set of direct evaluation parameters
 *
 * @param rel
 * @param table_schema
 * @param scan_parameters
 * @param query
 * @return shared_ptr<Schema>
 */
shared_ptr<Schema> directEvalPath(
    substrait::Rel *rel, shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const ScanParameter>> &scan_parameters,
    shared_ptr<const Query> query);