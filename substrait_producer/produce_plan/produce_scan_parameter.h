#pragma once
#include "metadata/complex_boundary.h"
#include "produce_plan/scan_parameter.h"

template <typename T>
unordered_set<T> set_difference(const unordered_set<T> &a,
                                const unordered_set<T> &b)
{
    unordered_set<T> result;
    for (auto it = a.begin(); it != a.end(); it++)
        if (b.count(*it) == 0)
            result.insert(*it);
    return result;
}

template <typename T>
unordered_set<T> set_intersection(const unordered_set<T> &a,
                                  const unordered_set<T> &b)
{
    unordered_set<T> result;
    for (auto it = a.begin(); it != a.end(); it++)
        if (b.count(*it))
            result.insert(*it);
    return result;
}

std::pair<vector<shared_ptr<const ScanParameter>>,
          vector<shared_ptr<const ScanParameter>>>
produceScanParametersAggregation(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions);

void produceScanParameterJoin(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions,
    vector<shared_ptr<const ScanParameter>> &direct_params,
    vector<shared_ptr<const ScanParameter>> &recons_filter_params,
    vector<vector<shared_ptr<const ScanParameter>>>
        &recons_measure_params);

namespace scan_parameter_internal
{
unordered_set<shared_ptr<const BlockMeta>> filterBlocks(
    const unordered_set<shared_ptr<const BlockMeta>> &blocks,
    shared_ptr<const Boundary> filter,
    const unordered_set<string> &attributes);
/**
 * @brief Find the extra filter to converge the source filter to be the
 * subset of the target filter. The source filter and the target filter
 * cannot be disjoint.
 *
 * @param source the source filter
 * @param target the target filter
 * @return shared_ptr<Boundary> the extra filter that should be added to
 * the source filter to converge.
 */
shared_ptr<Boundary> convergeBoundary(
    shared_ptr<const Boundary> source,
    shared_ptr<const Boundary> target);

shared_ptr<ComplexBoundary> convergeBoundary(
    shared_ptr<const Boundary> source,
    shared_ptr<const ComplexBoundary> target);

struct RawRequest
{
    shared_ptr<const BlockMeta> block;
    shared_ptr<const Query> query;

    // the attributes that are requested by other blocks to evaluate
    // predicates. An empty set means that non other blocks requests to
    // read data from this block.
    unordered_set<string> filter_requested_attributes;
    // the attributes that are requested by other blocks to evaluate
    // aggregation measures. An empty set means that non other blocks
    // requests to read data from this block.
    unordered_set<string> measure_requested_attributes;
    // The filters requested by other blocks to evaluate predicates. An
    // empty vector means that non other blocks requests to read data
    // from this block.
    vector<shared_ptr<const Boundary>> filter_requested_filters;
    // The filters requested by other blocks to evaluate aggregation
    // measures. An empty vector means that non other blocks requests to
    // read data from this block.
    vector<shared_ptr<const Boundary>> measure_requested_filters;

    // The attributes that the passed query predicates refer to. The
    // data block can satisfy a predicate in two cases: the boundary of
    // the block is equal to or the subset of the query on that
    // predicate; Or the data block contains the attribute referred by
    // the predicate.
    unordered_set<string> passed_filter_attributes;
    // The attributes in the second case. These attributes will be read
    // in the both the direct path and reconstruction path
    unordered_set<string> extra_check_filter_attributes;

    string toString() const;
    RawRequest *clone() const;
    void add_attributes(unordered_set<string> &target,
                        const unordered_set<string> &attributes);
    /**
     * @brief Add request
     *
     * @param attributes the requested attributes
     * @param filter request a subset of tuples
     * @param type 0 is to request for predicate. 1 is to request for
     * aggregation measures
     */
    void request(const unordered_set<string> &attributes,
                 shared_ptr<const Boundary> filter, int type);
    void intersectFilter(const Boundary &b);
    /**
     * @brief Remove the attributes in requested_attributes and
     * extra_check_filter_attributes that will be read in future but not
     * in the block. Second, it intersects all requested filters with
     * the block boundary and the query boundary
     */
    void finalize();
};

struct RawScanParameter
{
    // the attributes reading from the file
    unordered_set<string> read_attributes;

    // the projected attributes after reading but before union
    unordered_set<string> project_attributes;

    // the filters that evaluate right after scanning the data
    shared_ptr<ComplexBoundary> filters;
    // shared_ptr<Boundary> filters;

    unordered_set<string> passed_attributes;

    // the measures that are directly evaluated
    shared_ptr<boost::dynamic_bitset<>> direct_measures;

    ScanParameter produceScanParameter(
        shared_ptr<const Schema> table_schema,
        const vector<shared_ptr<const FunctionExpression>>
            &sub_filters_in_query,
        shared_ptr<const BlockMeta> block) const;
};

/**
 * @brief add requests to blocks
 *
 * @param query
 * @param target_blocks
 * @param filter the filter to request a subset of tuples
 * @param attributes requested attributes
 * @param request_type 0 if the request is for predicates. 1 if the
 * request is for aggregation measures
 * @param requests
 */
void postRequests(
    shared_ptr<const Query> query,
    const unordered_set<shared_ptr<const BlockMeta>> &target_blocks,
    shared_ptr<const Boundary> filter,
    const unordered_set<string> &attributes, int request_type,
    unordered_map<shared_ptr<const BlockMeta>, RawRequest> &requests);

unordered_map<shared_ptr<const BlockMeta>, RawRequest> postRequests(
    shared_ptr<const Query> query,
    const unordered_set<shared_ptr<const BlockMeta>> &block_measures,
    const unordered_set<shared_ptr<const BlockMeta>> &block_filters);
}; // namespace scan_parameter_internal