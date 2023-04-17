#pragma once
#include "produce_plan/scan_parameter.h"

/**
 * @brief Produce the scan parameters for early reconstruction plan.
 * When all the attributes requested by the query are always in the same
 * partition, we switch to a native columnar engine.
 *
 * @param query
 * @param table_schema
 * @param partitions
 * @return std::pair<vector<shared_ptr<const ScanParameter>>,
 * vector<shared_ptr<const ScanParameter>>> The first vector is for
 * partitions that do not need to be reconstructed and the second vector
 * is for reconstruction. Only one vector has elements.
 */
std::pair<vector<shared_ptr<const ScanParameter>>,
          vector<shared_ptr<const ScanParameter>>>
produceScanParameters(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions);