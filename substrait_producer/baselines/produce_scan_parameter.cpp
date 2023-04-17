#include "baselines/produce_scan_parameter.h"

shared_ptr<ScanParameter> produceScanParameters(
    shared_ptr<const BlockMeta> block, shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema)
{
    shared_ptr<ScanParameter> p = make_shared<ScanParameter>();
    p->file_path = block->getPartition()->getPath();
    p->block_id.insert(block->getBlockID());
    p->blocks.insert(block);

    auto query_boundary =
        shared_ptr<Boundary>(query->getFilterBoundary()->clone());
    auto block_attributes = block->getSchema()->getAttributeNames();
    // only keep intervals that the referred attributes are in the block
    query_boundary->keepAttributes(block_attributes);
    p->filter_boundary = make_shared<ComplexBoundary>(*query_boundary);
    p->filter = query_boundary->makeExpression();

    auto requested_attributes = query->getAllReferredAttributes();
    requested_attributes.insert(tuple_id_name);

    p->read_attributes.resize(table_schema->size());
    for (auto a : requested_attributes)
    {
        if (!block->getSchema()->contains(a))
            continue;
        int off = table_schema->getOffset(a);
        p->read_attributes.set(off, true);
    }
    p->project_attributes = p->read_attributes;
    return p;
}

std::pair<vector<shared_ptr<const ScanParameter>>,
          vector<shared_ptr<const ScanParameter>>>
produceScanParameters(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions)
{
    auto query_boundary = query->getFilterBoundary();
    unordered_set<string> requested_attributes =
        query->getAllReferredAttributes();

    vector<shared_ptr<const ScanParameter>> empty;
    vector<shared_ptr<ScanParameter>> result;
    bool all_skipping = true;

    for (auto p : partitions)
    {
        for (auto block : p->getBlocks())
        {
            // skip the block when the block does not contain any
            // requested tuple or attribute
            auto relation = block->relationship(
                query->getFilterBoundary(), requested_attributes);
            if (relation == SET_RELATION::DISJOINT)
                continue;

            // when the block miss any requested attributes
            relation =
                block->getSchema()->relationship(requested_attributes);
            if (relation == SET_RELATION::SUBSET ||
                relation == SET_RELATION::INTERSECT)
                all_skipping = false;

            result.push_back(
                produceScanParameters(block, query, table_schema));
        }
    }

    // skip reconstruction if all blocks contain all requested
    // attributes. Unset the corresponding bit of tuple_id to 0
    if (all_skipping)
    {
        vector<shared_ptr<const ScanParameter>> ans;
        int idx = table_schema->getOffset(tuple_id_name);
        for (auto p : result)
        {
            p->read_attributes.set(idx, false);
            p->project_attributes.set(idx, false);
            ans.push_back(p);
        }
        return make_pair(ans, empty);
    }
    // reconstruct everything if any block miss an attribute
    else
    {
        vector<shared_ptr<const ScanParameter>> ans(result.begin(),
                                                    result.end());
        return make_pair(empty, ans);
    }
}