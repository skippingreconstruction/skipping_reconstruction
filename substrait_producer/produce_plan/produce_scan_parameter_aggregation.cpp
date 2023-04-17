#include "metadata/boundary.h"
#include "produce_plan/produce_scan_parameter.h"

namespace scan_parameter_internal
{
namespace aggregate
{
RawScanParameter produceDirect(
    const RawRequest &finalized_request,
    const RawScanParameter *reconstruct_scan_parameter)
{
    RawScanParameter p;
    auto query = finalized_request.query;
    auto block = finalized_request.block;

    int measure_num = query->numOfMeasures();
    p.direct_measures = make_shared<boost::dynamic_bitset<>>();
    p.direct_measures->resize(measure_num, false);

    p.read_attributes = finalized_request.extra_check_filter_attributes;
    p.passed_attributes = finalized_request.passed_filter_attributes;
    if (p.passed_attributes !=
        query->getFilterBoundary()->getAttributes())
        throw Exception(
            "produceDirect: the block must pass all query predicates");

    unordered_set<string> reconstruct_attributes;
    bool reconstruct_all_tuples = false;
    if (reconstruct_scan_parameter)
    {
        reconstruct_attributes =
            reconstruct_scan_parameter->project_attributes;
        // all tuples in the block will be placed in the reconstruction
        // hash table if the filter immediately after reading covers the
        // entire block. These tuples will be kept in the hash table
        // because the passed_filter_attributes marks these tuples have
        // passed all query predicates.
        auto relationship =
            reconstruct_scan_parameter->filters->relationship(
                query->getFilterBoundary()->intersect(
                    *block->getBoundary()));
        if (relationship == SET_RELATION::EQUAL ||
            relationship == SET_RELATION::SUPERSET)
            reconstruct_all_tuples = true;
    }

    auto block_attributes = block->getSchema()->getAttributeNames();
    for (int i = 0; i < measure_num; i++)
    {
        auto measure_attributes = query->attributesInMeasure(i);
        if (set_difference(*measure_attributes, block_attributes)
                .size() != 0)
        {
            // the block does not contain all attributes in the measure.
            // In this case, the attributes that are both in the block
            // and the measure must have been requested
            auto t1 =
                set_intersection(*measure_attributes, block_attributes);
            auto t2 = set_intersection(*measure_attributes,
                                       reconstruct_attributes);
            if (t1 != t2)
                throw Exception("produceDirect: attributes in a "
                                "partial measure are not requested.");

            if (t1.size() != 0 && reconstruct_all_tuples == false)
                throw Exception("productDirect: tuples in a partial "
                                "measure are not requested");
            continue;
        }

        // The block contain all attributes in the measure. Skip the
        // measure if the reconstruction processes all attributes in the
        // measure and all tuples in the block.
        bool contain_attributes =
            set_difference(*measure_attributes, reconstruct_attributes)
                .size() == 0;
        if (contain_attributes && reconstruct_all_tuples)
            continue;

        // The reconstruction path does not process all tuples or all
        // attributes in the measure. Hence, we evaluate the measure in
        // the direct path.
        p.direct_measures->set(i);
        p.read_attributes.insert(measure_attributes->begin(),
                                 measure_attributes->end());
        // set_union(p.read_attributes, *measure_attributes);
        p.project_attributes.insert(measure_attributes->begin(),
                                    measure_attributes->end());
        // set_union(p.project_attributes, *measure_attributes);
    }

    p.filters =
        make_shared<ComplexBoundary>(*query->getFilterBoundary());
    p.filters->keepAttributes(
        finalized_request.extra_check_filter_attributes);

    return p;
}

RawScanParameter produceReconstruct(const RawRequest &finalized_request)
{
    RawScanParameter p;

    // read the requested attributes and attributes for evaluating
    // predicates
    p.project_attributes =
        finalized_request.filter_requested_attributes;
    p.project_attributes.insert(
        finalized_request.measure_requested_attributes.begin(),
        finalized_request.measure_requested_attributes.end());
    p.project_attributes.insert(tuple_id_name);

    p.read_attributes = p.project_attributes;
    p.read_attributes.insert(
        finalized_request.extra_check_filter_attributes.begin(),
        finalized_request.extra_check_filter_attributes.end());
    // set_union(p.project_attributes,
    //           finalized_request.extra_check_filter_attributes);

    p.passed_attributes = finalized_request.passed_filter_attributes;
    auto filters = finalized_request.filter_requested_filters;
    filters.insert(filters.end(),
                   finalized_request.measure_requested_filters.begin(),
                   finalized_request.measure_requested_filters.end());
    p.filters = ComplexBoundary::makeComplexBoundary(filters, 5);
    p.filters->keepAttributes(p.read_attributes);

    auto block_boundary = finalized_request.block->getBoundary();
    p.filters = convergeBoundary(block_boundary, p.filters);

    return p;
}
}; // namespace aggregate
}; // namespace scan_parameter_internal

std::pair<vector<shared_ptr<const ScanParameter>>,
          vector<shared_ptr<const ScanParameter>>>
produceScanParametersAggregation(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions)
{
    unordered_set<shared_ptr<const BlockMeta>> all_blocks;
    for (auto p : partitions)
    {
        auto b = p->getBlocks();
        all_blocks.insert(b.begin(), b.end());
    }

    // get blocks that overlap with the measures or the query predicate
    auto boundary_query = query->getFilterBoundary();
    auto measure_num = query->numOfMeasures();
    unordered_set<string> attributes_all_measures;
    for (int i = 0; i < measure_num; i++)
    {
        auto a = query->attributesInMeasure(i);
        attributes_all_measures.insert(a->begin(), a->end());
    }
    auto attributes_query_filters = boundary_query->getAttributes();

    auto block_measures = scan_parameter_internal::filterBlocks(
        all_blocks, boundary_query, attributes_all_measures);
    auto block_filters = scan_parameter_internal::filterBlocks(
        all_blocks, boundary_query, attributes_query_filters);

    // compute the requests to each block
    auto reconstruct_requests = scan_parameter_internal::postRequests(
        query, block_measures, block_filters);

    unordered_map<shared_ptr<const BlockMeta>,
                  scan_parameter_internal::RawScanParameter>
        direct_parameters, reconstruct_paramters;

    for (auto b : block_measures)
    {
        auto it = reconstruct_requests.find(b);
        if (it == reconstruct_requests.end())
            throw Exception(
                "produceScanParametersAggregation: Cannot find request "
                "for a measure block");

        auto &request = it->second;
        if (request.passed_filter_attributes.size() ==
            attributes_query_filters.size())
        {
            // the block can evaluate all predicates by block bounary
            // and the attribtues in the block, without referring to
            // other blocks
            if (request.filter_requested_attributes.size() +
                    request.measure_requested_attributes.size() !=
                0)
                // the block is requested by other blocks.
                reconstruct_paramters[b] = scan_parameter_internal::
                    aggregate::produceReconstruct(request);

            auto it = reconstruct_paramters.find(b);
            if (it != reconstruct_paramters.end())
            {
                direct_parameters[b] =
                    scan_parameter_internal::aggregate::produceDirect(
                        request, &reconstruct_paramters[b]);
                it->second.direct_measures =
                    direct_parameters[b].direct_measures;
            }
            else
                direct_parameters[b] =
                    scan_parameter_internal::aggregate::produceDirect(
                        request, nullptr);

            if (direct_parameters[b].direct_measures->count() == 0)
                direct_parameters.erase(b);
        }
        else
        {
            // The block cannot be directly evaluated.
            unordered_set<string> measure_attributes;
            for (int i = 0; i < measure_num; i++)
                measure_attributes.insert(
                    query->attributesInMeasure(i)->begin(),
                    query->attributesInMeasure(i)->end());
            request.request(measure_attributes, boundary_query, 0);
            request.finalize();
            reconstruct_paramters[b] =
                scan_parameter_internal::aggregate::produceReconstruct(
                    request);
            reconstruct_paramters[b].direct_measures =
                make_shared<boost::dynamic_bitset<>>();
            reconstruct_paramters[b].direct_measures->resize(
                measure_num, false);
        }
    }

    for (auto it = reconstruct_requests.begin();
         it != reconstruct_requests.end(); it++)
    {
        if (block_measures.count(it->first) == 0)
        {

            // the block is pure filter-requested block
            auto param =
                scan_parameter_internal::aggregate::produceReconstruct(
                    it->second);
            param.direct_measures =
                make_shared<boost::dynamic_bitset<>>();
            param.direct_measures->resize(measure_num, false);
            reconstruct_paramters[it->first] = param;
        }
    }

    auto sub_filter_query =
        query->getFilter()->getSubExpressions("and");
    vector<shared_ptr<const ScanParameter>> reconstruct_result,
        direct_result;
    for (auto it = reconstruct_paramters.begin();
         it != reconstruct_paramters.end(); it++)
        reconstruct_result.push_back(
            make_shared<ScanParameter>(it->second.produceScanParameter(
                table_schema, sub_filter_query, it->first)));
    for (auto it = direct_parameters.begin();
         it != direct_parameters.end(); it++)
        direct_result.push_back(
            make_shared<ScanParameter>(it->second.produceScanParameter(
                table_schema, sub_filter_query, it->first)));
    return make_pair(direct_result, reconstruct_result);
}