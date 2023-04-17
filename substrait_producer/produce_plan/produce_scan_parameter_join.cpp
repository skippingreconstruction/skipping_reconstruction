#include "metadata/boundary.h"
#include "produce_plan/produce_scan_parameter.h"

namespace scan_parameter_internal
{
namespace join
{
RawScanParameter produceReconstructFilter(const RawRequest &request)
{
    RawScanParameter p;
    if (request.filter_requested_attributes.size() == 0)
        throw Exception("produceReconstructFilter: invalid "
                        "filter_requested_attributes\n" +
                        request.toString());
    // the project attributes actually only need the tuple id and
    // passed_preds bitmap
    p.project_attributes.insert(tuple_id_name);

    p.read_attributes = request.filter_requested_attributes;
    p.read_attributes.insert(tuple_id_name);
    p.read_attributes.insert(
        request.extra_check_filter_attributes.begin(),
        request.extra_check_filter_attributes.end());

    p.passed_attributes = request.passed_filter_attributes;

    p.filters = ComplexBoundary::makeComplexBoundary(
        request.filter_requested_filters, 5);
    p.filters->keepAttributes(p.read_attributes);
    auto block_boundary = request.block->getBoundary();
    p.filters = convergeBoundary(block_boundary, p.filters);

    p.direct_measures = make_shared<boost::dynamic_bitset<>>();
    p.direct_measures->resize(request.query->numOfMeasures(), false);

    return p;
}

RawScanParameter produceReconstructMeasure(const RawRequest &request)
{
    RawScanParameter p;
    p.project_attributes = request.measure_requested_attributes;
    p.project_attributes.insert(tuple_id_name);

    p.read_attributes = p.project_attributes;
    p.read_attributes.insert(
        request.extra_check_filter_attributes.begin(),
        request.extra_check_filter_attributes.end());

    p.passed_attributes = request.passed_filter_attributes;
    p.filters = ComplexBoundary::makeComplexBoundary(
        request.measure_requested_filters, 5);
    p.filters->keepAttributes(p.read_attributes);

    auto block_boundary = request.block->getBoundary();
    p.filters = convergeBoundary(block_boundary, p.filters);

    return p;
}

RawScanParameter produceDirect(
    const RawRequest &request,
    const RawScanParameter *recons_measure_param)
{
    RawScanParameter p;
    auto query = request.query;
    auto block = request.block;

    int measure_num = query->numOfMeasures();
    p.direct_measures = make_shared<boost::dynamic_bitset<>>();
    p.direct_measures->resize(measure_num, false);

    p.read_attributes = request.extra_check_filter_attributes;
    p.passed_attributes = request.passed_filter_attributes;
    if (p.passed_attributes !=
        query->getFilterBoundary()->getAttributes())
        throw Exception(
            "produceDirect: the block must pass all query predicates");

    unordered_set<string> reconstruct_attributes;
    if (recons_measure_param)
    {
        reconstruct_attributes =
            recons_measure_param->project_attributes;
        SET_RELATION relationship =
            recons_measure_param->filters->relationship(
                query->getFilterBoundary()->intersect(
                    *block->getBoundary()));
        if (relationship != SET_RELATION::EQUAL &&
            relationship != SET_RELATION::SUPERSET)
            throw Exception("produceDirect: expect to reconstruct all "
                            "tuples in the block");
    }

    unordered_set<string> block_attributes =
        block->getSchema()->getAttributeNames();
    for (int i = 0; i < measure_num; i++)
    {
        auto measure_attributes = query->attributesInMeasure(i);
        unordered_set<string> t1 =
            set_intersection(*measure_attributes, block_attributes);
        // skip the measure if the block does not contain all attributes
        // in the measure
        if (t1 != *measure_attributes)
            continue;

        unordered_set<string> t2 = set_intersection(
            *measure_attributes, reconstruct_attributes);
        // skip the measure if all requested attributes are
        // reconstructed
        if (t2 == *measure_attributes)
            continue;

        p.direct_measures->set(i);
        p.read_attributes.insert(measure_attributes->begin(),
                                 measure_attributes->end());
        p.project_attributes.insert(measure_attributes->begin(),
                                    measure_attributes->end());
    }

    p.filters =
        make_shared<ComplexBoundary>(*query->getFilterBoundary());
    p.filters->keepAttributes(request.extra_check_filter_attributes);

    return p;
}

struct Node
{
    RawRequest request;
    unordered_set<shared_ptr<Node>> neighbors;
};

unordered_map<shared_ptr<const BlockMeta>, shared_ptr<Node>> buildGraph(
    shared_ptr<const Query> query,
    const unordered_map<shared_ptr<const BlockMeta>, RawRequest>
        &requests)
{
    unordered_map<shared_ptr<const BlockMeta>, shared_ptr<Node>> graph;
    auto query_boundary = query->getFilterBoundary();
    unordered_set<shared_ptr<const BlockMeta>> all_blocks;
    for (auto it = requests.begin(); it != requests.end(); it++)
        all_blocks.insert(it->first);
    for (int i = 0; i < query->numOfMeasures(); i++)
    {
        auto blocks_in_measure = filterBlocks(
            all_blocks, query_boundary, *query->attributesInMeasure(i));
        for (auto b : blocks_in_measure)
            if (graph.count(b) == 0)
            {
                graph[b] = make_shared<Node>();
                graph[b]->request = requests.at(b);
            }

        // compare each pair of blocks
        for (auto b1 : blocks_in_measure)
            for (auto b2 : blocks_in_measure)
            {
                if (b1 == b2)
                    continue;
                const auto &b1_boundary = *b1->getBoundary();
                const auto &b2_boundary = *b2->getBoundary();
                if (b1_boundary.relationship(b2_boundary) !=
                    SET_RELATION::DISJOINT)
                {
                    graph[b1]->neighbors.insert(graph[b2]);
                    graph[b2]->neighbors.insert(graph[b1]);
                }
            }
    }
    return graph;
}

vector<unordered_map<shared_ptr<const BlockMeta>, RawRequest>>
partitionGraph(const unordered_map<shared_ptr<const BlockMeta>,
                                   shared_ptr<Node>> &graph)
{
    vector<unordered_map<shared_ptr<const BlockMeta>, RawRequest>>
        subgraphs;
    unordered_set<shared_ptr<const BlockMeta>> visited;

    auto dfs = [&](shared_ptr<const BlockMeta> block,
                   unordered_map<shared_ptr<const BlockMeta>,
                                 RawRequest> &subgraph,
                   auto &&dfs) {
        if (visited.count(block))
            return;
        visited.insert(block);
        const auto n = graph.at(block);
        subgraph[block] = n->request;
        for (auto i : n->neighbors)
            dfs(i->request.block, subgraph, dfs);
        return;
    };

    for (auto it = graph.begin(); it != graph.end(); it++)
    {
        unordered_map<shared_ptr<const BlockMeta>, RawRequest> subgraph;
        dfs(it->first, subgraph, dfs);
        if (subgraph.size() > 0)
            subgraphs.push_back(subgraph);
    }

    return subgraphs;
}
}; // namespace join

}; // namespace scan_parameter_internal

void produceScanParameterJoin(
    shared_ptr<const Query> query,
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const PartitionMeta>> &partitions,
    vector<shared_ptr<const ScanParameter>> &direct_params,
    vector<shared_ptr<const ScanParameter>> &recons_filter_params,
    vector<vector<shared_ptr<const ScanParameter>>>
        &recons_measure_params)
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
        map_filter_params, map_measure_params, map_direct_params;
    for (const auto &request : reconstruct_requests)
    {
        if (request.second.filter_requested_attributes.size() > 0)
        {
            map_filter_params[request.first] =
                scan_parameter_internal::join::produceReconstructFilter(
                    request.second);
        }
    }

    for (auto b : block_measures)
    {
        auto it = reconstruct_requests.find(b);
        if (it == reconstruct_requests.end())
            throw Exception(
                "produceScanParameterJoin: Cannot find request "
                "for a measure block");

        auto &request = it->second;
        if (request.passed_filter_attributes ==
            attributes_query_filters)
        {
            // the block can evaluate all predicates by block boundary
            // and the attributes in the block, without referring to
            // other blocks

            // reconstruct for measures
            if (!request.measure_requested_attributes.empty())
                map_measure_params[b] = scan_parameter_internal::join::
                    produceReconstructMeasure(request);

            auto it = map_measure_params.find(b);
            if (it != map_measure_params.end())
            {
                map_direct_params[b] =
                    scan_parameter_internal::join::produceDirect(
                        request, &it->second);
                it->second.direct_measures =
                    map_direct_params[b].direct_measures;
            }
            else
                map_direct_params[b] =
                    scan_parameter_internal::join::produceDirect(
                        request, nullptr);
            if (map_direct_params[b].direct_measures->count() == 0)
                map_direct_params.erase(b);
        }
        else
        {
            unordered_set<string> measure_attributes;
            for (int i = 0; i < measure_num; i++)
                measure_attributes.insert(
                    query->attributesInMeasure(i)->begin(),
                    query->attributesInMeasure(i)->end());
            auto t_request = request;
            t_request.request(measure_attributes, boundary_query, 1);
            t_request.finalize();
            map_measure_params[b] = scan_parameter_internal::join::
                produceReconstructMeasure(t_request);
            map_measure_params[b].direct_measures =
                make_shared<boost::dynamic_bitset<>>();
            map_measure_params[b].direct_measures->resize(measure_num,
                                                          false);
        }
    }

    // build and partition the graph
    auto graph = scan_parameter_internal::join::buildGraph(
        query, reconstruct_requests);
    auto sub_graphs =
        scan_parameter_internal::join::partitionGraph(graph);

    auto query_sub_filters =
        query->getFilter()->getSubExpressions("and");
    direct_params.clear();
    recons_filter_params.clear();
    recons_measure_params.clear();
    for (auto it = map_direct_params.begin();
         it != map_direct_params.end(); it++)
        direct_params.push_back(
            make_shared<ScanParameter>(it->second.produceScanParameter(
                table_schema, query_sub_filters, it->first)));
    for (auto it = map_filter_params.begin();
         it != map_filter_params.end(); it++)
        recons_filter_params.push_back(
            make_shared<ScanParameter>(it->second.produceScanParameter(
                table_schema, query_sub_filters, it->first)));

    for (const auto &sub : sub_graphs)
    {
        recons_measure_params.push_back(
            vector<shared_ptr<const ScanParameter>>());
        auto &v = recons_measure_params.back();

        for (auto &p : sub)
        {
            auto it = map_measure_params.find(p.first);
            if (it != map_measure_params.end())
                v.push_back(make_shared<ScanParameter>(
                    it->second.produceScanParameter(
                        table_schema, query_sub_filters, p.first)));
        }
        if (v.size() == 0)
            recons_measure_params.resize(recons_measure_params.size() -
                                         1);
    }
}