#include "produce_plan/produce_scan_parameter.h"
#include "metadata/boundary.h"

namespace scan_parameter_internal
{
unordered_set<shared_ptr<const BlockMeta>> filterBlocks(
    const unordered_set<shared_ptr<const BlockMeta>> &blocks,
    shared_ptr<const Boundary> filter,
    const unordered_set<string> &attributes)
{
    unordered_set<shared_ptr<const BlockMeta>> result;
    for (auto b : blocks)
    {
        if (b->relationship(filter, attributes) !=
            SET_RELATION::DISJOINT)
            result.insert(b);
    }
    return result;
}

shared_ptr<Boundary> convergeBoundary(shared_ptr<const Boundary> source,
                                      shared_ptr<const Boundary> target)
{
    Boundary b = source->intersect(*target);
    // remove the intervals from b that are equal to the source
    auto intervals_b = b.getIntervals();
    auto intervals_s = source->getIntervals();
    unordered_map<string, shared_ptr<const Interval>> intervals_result;
    for (auto it = intervals_b.begin(); it != intervals_b.end(); it++)
    {
        auto it_s = intervals_s.find(it->first);
        if (it_s == intervals_s.end() ||
            it->second->relationship(*it_s->second) !=
                SET_RELATION::EQUAL)
            intervals_result.emplace(it->first, it->second);
    }
    return make_shared<Boundary>(intervals_result);
}

shared_ptr<ComplexBoundary> convergeBoundary(
    shared_ptr<const Boundary> source,
    shared_ptr<const ComplexBoundary> target)
{
    ComplexBoundary b = target->intersect(*source);
    auto intervals_b = b.getIntervals();
    auto intervals_s = source->getIntervals();
    unordered_set<string> keep_attributes;
    for (auto it = intervals_b.begin(); it != intervals_b.end(); it++)
    {
        auto it_s = intervals_s.find(it->first);
        if (it_s == intervals_s.end())
            keep_attributes.insert(it->first);
        else if (it->second.size() != 1 ||
                 it->second[0]->relationship(*it_s->second) !=
                     SET_RELATION::EQUAL)
            keep_attributes.insert(it->first);
    }
    b.keepAttributes(keep_attributes);
    return make_shared<ComplexBoundary>(b);
}

string RawRequest::toString() const
{
    string s = "RawRequest{\n\tpartition:";
    s += block->getPartition()->getPath() + "\n\tblock_id:";
    s += std::to_string(block->getBlockID()) +
         "\n\tfilter_requested_attributes:{";
    for (auto a : filter_requested_attributes)
        s += a + ",";
    s += "}\n\tmeasure_requested_attributes:{";
    for (auto a : measure_requested_attributes)
        s += a + ",";
    s += "}\n\tfilter_requested_filters:{";
    for (auto f : filter_requested_filters)
        s += "\t\t" + f->toString() + "\n";
    s += "}\n\tmeasure_requested_filters:{";
    for (auto f : measure_requested_filters)
        s += "\t\t" + f->toString() + "\n";
    s += "}\n\tpassed_filter_attributes:";
    for (auto a : passed_filter_attributes)
        s += a;
    s += "\n\textra_check_filter_attributes:";
    for (auto a : extra_check_filter_attributes)
        s += a;
    s += "\n}";
    return s;
}

RawRequest *RawRequest::clone() const
{
    RawRequest *new_request = new RawRequest();
    new_request->block = this->block;
    new_request->query = this->query;
    new_request->filter_requested_attributes =
        this->filter_requested_attributes;
    new_request->measure_requested_attributes =
        this->measure_requested_attributes;
    new_request->passed_filter_attributes =
        this->passed_filter_attributes;
    new_request->extra_check_filter_attributes =
        this->extra_check_filter_attributes;

    for (auto f : this->filter_requested_filters)
        new_request->filter_requested_filters.push_back(
            shared_ptr<const Boundary>(f->clone()));
    for (auto f : this->measure_requested_filters)
        new_request->measure_requested_filters.push_back(
            shared_ptr<const Boundary>(f->clone()));
    return new_request;
}

void RawRequest::add_attributes(unordered_set<string> &target,
                                const unordered_set<string> &attributes)
{
    auto attribute_block = block->getSchema()->getAttributeNames();
    auto intersection = set_intersection(attribute_block, attributes);
    target.insert(intersection.begin(), intersection.end());
}

void RawRequest::request(const unordered_set<string> &attributes,
                         shared_ptr<const Boundary> filter, int type)
{
    switch (type)
    {
    case 0:
        add_attributes(filter_requested_attributes, attributes);
        if (filter)
            this->filter_requested_filters.push_back(filter);
        break;
    case 1:
        add_attributes(measure_requested_attributes, attributes);
        if (filter)
            this->measure_requested_filters.push_back(filter);
        break;
    default:
        throw Exception("RawRequest:request Unknow request type " +
                        to_string(type));
    }
}

void RawRequest::intersectFilter(const Boundary &b)
{
    for (int i = 0; i < this->filter_requested_filters.size(); i++)
        filter_requested_filters[i] = make_shared<Boundary>(
            filter_requested_filters[i]->intersect(b));
    for (int i = 0; i < this->measure_requested_filters.size(); i++)
        measure_requested_filters[i] = make_shared<Boundary>(
            measure_requested_filters[i]->intersect(b));
}

void RawRequest::finalize()
{
    // remove requested attributes that are not in the block
    for (auto it = this->filter_requested_attributes.begin();
         it != this->filter_requested_attributes.end();)
    {
        if (block->getSchema()->contains(*it))
            it++;
        else
            it = this->filter_requested_attributes.erase(it);
    }
    for (auto it = this->measure_requested_attributes.begin();
         it != this->measure_requested_attributes.end();)
    {
        if (block->getSchema()->contains(*it))
            it++;
        else
            it = this->measure_requested_attributes.erase(it);
    }

    for (auto it = extra_check_filter_attributes.begin();
         it != extra_check_filter_attributes.end();)
        if (block->getSchema()->contains(*it))
            it++;
        else
            it = this->extra_check_filter_attributes.erase(it);

    // intersect filters
    intersectFilter(
        block->getBoundary()->intersect(*query->getFilterBoundary()));
}

ScanParameter RawScanParameter::produceScanParameter(
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const FunctionExpression>>
        &sub_filters_in_query,
    shared_ptr<const BlockMeta> block) const
{
    ScanParameter p;
    p.read_attributes.resize(table_schema->size(), false);
    for (auto i : read_attributes)
        p.read_attributes.set(table_schema->getOffset(i));
    p.project_attributes.resize(table_schema->size(), false);
    for (auto i : project_attributes)
        p.project_attributes.set(table_schema->getOffset(i));
    p.direct_meassures = *this->direct_measures;

    p.passed_preds.resize(sub_filters_in_query.size(), false);
    for (int i = 0; i < sub_filters_in_query.size(); i++)
    {
        auto attributes = sub_filters_in_query[i]->getAttributes();
        if (attributes.size() != 1)
            throw Exception("The atom sub expression in query "
                            "predicate must have one attribtue");
        string a = *attributes.begin();
        if (this->passed_attributes.count(a))
            p.passed_preds.set(i);
    }
    p.filter_boundary = this->filters;
    p.filter = this->filters->makeExpression();

    p.block_id = {block->getBlockID()};
    p.blocks = {block};
    p.file_path = block->getPartition()->getPath();

    return p;
}

void postRequests(
    shared_ptr<const Query> query,
    const unordered_set<shared_ptr<const BlockMeta>> &target_blocks,
    shared_ptr<const Boundary> filter,
    const unordered_set<string> &attributes, int request_type,
    unordered_map<shared_ptr<const BlockMeta>, RawRequest> &requests)
{
    for (auto b : target_blocks)
    {
        if (requests.count(b) == 0)
            requests[b] = {b, query};
        requests[b].request(attributes, filter, request_type);
    }
}

unordered_map<shared_ptr<const BlockMeta>, RawRequest> postRequests(
    shared_ptr<const Query> query,
    const unordered_set<shared_ptr<const BlockMeta>> &block_measures,
    const unordered_set<shared_ptr<const BlockMeta>> &block_filters)
{
    unordered_map<shared_ptr<const BlockMeta>, RawRequest> requests;

    auto boundary_query = query->getFilterBoundary();
    auto query_filter_attributes = boundary_query->getAttributes();
    int measure_num = query->numOfMeasures();

    for (auto b : block_measures)
    {
        auto boundary_block = b->getBoundary();
        auto schema_block = b->getSchema();
        auto attributes_block = schema_block->getAttributeNames();
        // skip the block if the block boundary is disjoint with the
        // query boundary (should not happen)
        auto filter_rel = boundary_block->relationship(*boundary_query);
        if (filter_rel == SET_RELATION::DISJOINT)
            continue;

        shared_ptr<Boundary> boundary_block_query =
            make_shared<Boundary>(
                boundary_block->intersect(*boundary_query));
        // post the missing attributes requested in measures
        for (int i = 0; i < measure_num; i++)
        {
            auto attributes_measure = query->attributesInMeasure(i);
            auto attributes_diff =
                set_difference(*attributes_measure, attributes_block);
            if (attributes_diff.size() == attributes_measure->size())
                // the block does not contain any attributes in the
                // measure
                continue;
            else if (attributes_diff.size() == 0)
                // the block contains all requested attributes in the
                // measure
                continue;

            // find all blocks from block_measures that contain the
            // missing attributes and then post requests to the target
            // blocks in order to read the missing attributes
            auto target_blocks = filterBlocks(
                block_measures, boundary_block_query, attributes_diff);
            postRequests(query, target_blocks, boundary_block_query,
                         attributes_diff, 1, requests);
        }

        if (requests.count(b) == 0)
            requests[b] = {b, query};

        if (filter_rel == SET_RELATION::SUBSET ||
            filter_rel == SET_RELATION::EQUAL)
        {
            // the entire data block pass all predicates
            requests[b].passed_filter_attributes =
                query_filter_attributes;
        }
        else if (filter_rel == SET_RELATION::INTERSECT ||
                 filter_rel == SET_RELATION::SUPERSET)
        {
            // post the missing filters if the block is not fully
            // covered by the query predicates

            auto boundary_extra =
                convergeBoundary(boundary_block, boundary_query);
            // the attributes that we should read and evaluate
            // predicates on
            auto boundary_extra_attributes =
                boundary_extra->getAttributes();

            // only request attributes that are not in the data block
            auto extra_attributes_not_in_block = set_difference(
                boundary_extra_attributes, attributes_block);
            if (extra_attributes_not_in_block.size() > 0)
            {
                auto target_blocks =
                    filterBlocks(block_filters, boundary_block_query,
                                 extra_attributes_not_in_block);
                postRequests(query, target_blocks, boundary_block_query,
                             extra_attributes_not_in_block, 0,
                             requests);
            }

            requests[b].passed_filter_attributes = set_difference(
                query_filter_attributes, extra_attributes_not_in_block);
            requests[b].extra_check_filter_attributes =
                set_intersection(boundary_extra_attributes,
                                 attributes_block);
        }
    }

    for (auto it = requests.begin(); it != requests.end(); it++)
    {
        if (block_measures.count(it->first) == 0)
        {
            // the block is a pure filter block. Compute the
            // passed_filter_attributes and
            // extra_check_filter_attributes
            auto &request = it->second;
            auto boundary_extra_attributes =
                convergeBoundary(request.block->getBoundary(),
                                 boundary_query)
                    ->getAttributes();
            auto block_attributes =
                request.block->getSchema()->getAttributeNames();
            auto extra_attributes_not_in_block = set_difference(
                boundary_extra_attributes, block_attributes);
            request.passed_filter_attributes = set_difference(
                query_filter_attributes, extra_attributes_not_in_block);
            request.extra_check_filter_attributes = set_intersection(
                boundary_extra_attributes, block_attributes);
        }
        it->second.finalize();
    }

    return requests;
}
}; // namespace scan_parameter_internal
