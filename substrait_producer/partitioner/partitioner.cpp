#include "baselines/produce_scan_parameter.h"
#include "google/protobuf/util/json_util.h"
#include "metadata/boundary.h"
#include "metadata/query.h"
#include "metadata/schema.h"
#include "partitioner/common.h"
#include "partitioner/hierarchical_partitioner.h"
#include "partitioner/horizontal_partitioner.h"
#include "partitioner/model.h"
#include "produce_plan/produce_scan_parameter.h"
#include "substrait/partition.pb.h"
#include <boost/functional/hash.hpp>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>

void setMinMax(const Boundary *boundary)
{
    auto intervals = boundary->getIntervals();
    for (auto p : intervals)
        setMinMax(p.first, p.second->getMin(), p.second->getMax());
}

int main(int argc, char const *argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    auto parameter = PartitionParameter::parse(argc, argv);

    // parse schema
    shared_ptr<Schema> table_schema;
    shared_ptr<BlockMeta> root_block;
    unordered_set<shared_ptr<const Query>> queries, validate_queries,
        test_queries;

    {
        substrait::NamedStruct s;
        readSubstrait(&s, parameter.schema_path);
        table_schema = Schema::parseSubstraitSchema(&s);
    }
    // set min/max
    {
        substrait::Partition s;
        readSubstrait(&s, parameter.table_range_path);
        auto p = PartitionMeta::parseSubstraitPartition(
            &s, table_schema, "", false);
        root_block = shared_ptr<BlockMeta>(p->getBlocks()[0]->clone());
        setMinMax(root_block->getBoundary().get());
        unordered_map<string, shared_ptr<const Interval>>
            empty_intervals;
        root_block->setBoundary(make_shared<Boundary>(empty_intervals));
    }
    // parse query
    {
        {
            substrait::Plan p;
            readSubstrait(&p, parameter.query_path);
            auto q = Query::parseSubstraitQuery(&p, table_schema, "");
            for (auto i : q)
                queries.insert(i);
        }

        {
            substrait::Plan p;
            readSubstrait(&p, parameter.validation_path);
            auto q = Query::parseSubstraitQuery(&p, table_schema, "");
            for (auto i : q)
                validate_queries.insert(i);
        }

        if (parameter.test_query_path.length() > 0)
        {
            substrait::Plan p;
            readSubstrait(&p, parameter.test_query_path);
            auto q = Query::parseSubstraitQuery(&p, table_schema, "");
            for (auto i : q)
                test_queries.insert(i);
        }
    }

    substrait::PartitionList plist;
    vector<shared_ptr<const BlockMeta>> blocks;

    auto produceParams = produceScanParameters;
    auto aggModel = predictAggTimeEarly;
    if (parameter.partition_type == PartitionParameter::Horizontal)
        blocks =
            horizontalPartition(root_block, queries, stopByRowNum, {});
    else if (parameter.partition_type ==
             PartitionParameter::Hierarchical_Late)
    {
        produceParams = produceScanParametersAggregation;
        aggModel = predictAggTimeLate;
        blocks = hierarchicalPartition(root_block, queries,
                                       validate_queries, stopByRowNum,
                                       produceParams, aggModel);
    }
    else
    {
        blocks = hierarchicalPartition(root_block, queries,
                                       validate_queries, stopByRowNum,
                                       produceParams, aggModel);
    }

    if (test_queries.size())
    {
        estimateCost(blocks, test_queries, table_schema, produceParams,
                     aggModel);
    }

    // get all used attributes in the query or in the partitioning
    // boundary
    boost::dynamic_bitset<> accessed_attributes(table_schema->size());
    for (auto q : validate_queries)
    {
        auto attr = q->getAllReferredAttributes();
        for (auto a : attr)
            accessed_attributes.set(table_schema->getOffset(a));
    }
    for (auto b : blocks)
    {
        auto attr = table_schema->getOffsets(b->getSchema());
        if ((attr & accessed_attributes).count() == 0)
            continue;
        auto boundary_attr = b->getBoundary()->getAttributes();
        for (auto a : boundary_attr)
            accessed_attributes.set(table_schema->getOffset(a));
    }

    // find all vertical partitions
    vector<pair<boost::dynamic_bitset<>,
                vector<shared_ptr<const BlockMeta>>>>
        column_blocks;
    for (auto b : blocks)
    {
        // skip the block if it does not contain any used attributes
        auto attr = table_schema->getOffsets(b->getSchema());
        if ((accessed_attributes & attr).count() == 0)
            continue;
        auto it = column_blocks.begin();
        for (; it != column_blocks.end(); it++)
        {
            if (it->first == attr)
                break;
        }
        if (it == column_blocks.end())
        {
            column_blocks.push_back(
                make_pair(attr, vector<shared_ptr<const BlockMeta>>{}));
            it = column_blocks.end() - 1;
        }

        it->second.push_back(b);
    }

    int pid = 0;
    for (auto it = column_blocks.begin(); it != column_blocks.end();
         it++)
    {
        PartitionMeta p("");
        for (auto b : it->second)
        {
            shared_ptr<BlockMeta> i = shared_ptr<BlockMeta>(b->clone());
            p.addBlock(i);
            cout << i->toString() << endl;
        }
        p.makeSubstraitPartition(plist.add_partitions(), pid++,
                                 table_schema);
    }

    for (auto it = column_blocks.begin(); it != column_blocks.end();
         it++)
    {
        auto attr = table_schema->get(it->first);
        int64_t tnum = 0;
        for (auto b : it->second)
            tnum += b->getRowNum();
        printf("Schema %s has %lld tuples\n", attr->toString().c_str(),
               tnum);
    }

    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    string plist_str;
    if (!google::protobuf::util::MessageToJsonString(plist, &plist_str,
                                                     options)
             .ok())
    {
        std::cerr << "Failed to serialize partitioning layout" << endl;
        return -1;
    }

    ofstream ofile(parameter.partition_path + "_readable", ios::trunc);
    ofile << plist_str;
    ofile.close();
    ofstream ofile_binary(parameter.partition_path,
                          ios::trunc | ios::binary);
    plist.SerializeToOstream(&ofile_binary);
    ofile_binary.close();

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
