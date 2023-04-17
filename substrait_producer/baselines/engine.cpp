#include "baselines/make_plan_base.h"
#include "baselines/produce_scan_parameter.h"
#include "configuration.h"
#include "google/protobuf/util/json_util.h"
#include "metadata/boundary.h"
#include "metadata/query.h"
#include "metadata/schema.h"
#include "produce_plan/build_substrait.h"
#include <google/protobuf/text_format.h>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

void setMinMax(const Boundary *boundary)
{
    auto intervals = boundary->getIntervals();
    for (auto p : intervals)
        setMinMax(p.first, p.second->getMin(), p.second->getMax());
}

int main(int argc, char const *argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    auto parameter = InputParameter::parse(argc, argv);

    // parse schema
    shared_ptr<Schema> table_schema;
    vector<shared_ptr<const PartitionMeta>> partitions;
    vector<shared_ptr<Query>> queries;
    {
        substrait::NamedStruct s;
        readSubstrait(&s, parameter->schema_path);
        table_schema = Schema::parseSubstraitSchema(&s);
    }

    // set min/max
    {
        substrait::Partition s;
        readSubstrait(&s, parameter->table_range_path);
        auto p = PartitionMeta::parseSubstraitPartition(
            &s, table_schema, parameter->data_path);
        setMinMax(p->getBlocks()[0]->getBoundary().get());
    }

    // parse partitions
    {
        substrait::PartitionList s;
        readSubstrait(&s, parameter->partition_path);
        for (int i = 0; i < s.partitions_size(); i++)
        {
            bool check_path = false;
            if (parameter->engine == InputParameter::Engine::Velox)
                check_path = true;
            auto &p = s.partitions(i);
            partitions.push_back(PartitionMeta::parseSubstraitPartition(
                &p, table_schema, parameter->data_path, check_path));
        }
    }
    // parse query
    {
        substrait::Plan p;
        readSubstrait(&p, parameter->query_path);
        queries = Query::parseSubstraitQuery(&p, table_schema,
                                             parameter->data_path);
    }

    // evalaute queries
    for (int i = 0; i < queries.size(); i++)
    {
        substrait::Plan plan;
        registFunctions(&plan);
        auto rel = plan.add_relations()->mutable_rel();
        auto scan_parameters =
            produceScanParameters(queries[i], table_schema, partitions);

        evaluate(rel, table_schema, queries[i], scan_parameters.second,
                 scan_parameters.first);

        // write plan
        string plan_path =
            parameter->plan_dir + "/q" + std::to_string(i);
        // ofstream ofile(plan_path, ios::trunc | ios::binary);
        // if (!plan.SerializeToOstream(&ofile))
        // {
        //     std::cerr << "Failed to write plan" << endl;
        //     return -1;
        // }
        // ofile.close();

        ofstream ofile(plan_path, ios::trunc);
        string plan_str;
        if (!google::protobuf::TextFormat::PrintToString(plan,
                                                         &plan_str))
        {
            std::cerr << "Failed to serialize plan" << endl;
            return -1;
        }
        ofile << plan_str;
        ofile.close();
    }
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
