#include "configuration.h"
#include <fstream>
#include <unordered_map>

using namespace std;

unordered_map<string, shared_ptr<const DataType>> MinValues;
unordered_map<string, shared_ptr<const DataType>> MaxValues;

unordered_set<string> getMinMaxAttributes()
{
    unordered_set<string> ans;
    for (auto &s : MinValues)
        ans.insert(s.first);
    return ans;
}

void setMinMax(const string &attribute,
               shared_ptr<const DataType> min_value,
               shared_ptr<const DataType> max_value)
{
    MinValues[attribute] = min_value;
    MaxValues[attribute] = max_value;
}

shared_ptr<const DataType> getMinValue(const string &attribute)
{
    auto it = MinValues.find(attribute);
    if (it == MinValues.end())
        throw Exception(
            "getMinValue: Miss the min value of attribute " +
            attribute);
    return it->second;
}

shared_ptr<const DataType> getMaxValue(const string &attribute)
{
    auto it = MaxValues.find(attribute);
    if (it == MaxValues.end())
        throw Exception(
            "getMaxValue: Miss the max value of attribute " +
            attribute);
    return it->second;
}

shared_ptr<InputParameter> InputParameter::parameter = nullptr;

shared_ptr<const InputParameter> InputParameter::parse(
    int argc, char const *argv[])
{
    parameter = make_shared<InputParameter>();
    int idx = 1;
    while (idx < argc)
    {
        string op = argv[idx++];
        if (op == "--data_path")
            parameter->data_path = argv[idx++];
        else if (op == "--schema_path")
            parameter->schema_path = argv[idx++];
        else if (op == "--table_range")
            parameter->table_range_path = argv[idx++];
        else if (op == "--partition_path")
            parameter->partition_path = argv[idx++];
        else if (op == "--query_path")
            parameter->query_path = argv[idx++];
        else if (op == "--plan_dir")
            parameter->plan_dir = argv[idx++];
        else if (op == "--engine")
        {
            string engine = argv[idx++];
            std::transform(engine.begin(), engine.end(), engine.begin(),
                           ::tolower);
            if (engine == "arrow")
                parameter->engine = Engine::Arrow;
            else if (engine == "velox")
                parameter->engine = Engine::Velox;
            else
                throw Exception("Invalid engine " + engine);
        }
        else if (op == "--reconstruct-type")
        {
            string type = argv[idx++];
            std::transform(type.begin(), type.end(), type.begin(),
                           ::tolower);
            if (type == "join")
                parameter->reconstruct = ReconstructType::Join;
            else if (type == "aggregate")
                parameter->reconstruct = ReconstructType::Aggregate;
            else
                throw Exception("Invalid reconstruct-type " + type);
        }
        else if (op == "--parallel-partition")
            parameter->parallel = AggParallelMethod::Partition;
        else
            throw Exception("Unknow input parameter " + op);
    }
    return parameter;
}