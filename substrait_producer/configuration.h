#pragma once

#include "data_type/data_type.h"
#include "substrait/partition.pb.h"
#include <fstream>
#include <string>

// // define the consumer engine. Could be VELOX or ARROW
// #define ARROW 1
// #define VELOX 2
// #define ENGINE VELOX

enum SET_RELATION
{
    EQUAL,
    SUBSET,
    INTERSECT,
    SUPERSET,
    DISJOINT
};

const std::string kUDFURI =
    "file:///home/user/code/hierarchical-partitioning/"
    "substrait_arrow_producer/udf.yaml";
const std::string kComparisonURI =
    "https://github.com/substrait-io/substrait/blob/main/extensions/"
    "functions_comparison.yaml";
const std::string kBooleanURI =
    "https://github.com/substrait-io/substrait/blob/main/extensions/"
    "functions_boolean.yaml";
const std::string kArithmeticURI =
    "https://github.com/substrait-io/substrait/blob/main/extensions/"
    "functions_arithmetic.yaml";

const string block_id_name = "block_id";
const string passed_pred_name = "passed_preds";
const string direct_measures_name = "direct_measures";
const string possible_measures_name = "possible_measures";
const string valid_attribute_name = "valid_attributes";
const string tuple_id_name = "tid";

void setMinMax(const string &attribute,
               shared_ptr<const DataType> min_value,
               shared_ptr<const DataType> max_value);
shared_ptr<const DataType> getMinValue(const string &attribute);
shared_ptr<const DataType> getMaxValue(const string &attribute);
unordered_set<string> getMinMaxAttributes();

template <typename T> void readSubstrait(T *serialized, string path)
{
    std::ifstream ifile(path, ios::in | ios::binary);
    serialized->ParseFromIstream(&ifile);
    ifile.close();
}

struct InputParameter
{

    enum AggParallelMethod
    {
        None,
        Partition // partition the data by tuple id before
                  // reconstruction; Arrow currently does not support
                  // this feature
    };

    enum Engine
    {
        Arrow,
        Velox
    };

    enum ReconstructType
    {
        Join,
        Aggregate
    };

    // the path of the data folder. Should be started with \"file://\""
    string data_path;
    // the schema file
    string schema_path;
    // the table range path, storing the min/max values
    string table_range_path;
    // the file storing the metadata of partitions
    string partition_path;
    string query_path;
    // the output dir
    string plan_dir;

    ReconstructType reconstruct = ReconstructType::Aggregate;

    Engine engine = Engine::Arrow;

    AggParallelMethod parallel = AggParallelMethod::None;

    static shared_ptr<const InputParameter> parse(int argc,
                                                  char const *argv[]);
    static shared_ptr<const InputParameter> get()
    {
        return parameter;
    }
    static shared_ptr<InputParameter> parameter;
};
