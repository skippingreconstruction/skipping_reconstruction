#pragma once
#include "metadata/schema.h"
#include "produce_plan/scan_parameter.h"
#include "substrait/plan.pb.h"
#include <variant>

class MiniTable
{
  public:
    MiniTable(shared_ptr<const Schema> table_schema)
        : table_schema(table_schema)
    {
        mini_schema = make_shared<Schema>();
    }

    MiniTable(
        shared_ptr<const Schema> table_schema,
        const unordered_set<shared_ptr<const ScanParameter>> &blocks);

    void addDataBlock(shared_ptr<const ScanParameter>);

    shared_ptr<const Schema> getSchema() const
    {
        return mini_schema;
    }

    vector<shared_ptr<const ScanParameter>> getDataBlocks() const
    {
        return data_blocks;
    }

    shared_ptr<Schema> makeSubstraitRel(::substrait::Rel *rel) const;

    string toString() const;

  private:
    vector<shared_ptr<const ScanParameter>> data_blocks;
    shared_ptr<Schema> mini_schema;
    shared_ptr<const Schema> table_schema;
};

struct FilterParameter;

struct JoinParameter
{
    // Prob tale of the hash join
    shared_ptr<MiniTable> child_left;

    // Build table of the hash join
    variant<shared_ptr<MiniTable>, shared_ptr<FilterParameter>>
        child_right;

    shared_ptr<Schema> makeSubstraitRel(
        ::substrait::Rel *rel,
        shared_ptr<const Schema> table_schema) const;

    string toString() const;
};

struct FilterParameter
{
    shared_ptr<JoinParameter> child;

    // all these attributes must be valid in the output of child
    boost::dynamic_bitset<> expect_valid;

    // the key attribute must be valid if any attribute of the value is
    // valid
    unordered_map<int, boost::dynamic_bitset<>> expect_same;

    bool needFilter() const
    {
        return expect_valid.count() > 0 || expect_same.size() > 0;
    }

    string toString() const;

    shared_ptr<Schema> makeSubstraitRel(
        ::substrait::Rel *rel,
        shared_ptr<const Schema> table_schema) const;
};

variant<shared_ptr<MiniTable>, shared_ptr<FilterParameter>>
makeJoinSequence(
    shared_ptr<const Schema> table_schema,
    const vector<shared_ptr<const ScanParameter>> &scan_parameters);