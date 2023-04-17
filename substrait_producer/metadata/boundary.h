#pragma once
#include "metadata/expression.h"
#include "metadata/interval.h"
#include "metadata/schema.h"
#include "substrait/partition.pb.h"
#include <cstring>
#include <unordered_map>

using namespace std;

class ComplexBoundary;

class Boundary
{
  public:
    Boundary(const unordered_map<string, shared_ptr<const Interval>>
                 &intervals);

    Boundary *clone() const;

    string toString() const;
    /**
     * @brief compute the set relationship of two boundaries. This
     * boundary is the superset of the other boundary if each interval
     * in this boundary is the superset of equal to the corresponding
     * interval of the other boundary. This boundary is the subset of
     * the other boundary if each interval in this boundary is the
     * subset of or equal to the corresponding subset of the other
     * interval. The two boundaries can contain different intervals. A
     * missing interval is the superset of any interval.
     *
     * @param other
     * @return SET_RELATION
     */
    SET_RELATION relationship(const Boundary &other) const;

    /**
     * @brief compute the intersection of two boundaries that are not
     * disjoint.
     *
     * @param other
     * @return Boundary
     */
    Boundary intersect(const Boundary &other) const;

    double intersectionRatio(const Boundary &other) const;

    double intersectionRatio(const ComplexBoundary &other) const;

    /**
     * @brief split the boundary into two boundaries by the point
     *
     * @param attribute the attribute to split
     * @param point the value to split
     * @param point_target true if the point will belong to the first
     * boundary and false if the point will belong to the second
     * boundary
     * @return vector<shared_ptr<Boundary>> return an empty set
     * if the boundary cannot be splited by the point
     */
    vector<shared_ptr<Boundary>> split(string attribute,
                                       shared_ptr<DataType> point,
                                       bool point_target) const;

    unordered_map<string, shared_ptr<const Interval>> getIntervals()
        const
    {
        return intervals;
    }

    unordered_set<string> getAttributes() const;

    bool isEmpty() const
    {
        return intervals.size() == 0;
    }

    void erase(const string &attribute_name)
    {
        intervals.erase(attribute_name);
    }

    void keepAttributes(const unordered_set<string> &attributes);

    shared_ptr<FunctionExpression> makeExpression() const;

    /**
     * @brief compute the bounding boundary of a set of boundaries. This
     * function compute the union of all attributes so that the result
     * boundary could cover values that are not covered by any input
     * boundary. For example, the union of {a0:[0, 10], a1: [0, 10]} and
     * {a0:[5, 15], a1: [5, 15]} is the boundary {a0:[0, 15], a1:[0,
     * 15]}.
     *
     * @param boundaries
     * @return Boundary
     */
    static Boundary Union(
        const vector<shared_ptr<const Boundary>> &boundaries);

  private:
    unordered_map<string, shared_ptr<const Interval>> intervals;
};

class PartitionMeta;

class BlockMeta
{
  public:
    BlockMeta(int id, shared_ptr<const Boundary> boundary,
              shared_ptr<const Schema> schema, PartitionMeta *partition,
              int64_t row_num = -1)
    {
        this->block_id = id;
        this->boundary = boundary;
        this->schema = schema;
        this->partition = partition;
        this->row_num = row_num;
    }

    BlockMeta *clone() const
    {
        return new BlockMeta(this->block_id, this->boundary,
                             this->schema, nullptr, this->row_num);
    }

    void setBoundary(shared_ptr<const Boundary> boundary)
    {
        this->boundary = boundary;
    }

    void setSchema(shared_ptr<const Schema> schema)
    {
        this->schema = schema;
    }

    /**
     * @brief Compute the relationship between the Block and the
     * requested synthetic block {boundary, attributes}. A block is the
     * superset of the other one if both the boundary and the schema are
     * the superset of or equal to the other one.
     *
     * @param boundary
     * @param attributes
     * @return SET_RELATION
     */
    SET_RELATION relationship(
        shared_ptr<const Boundary> boundary,
        const unordered_set<string> &attributes) const;

    shared_ptr<const Boundary> getBoundary() const
    {
        return boundary;
    }

    shared_ptr<const Schema> getSchema() const
    {
        return schema;
    }

    const PartitionMeta *getPartition() const
    {
        return partition;
    }

    int getBlockID() const
    {
        return block_id;
    }

    int64_t getRowNum() const
    {
        assert(row_num >= 0);
        return row_num;
    }

    int64_t estimateRowNum(const Boundary &boundary) const
    {
        if (row_num == -1)
            return -1;
        return row_num * this->boundary->intersectionRatio(boundary);
    }

    int64_t estimateRowNum(const ComplexBoundary &boundary) const
    {
        if (row_num == -1)
            return -1;
        return row_num * this->boundary->intersectionRatio(boundary);
    }

    size_t estimateIOSize(
        const unordered_set<string> &attributes) const;

    /**
     * @brief split the block into two blocks by the input value
     *
     * @param attribute the attribute to split
     * @param point the value where to split
     * @param point_target true if the point will belong to the first
     * block, false if the point will belong to the second block
     * @return vector<shared_ptr<BlockMeta>>
     */
    vector<shared_ptr<BlockMeta>> split(string attribute,
                                        shared_ptr<DataType> point,
                                        bool point_target) const;

    string toString() const;

    static shared_ptr<BlockMeta> parseSubstraitBlock(
        const substrait::Partition_Block *serialized,
        shared_ptr<const Schema> table_schema);

    void makeSubstraitBlock(
        substrait::Partition_Block *mutable_out,
        shared_ptr<const Schema> table_schema = nullptr) const;

  private:
    shared_ptr<const Boundary> boundary;
    shared_ptr<const Schema> schema;
    int block_id;
    int64_t row_num = -1;

    const PartitionMeta *partition;

    friend class PartitionMeta;
};

class PartitionMeta
{
  public:
    PartitionMeta(const string &path)
    {
        this->file_path = path;
    }

    vector<shared_ptr<const BlockMeta>> getBlocks() const
    {
        return blocks;
    }

    void addBlock(shared_ptr<BlockMeta> block)
    {
        block->partition = this;
        block->block_id = this->blocks.size();
        this->blocks.push_back(block);
    }

    string getPath() const
    {
        return file_path;
    }

    string toString() const;

    /**
     * @brief Parse a partition
     *
     * @param serialized
     * @param table_schema
     * @param root_path
     * @param find_file True if the function checks the boundary path is
     * a parquet file or a directory that only contains one parquet
     * file; False if skip the checking
     * @return shared_ptr<PartitionMeta>
     */
    static shared_ptr<PartitionMeta> parseSubstraitPartition(
        const substrait::Partition *serialized,
        shared_ptr<const Schema> table_schema, string root_path,
        bool find_file = false);

    void makeSubstraitPartition(
        substrait::Partition *mutable_out, int partition_id,
        shared_ptr<const Schema> table_schema = nullptr) const;

  private:
    vector<shared_ptr<const BlockMeta>> blocks;
    string file_path;
};