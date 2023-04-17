#include "metadata/boundary.h"
#include "metadata/complex_boundary.h"
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

Boundary::Boundary(
    const unordered_map<string, shared_ptr<const Interval>> &intervals)
{
    this->intervals = intervals;
}

string Boundary::toString() const
{
    string result = "Boundary: { ";
    for (auto it = this->intervals.begin(); it != this->intervals.end();
         it++)
    {
        result += it->first + ": " + it->second->toString() + ", ";
    }
    result += "}";
    return result;
}

Boundary *Boundary::clone() const
{
    unordered_map<string, shared_ptr<const Interval>> new_intervals;
    for (auto it : this->intervals)
        new_intervals[it.first] =
            shared_ptr<const Interval>(it.second->clone());
    return new Boundary(new_intervals);
}

SET_RELATION Boundary::relationship(const Boundary &other) const
{
    // fill the missing attributes of the two boundaries
    auto b = this->intervals;
    auto b_other = other.intervals;
    for (auto it = b.begin(); it != b.end(); it++)
    {
        auto it_o = b_other.find(it->first);
        if (it_o == b_other.end())
            b_other[it->first] = make_shared<const Interval>(
                getMinValue(it->first), false, getMaxValue(it->first),
                false);
        else if (it->second->getType() != it_o->second->getType())
            throw Exception(
                "Boundaries do not have the same type at attribute " +
                it->first);
    }
    for (auto it_o = b_other.begin(); it_o != b_other.end(); it_o++)
        if (b.find(it_o->first) == b.end())
            b[it_o->first] = make_shared<const Interval>(
                getMinValue(it_o->first), false,
                getMaxValue(it_o->first), false);

    // compare each attribute in both boundaries
    SET_RELATION relation = SET_RELATION::EQUAL;
    for (auto it = b.begin(); it != b.end(); it++)
    {
        auto it_o = b_other.find(it->first);
        auto r = it->second->relationship(*it_o->second);
        switch (r)
        {
        case SET_RELATION::DISJOINT:
            return SET_RELATION::DISJOINT;
            break;
        case SET_RELATION::EQUAL:
            break;
        case SET_RELATION::INTERSECT:
            relation = SET_RELATION::INTERSECT;
            break;
        case SET_RELATION::SUBSET:
            if (relation == SET_RELATION::SUBSET ||
                relation == SET_RELATION::EQUAL)
                relation = SET_RELATION::SUBSET;
            else // if the relation is intersect or superset
                relation = SET_RELATION::INTERSECT;
            break;
        case SET_RELATION::SUPERSET:
            if (relation == SET_RELATION::SUPERSET ||
                relation == SET_RELATION::EQUAL)
                relation = SET_RELATION::SUPERSET;
            else
                relation = SET_RELATION::INTERSECT;
            break;
        default:
            throw Exception(
                "Boundary::relationship: Unknow interval relation");
            break;
        }
    }
    return relation;
}

Boundary Boundary::intersect(const Boundary &other) const
{
    if (this->relationship(other) == SET_RELATION::DISJOINT)
        throw Exception(
            "Boundary::intersect: two bondaries are disjoint");

    unordered_map<string, shared_ptr<const Interval>> m;
    for (auto it = intervals.begin(); it != intervals.end(); it++)
    {
        auto it1 = other.intervals.find(it->first);
        if (it1 == other.intervals.end())
            m[it->first] = it->second;
        else
            m[it->first] = make_shared<const Interval>(
                it->second->interesct(*it1->second));
    }

    for (auto it1 = other.intervals.begin();
         it1 != other.intervals.end(); it1++)
        if (this->intervals.find(it1->first) == this->intervals.end())
            m[it1->first] = it1->second;
    return Boundary(m);
}

double Boundary::intersectionRatio(const Boundary &other) const
{
    const Boundary &inter = this->intersect(other);
    double ratio = 1;
    for (auto it = inter.intervals.begin(); it != inter.intervals.end();
         it++)
    {
        auto it_this = this->intervals.find(it->first);
        if (it_this == this->intervals.end())
        {
            auto min_val = getMinValue(it->first);
            auto max_val = getMaxValue(it->first);
            Interval i(min_val, false, max_val, false);
            ratio *= i.intersectionRatio(*it->second);
        }
        else
            ratio *= it_this->second->intersectionRatio(*it->second);
    }
    return ratio;
}

double Boundary::intersectionRatio(const ComplexBoundary &other) const
{
    const ComplexBoundary &inter = other.intersect(*this);
    const auto &other_inters = inter.getIntervals();
    double ratio = 1;
    for (auto it = other_inters.begin(); it != other_inters.end(); it++)
    {
        auto it_this = this->intervals.find(it->first);
        shared_ptr<const Interval> this_i;
        if (it_this == this->intervals.end())
            this_i =
                make_shared<Interval>(getMinValue(it->first), false,
                                      getMaxValue(it->first), false);
        else
            this_i = it_this->second;

        double r = 0;
        for (auto i : it->second)
        {
            if (this_i->relationship(*i) == SET_RELATION::DISJOINT)
                continue;
            r += this_i->intersectionRatio(*i);
        }
        assert(r > 0 && r <= 1);
        ratio *= r;
    }
    return ratio;
}

vector<shared_ptr<Boundary>> Boundary::split(string attribute,
                                             shared_ptr<DataType> point,
                                             bool point_target) const
{
    shared_ptr<const Interval> point_interval;
    if (intervals.count(attribute))
        point_interval = intervals.at(attribute);
    else
    {
        auto min_val = getMinValue(attribute);
        auto max_val = getMaxValue(attribute);
        point_interval =
            make_shared<Interval>(min_val, false, max_val, false);
    }

    // split the interval
    auto split_interval = point_interval->split(point, point_target);
    if (split_interval.size() == 0)
        return {};

    auto b1 = shared_ptr<Boundary>(this->clone()),
         b2 = shared_ptr<Boundary>(this->clone());
    b1->intervals[attribute] = split_interval[0];
    b2->intervals[attribute] = split_interval[1];
    return {b1, b2};
}

unordered_set<string> Boundary::getAttributes() const
{
    unordered_set<string> attributes;
    for (auto it = intervals.begin(); it != intervals.end(); it++)
        attributes.insert(it->first);
    return attributes;
}

Boundary Boundary::Union(
    const vector<shared_ptr<const Boundary>> &boundaries)
{
    if (boundaries.size() == 0)
        throw Exception("Boundary::Union: the input must at least have "
                        "one boundary");
    unordered_map<string, vector<shared_ptr<const Interval>>> intervals;
    for (auto b : boundaries)
    {
        auto &i = b->intervals;
        for (auto it = i.begin(); it != i.end(); it++)
        {
            if (intervals.find(it->first) == intervals.end())
                intervals[it->first] = {};
            intervals[it->first].push_back(it->second);
        }
    }

    unordered_map<string, shared_ptr<const Interval>> result_interval;
    for (auto it = intervals.begin(); it != intervals.end(); it++)
    {
        if (it->second.size() != boundaries.size())
            // If any boundary does not have constraint on one
            // attribute, the union on that attribute should be the full
            // value range
            continue;
        result_interval[it->first] =
            make_shared<Interval>(Interval::Union(it->second));
    }
    return Boundary(result_interval);
}

void Boundary::keepAttributes(const unordered_set<string> &attributes)
{
    for (auto it = intervals.begin(); it != intervals.end();)
    {
        if (attributes.count(it->first) == 0)
            it = intervals.erase(it);
        else
            it++;
    }
}

shared_ptr<FunctionExpression> Boundary::makeExpression() const
{
    if (intervals.size() == 0)
        return nullptr;
    auto it = intervals.begin();
    auto baseExp = it->second->makeExpression(it->first);
    it++;
    for (; it != intervals.end(); it++)
    {
        auto e = it->second->makeExpression(it->first);
        baseExp = make_shared<FunctionExpression>(
            "filter_exp", "and",
            vector<shared_ptr<const Expression>>{baseExp, e},
            DATA_TYPE::BOOLEAN, false);
    }
    return baseExp;
}

size_t BlockMeta::estimateIOSize(
    const unordered_set<string> &attributes) const
{
    size_t row_size = 0;
    for (const string &a : attributes)
    {
        if (!schema->contains(a))
            continue;
        auto attr = schema->get(a);
        row_size += attr->getSize();
    }
    if (row_num < 0)
        throw Exception(
            "BlockMeta::estimateIOSize: The row_num is unintialized");
    return row_size * row_num;
}

vector<shared_ptr<BlockMeta>> BlockMeta::split(
    string attribute, shared_ptr<DataType> point,
    bool point_target) const
{
    auto split_boundary =
        this->boundary->split(attribute, point, point_target);
    if (split_boundary.size() == 0)
        return {};
    assert(split_boundary.size() == 2);
    int64_t b1_tnum = this->estimateRowNum(*split_boundary[0]);
    int64_t b2_tnum =
        this->row_num == -1 ? -1 : this->row_num - b1_tnum;
    if (this->row_num != -1 && (b1_tnum < 0 || b2_tnum < 0 ||
                                b1_tnum + b2_tnum != this->row_num))
        throw Exception(
            "BlockMeta::split: row_num computation is not valid");
    return {make_shared<BlockMeta>(0, split_boundary[0], this->schema,
                                   nullptr, b1_tnum),
            make_shared<BlockMeta>(0, split_boundary[1], this->schema,
                                   nullptr, b2_tnum)};
}

SET_RELATION BlockMeta::relationship(
    shared_ptr<const Boundary> other_boundary,
    const unordered_set<string> &other_attributes) const
{
    auto boundary_relation =
        this->boundary->relationship(*other_boundary);
    auto attribute_relation =
        this->schema->relationship(other_attributes);
    if (boundary_relation == SET_RELATION::DISJOINT ||
        attribute_relation == SET_RELATION::DISJOINT)
        return SET_RELATION::DISJOINT;
    else if (boundary_relation == SET_RELATION::EQUAL &&
             attribute_relation == SET_RELATION::EQUAL)
        return SET_RELATION::EQUAL;
    else if ((boundary_relation == SET_RELATION::SUPERSET ||
              boundary_relation == SET_RELATION::EQUAL) &&
             (attribute_relation == SET_RELATION::SUPERSET ||
              attribute_relation == SET_RELATION::EQUAL))
        return SET_RELATION::SUPERSET;
    else if ((boundary_relation == SET_RELATION::SUBSET ||
              boundary_relation == SET_RELATION::EQUAL) &&
             (attribute_relation == SET_RELATION::SUBSET ||
              attribute_relation == SET_RELATION::EQUAL))
        return SET_RELATION::SUBSET;
    else
        return SET_RELATION::INTERSECT;
}

string BlockMeta::toString() const
{
    string result =
        "Block{ block_id: " + std::to_string(block_id) + ", ";
    result += this->schema->toString() + ", ";
    result += this->boundary->toString() + ", ";
    if (this->row_num > 0)
        result += "row_num: " + to_string(this->row_num) + ", ";
    result += "}";
    return result;
}

string PartitionMeta::toString() const
{
    string result = "Partition: { " + this->file_path + ", \n";
    for (auto b : blocks)
        result += "\t " + b->toString() + "\n";
    result += "}";
    return result;
}

shared_ptr<BlockMeta> BlockMeta::parseSubstraitBlock(
    const substrait::Partition_Block *serialized,
    shared_ptr<const Schema> table_schema)
{
    int bid = serialized->block_id();
    shared_ptr<Schema> block_schema = make_shared<Schema>();
    for (int i = 0; i < serialized->attributes_size(); i++)
    {
        auto a = serialized->attributes(i);
        block_schema->add(table_schema->get(a));
    }

    unordered_map<string, shared_ptr<const Interval>> intervals;
    for (int i = 0; i < serialized->boundary_size(); i++)
    {
        auto &interval = serialized->boundary(i);
        auto &low = interval.low();
        auto &high = interval.high();
        shared_ptr<Interval> si = make_shared<Interval>(
            DataType::parseSubstraitLiteral(&low), false,
            DataType::parseSubstraitLiteral(&high), false);
        intervals.emplace(interval.attribute(), std::move(si));
    }

    int64_t row_num = -1;
    if (serialized->has_rows_num())
        row_num = serialized->rows_num();
    return make_shared<BlockMeta>(bid, make_shared<Boundary>(intervals),
                                  block_schema, nullptr, row_num);
}

void BlockMeta::makeSubstraitBlock(
    substrait::Partition_Block *mutable_out,
    shared_ptr<const Schema> table_schema) const
{
    mutable_out->set_block_id(this->block_id);
    auto attribute_names = this->schema->getAttributeNames();
    if (table_schema)
    {
        for (int i = 0; i < table_schema->size(); i++)
        {
            string a = table_schema->get(i)->getName();
            if (attribute_names.count(a))
                mutable_out->add_attributes(a);
        }
    }
    else
    {
        for (auto a : attribute_names)
            mutable_out->add_attributes(a);
    }

    if (this->row_num >= 0)
        mutable_out->set_rows_num(this->row_num);

    auto intervals = this->boundary->getIntervals();
    for (auto it = intervals.begin(); it != intervals.end(); it++)
    {
        auto interval_out = mutable_out->add_boundary();
        interval_out->set_attribute(it->first);
        it->second->getMin()->makeSubstraitLiteral(
            interval_out->mutable_low());
        it->second->getMax()->makeSubstraitLiteral(
            interval_out->mutable_high());
    }
}

string findParquet(const string &path, const string &suffix)
{
    const string prefix = "file://";
    string path_wo_prefix = path;
    if (path.substr(0, prefix.length()) == prefix)
        path_wo_prefix = path.substr(prefix.length());

    auto check_suffix = [](const string &path,
                           const string &suffix) -> bool {
        return path.substr(path.length() - suffix.length()) == suffix;
    };
    if (fs::is_regular_file(path_wo_prefix))
    {
        if (check_suffix(path_wo_prefix, suffix))
            return path;
        else
            throw Exception(
                "findParquet: the suffix of the path is not " + suffix);
    }
    else if (fs::is_directory(path_wo_prefix))
    {
        string file_name = "";
        for (const auto &entry : fs::directory_iterator(path_wo_prefix))
        {
            if (!fs::is_regular_file(fs::status(entry.path())) ||
                !check_suffix(entry.path(), suffix))
                continue;
            else if (file_name.length() != 0)
                throw Exception("findParquet: dir " + path +
                                " has more than one parquet file");
            else
                file_name = entry.path();
        }

        if (file_name.length() == 0)
            throw Exception("findParquet: dir " + path +
                            " does not have parquet file");
        return prefix + file_name;
    }
    else
        throw Exception("findParquet: path " + path +
                        " is not a directory or a file");
}

shared_ptr<PartitionMeta> PartitionMeta::parseSubstraitPartition(
    const substrait::Partition *serialized,
    shared_ptr<const Schema> table_schema, string root_path,
    bool find_file)
{
    string path = serialized->path();
    vector<shared_ptr<BlockMeta>> blocks(serialized->blocks_size());
    for (int i = 0; i < serialized->blocks_size(); i++)
    {
        auto &serialized_block = serialized->blocks(i);
        auto b = BlockMeta::parseSubstraitBlock(&serialized_block,
                                                table_schema);
        if (b->block_id < 0 || blocks[b->block_id])
            throw Exception("PartitionMeta::parseSubstraitPartition: "
                            "Invalid blocks");
        blocks[b->block_id] = b;
    }

    path = root_path + "/" + path;
    if (find_file)
        path = findParquet(path, ".parquet");
    shared_ptr<PartitionMeta> partition =
        make_shared<PartitionMeta>(path);
    for (auto b : blocks)
        partition->addBlock(b);
    return partition;
}

void PartitionMeta::makeSubstraitPartition(
    substrait::Partition *mutable_out, int partition_id,
    shared_ptr<const Schema> table_schema) const
{
    mutable_out->set_partition_id(partition_id);
    mutable_out->set_path(this->file_path);
    for (auto b : blocks)
        b->makeSubstraitBlock(mutable_out->add_blocks(), table_schema);
}