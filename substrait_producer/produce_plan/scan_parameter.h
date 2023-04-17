#pragma once

#include "metadata/complex_boundary.h"
#include "metadata/expression.h"
#include "metadata/query.h"
#include <boost/dynamic_bitset.hpp>

/**
 * Define the parameter to read a block for tuple reconstruction and
 * directly evaluating aggregations
 **/
struct ScanParameter
{
    // The filter before reconstructing tuples.
    shared_ptr<const FunctionExpression> filter;
    shared_ptr<const ComplexBoundary> filter_boundary = nullptr;
    // shared_ptr<const Boundary> filter_boundary = nullptr;

    // The attributes to read. Each bit is an attribute in the table
    // schema
    boost::dynamic_bitset<> read_attributes;

    // The projected attributes after reading but before union
    boost::dynamic_bitset<> project_attributes;

    // The measures that are directly evaluated w/o reconstruction.
    // Should have the same value for reconstruction and direct
    // evaluation
    boost::dynamic_bitset<> direct_meassures;
    // The measures that contain any read attributes; (NOT USED)
    boost::dynamic_bitset<> possible_measures;
    // The predicates that will be passed after filtering but before
    // reconstruction. Ignored in the direct_evaluation path
    boost::dynamic_bitset<> passed_preds;

    // The path of the parquet file to read. One file is an irregular
    // partition
    string file_path;
    // The id of the rectangular block in the file
    unordered_set<int> block_id;
    unordered_set<shared_ptr<const BlockMeta>> blocks;

    shared_ptr<ScanParameter> clone() const
    {
        shared_ptr<ScanParameter> other = make_shared<ScanParameter>();
        other->file_path = file_path;
        other->block_id = block_id;
        other->blocks = blocks;
        other->filter = filter;
        other->filter_boundary = filter_boundary;
        other->read_attributes = read_attributes;
        other->project_attributes = project_attributes;
        other->direct_meassures = direct_meassures;
        other->possible_measures = possible_measures;
        other->passed_preds = passed_preds;
        return other;
    }

    bool equal(shared_ptr<const ScanParameter> other) const
    {
        bool filter_equal = false;
        if (this->filter && other->filter &&
            this->filter->equal(other->filter))
            filter_equal = true;
        else if (!this->filter && !other->filter)
            filter_equal = true;

        return this->file_path == other->file_path && filter_equal &&
               this->read_attributes == other->read_attributes &&
               this->project_attributes == other->project_attributes &&
               this->direct_meassures == other->direct_meassures &&
               this->possible_measures == other->possible_measures &&
               this->passed_preds == other->passed_preds;
    }

    string toString() const
    {
        string result =
            "Scan Parameter: {\n \t file: " + file_path + "\n";

        result += "\t block_id: {";
        for (auto b : block_id)
            result += std::to_string(b) + ", ";
        result += "}\n";

        if (this->filter)
            result += "\t filter: " + this->filter->toString() + "\n";

        auto bitset_to_string =
            [&](const boost::dynamic_bitset<> &bitset) -> string {
            size_t size = bitset.size();
            char c_str[size + 1] = {0};
            // memset(c_str, '0', size);
            for (size_t i = 0; i < size; i++)
                if (bitset.test(i))
                    c_str[i] = '1';
                else
                    c_str[i] = '0';
            return string(c_str);
        };

        result += "\t read_attributes: " +
                  bitset_to_string(this->read_attributes) + "\n";
        result += "\t project_attributes: " +
                  bitset_to_string(this->project_attributes) + "\n";
        result += "\t direct_measures: " +
                  bitset_to_string(this->direct_meassures) + "\n";
        result +=
            "\t passed_preds: " + bitset_to_string(this->passed_preds) +
            "\n";
        result += "}";
        return result;
    }
};
