#pragma once

#include "configuration.h"
#include "metadata/expression.h"
#include "substrait/type.pb.h"
#include <boost/dynamic_bitset.hpp>
#include <cstring>
#include <memory>
#include <unordered_map>

using namespace std;

class Schema
{
  public:
    void add(shared_ptr<Attribute> attribute);
    void append(shared_ptr<const Schema> other);

    const shared_ptr<Attribute> get(const string &name) const;
    const shared_ptr<Attribute> get(int offset) const;
    shared_ptr<Schema> get(
        const boost::dynamic_bitset<> &offsets) const;
    int getOffset(const string &name) const;
    /**
     * @brief Find the offset of other's attributes in this schema
     *
     * @param other
     * @return boost::dynamic_bitset<>
     */
    boost::dynamic_bitset<> getOffsets(
        shared_ptr<const Schema> other) const;

    unordered_set<string> getAttributeNames() const;

    bool contains(const string &name) const;
    void rename(const string &src_name, const string &dest_name);

    string toString() const;

    /**
     * @brief Compute the relationship between the attributes in the
     * schema and the input set of attributes.
     *
     * @param attributes
     * @return SET_RELATION
     */
    SET_RELATION relationship(
        const unordered_set<string> &attributes) const;

    size_t size() const
    {
        return vattr.size();
    }

    bool equal(shared_ptr<const Schema> other) const;

    static shared_ptr<Schema> parseSubstraitSchema(
        substrait::NamedStruct *serialized);

  private:
    unordered_map<string, shared_ptr<Attribute>> mattr;
    vector<shared_ptr<Attribute>> vattr;
};