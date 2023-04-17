#include "metadata/schema.h"
#include "configuration.h"

const shared_ptr<Attribute> Schema::get(const string &name) const
{
    if (mattr.find(name) == mattr.end())
        throw Exception("Cannot find attribute " + name + " in schema");
    return mattr.at(name);
}

const shared_ptr<Attribute> Schema::get(int offset) const
{
    if (vattr.size() <= offset || offset < 0)
        throw Exception("Invalid offset " + std::to_string(offset));
    return vattr[offset];
}

int Schema::getOffset(const string &name) const
{
    for (size_t i = 0; i < vattr.size(); i++)
    {
        if (vattr[i]->getName() == name)
            return i;
    }
    return -1;
}

boost::dynamic_bitset<> Schema::getOffsets(
    shared_ptr<const Schema> other) const
{
    boost::dynamic_bitset<> bits(this->size(), false);
    auto other_names = other->getAttributeNames();
    for (auto a : other_names)
    {
        int idx = this->getOffset(a);
        if (idx < 0)
            throw Exception("Schema::getOffsets: cannot find " + a);
        bits.set(idx);
    }
    return bits;
}

bool Schema::contains(const string &name) const
{
    return mattr.find(name) != mattr.end();
}

void Schema::add(shared_ptr<Attribute> attr)
{
    if (mattr.find(attr->getName()) != mattr.end())
        throw Exception("The schema already has attribute " +
                        attr->getName());
    vattr.push_back(attr);
    mattr[attr->getName()] = attr;
}

void Schema::append(shared_ptr<const Schema> other)
{
    for (auto a : other->vattr)
    {
        if (mattr.find(a->getName()) != mattr.end())
            continue;
        add(a);
    }
}

shared_ptr<Schema> Schema::get(
    const boost::dynamic_bitset<> &offsets) const
{
    if (offsets.size() != vattr.size())
        throw Exception("Bitmap length is not correct");
    shared_ptr<Schema> ret = make_shared<Schema>();
    for (auto it = offsets.find_first();
         it != boost::dynamic_bitset<>::npos;
         it = offsets.find_next(it))
    {
        ret->add(this->get(it));
    }
    return std::move(ret);
}

bool Schema::equal(shared_ptr<const Schema> other) const
{
    if (vattr.size() != other->vattr.size())
        return false;
    for (int i = 0; i < vattr.size(); i++)
        if (!vattr[i]->equal(other->vattr[i]))
            return false;
    return true;
}

SET_RELATION Schema::relationship(
    const unordered_set<string> &attributes) const
{
    bool found = false;
    for (auto it = mattr.begin(); it != mattr.end() && !found; it++)
        if (attributes.count(it->first))
            found = true;
    if (!found && mattr.size() != 0)
        return SET_RELATION::DISJOINT;

    bool has_unique[]{false, false};
    for (auto it = mattr.begin();
         it != mattr.end() && has_unique[0] == false; it++)
        if (attributes.count(it->first) == 0)
            has_unique[0] = true;
    for (auto it = attributes.begin();
         it != attributes.end() && has_unique[1] == false; it++)
        if (mattr.count(*it) == 0)
            has_unique[1] = true;
    if (has_unique[0] == false && has_unique[1] == false)
        return SET_RELATION::EQUAL;
    else if (has_unique[0] == true && has_unique[1] == false)
        return SET_RELATION::SUPERSET;
    else if (has_unique[0] == false && has_unique[1] == true)
        return SET_RELATION::SUBSET;
    else
        return SET_RELATION::INTERSECT;
}

unordered_set<string> Schema::getAttributeNames() const
{
    unordered_set<string> result;
    for (auto it = mattr.begin(); it != mattr.end(); it++)
        result.insert(it->first);
    return result;
}

void Schema::rename(const string &src_name, const string &dest_name)
{
    if (!contains(src_name))
        throw Exception(
            "Schema::rename: the schema does not contain attribute " +
            src_name);
    if (contains(dest_name))
        throw Exception(
            "Schema::rename: the schema already has the attribute " +
            dest_name);
    auto a = mattr.at(src_name);
    a->setName(dest_name);
    mattr.erase(src_name);
    mattr[dest_name] = a;
}

string Schema::toString() const
{
    string result = "Schema: { ";
    for (auto &a : vattr)
        result += a->toString() + ", ";
    result += "}";
    return result;
}

shared_ptr<Schema> Schema::parseSubstraitSchema(
    substrait::NamedStruct *serialized)
{
    shared_ptr<Schema> schema = make_shared<Schema>();
    int size = serialized->names_size();

    for (int i = 0; i < size; i++)
    {
        string name = serialized->names(i);
        auto t = serialized->struct_().types(i);
        std::optional<size_t> size = std::nullopt;
        if (serialized->sizes_size() > 0)
            size = serialized->sizes(i);
        bool nullability;
        shared_ptr<Attribute> a = make_shared<Attribute>(
            name, parseSubstraitType(&t, nullability), size);
        schema->add(std::move(a));
    }
    return schema;
}