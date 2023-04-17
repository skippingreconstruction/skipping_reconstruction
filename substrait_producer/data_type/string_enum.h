#pragma once
#include "data_type/data_type.h"
#include "exceptions.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
using namespace std;

class StringEnum : public DataType
{
  public:
    class StringEnumList
    {
      public:
        StringEnumList(const unordered_set<string> &allStrings)
        {
            for (auto it = allStrings.begin(); it != allStrings.end();
                 ++it)
                this->allStrings.push_back(*it);

            for (int i = 0; i < this->allStrings.size(); i++)
                indexes.insert({this->allStrings[i], i});
        }
        inline int indexOf(const string &str) const;
        int size() const
        {
            return allStrings.size();
        }
        string get(int index) const
        {
            return allStrings[index];
        }

      private:
        vector<string> allStrings;
        unordered_map<string, int> indexes;
    };

    StringEnum(StringEnumList *allStrings)
    {
        this->allStrings = allStrings;
        this->index = -1;
        this->type = DATA_TYPE::STRINGENUM;
    }

    StringEnum(const std::string &value, StringEnumList *allStrings)
    {
        this->allStrings = allStrings;
        this->index = allStrings->indexOf(value);
        if (this->index < 0)
            throw DataTypeException();
        this->type = DATA_TYPE::STRINGENUM;
    }

    inline int cmp(const DataType *other) const;
    inline double minius(const DataType *other) const override;

    double distance(const DataType *other) const override
    {
        return abs(minius(other)) + 1;
    }

    inline DataType *middle(const DataType *other,
                            double ratio) const override
    {
        assert(ratio >= 0 && ratio <= 1);
        int d = other->minius(this) * ratio;
        StringEnum *o = (StringEnum *)(this->clone());
        o->index += d;
        return o;
    }

    inline bool prev();
    inline bool next();

    string toString() const
    {
        return "string_enum(" + this->toString() + ")";
    }

    DataType *clone() const
    {
        StringEnum *target = new StringEnum(allStrings);
        target->index = this->index;
        return target;
    }

    string getValue() const
    {
        return allStrings->get(index);
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        mutable_out->set_string(this->getValue());
    }

  protected:
    inline void checkType(const DataType *other) const;

  private:
    StringEnumList *allStrings;
    int index;
};

int StringEnum::StringEnumList::indexOf(const string &str) const
{
    if (indexes.find(str) == indexes.end())
        return -1;
    return indexes.at(str);
}

void StringEnum::checkType(const DataType *other) const
{
    DataType::checkType(other);
    auto o = dynamic_cast<const StringEnum *>(other);
    if (o == nullptr || this->allStrings != o->allStrings)
        throw DataTypeNotMatchException();
}

int StringEnum::cmp(const DataType *other) const
{
    checkType(other);
    const StringEnum *o = (const StringEnum *)other;
    return this->index == o->index  ? 0
           : this->index < o->index ? -1
                                    : 1;
}

bool StringEnum::prev()
{
    if (this->index == 0)
        return false;
    this->index--;
    return true;
}

bool StringEnum::next()
{
    if (this->index == this->allStrings->size() - 1)
        return false;
    this->index++;
    return true;
}

double StringEnum::minius(const DataType *other) const
{
    checkType(other);
    const StringEnum *o = (const StringEnum *)other;
    return this->index - o->index;
}