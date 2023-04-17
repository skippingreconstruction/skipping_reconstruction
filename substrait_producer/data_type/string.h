#pragma once
#include "data_type/data_type.h"
#include "exceptions.h"

#include <string>

class String : public DataType
{
  public:
    String() : String("")
    {
    }
    String(const std::string &value)
    {
        this->value = value;
        this->type = DATA_TYPE::STRING;
    }
    inline int cmp(const DataType *other) const;

    double minius(const DataType *other) const override
    {
        throw Exception("String::minius: unimplemented function");
    }

    double distance(const DataType *other) const override
    {
        throw Exception("String::distance: unimplemented function");
    }

    inline DataType *middle(const DataType *other,
                            double ratio) const override
    {
        throw Exception("String::middle: unimplemented function");
    }

    bool prev()
    {
        throw UnimplementedFunctionException();
        return false;
    }
    bool next()
    {
        throw UnimplementedFunctionException();
        return false;
    }

    string toString() const
    {
        return "string(" + this->getValue() + ")";
    }

    DataType *clone() const
    {
        return new String(this->value);
    }

    string getValue() const
    {
        return value;
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        mutable_out->set_string(this->getValue());
    }

  private:
    std::string value;
};

int String::cmp(const DataType *other) const
{
    checkType(other);
    String *o = (String *)other;
    return this->value.compare(o->value);
}