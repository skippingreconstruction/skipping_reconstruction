#pragma once
#include "data_type/data_type.h"
#include "exceptions.h"

class Boolean : public DataType
{
  public:
    Boolean(bool value = false) : value(value)
    {
        this->type = DATA_TYPE::BOOLEAN;
    };
    inline int cmp(const DataType *other) const;
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

    inline double minius(const DataType *other) const override;

    inline double distance(const DataType *other) const override
    {
        return abs(this->minius(other)) + 1;
    }

    inline DataType *middle(const DataType *other,
                            double ratio) const override
    {
        assert(ratio >= 0 && ratio <= 1);
        double d = other->minius(this) * ratio;
        assert(d >= 0);
        return new Boolean((int)(this->value + d));
    }

    string toString() const
    {
        return "bool(" + std::to_string(this->getValue()) + ")";
    }

    DataType *clone() const
    {
        return new Boolean(this->value);
    }

    bool getValue() const
    {
        return value;
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        mutable_out->set_boolean(value);
    }

  private:
    bool value;
};

int Boolean::cmp(const DataType *other) const
{
    checkType(other);
    const Boolean *o = (const Boolean *)other;
    return this->value == o->value ? 0
                                   : (this->value < o->value ? -1 : 1);
}

double Boolean::minius(const DataType *other) const
{
    checkType(other);
    const Boolean *o = (const Boolean *)other;
    return (int)this->value - (int)o->value;
}