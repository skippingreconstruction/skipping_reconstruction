#pragma once
#include "data_type/data_type.h"
#include "exceptions.h"

class Integer : public DataType
{
  public:
    Integer() : Integer(0, 32)
    {
    }
    Integer(int64_t value, int size)
    {
        this->value = value;
        this->type = DATA_TYPE::INTEGER;
        this->size = size;
    }
    inline int cmp(const DataType *other) const;
    inline double minius(const DataType *other) const override;

    inline DataType *middle(const DataType *other,
                            double ratio) const override
    {
        assert(ratio >= 0 && ratio <= 1);
        int64_t d = other->minius(this) * ratio;
        return new Integer(this->value + d, this->size);
    }

    double distance(const DataType *other) const
    {
        return abs(this->minius(other)) + 1;
    }

    inline bool prev()
    {
        this->value--;
        return true;
    }
    inline bool next()
    {
        this->value++;
        return true;
    }

    string toString() const
    {
        return "int(" + std::to_string(this->getValue()) + ")";
    }

    DataType *clone() const
    {
        return new Integer(this->value, this->size);
    }

    int64_t getValue() const
    {
        return value;
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        switch (size)
        {
        case 8:
            mutable_out->set_i8(value);
            break;
        case 16:
            mutable_out->set_i16(value);
            break;
        case 32:
            mutable_out->set_i32(value);
            break;
        case 64:
            mutable_out->set_i64(value);
            break;
        default:
            throw Exception(
                "Integer::makeSubstraitLiteral: Invalid size " +
                std::to_string(size));
        }
    }

  private:
    int64_t value;
    int size;
};

int Integer::cmp(const DataType *other) const
{
    checkType(other);
    const Integer *o = (const Integer *)other;
    return this->value == o->value ? 0
                                   : (this->value < o->value ? -1 : 1);
}

double Integer::minius(const DataType *other) const
{
    checkType(other);
    const Integer *o = (const Integer *)other;
    return this->value - o->value;
}