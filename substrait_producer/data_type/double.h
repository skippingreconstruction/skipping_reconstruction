#pragma once
#include "data_type/data_type.h"
#include "exceptions.h"
#include <math.h>

class Double : public DataType
{
  public:
    Double(int precision = 0) : Double(0, precision)
    {
    }
    Double(double value, int precision)
    {
        this->precision = precision;
        double f = pow(10, precision);
        this->value = round(value * f);
        this->type = DATA_TYPE::DOUBLE;
    }

    inline int cmp(const DataType *other) const;
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
        Double *o = new Double(this->precision);
        o->value = this->value + d;
        return o;
    }

    bool prev()
    {
        this->value--;
        return true;
    }
    bool next()
    {
        this->value++;
        return true;
    }

    string toString() const
    {
        return "double(" + std::to_string(this->getValue()) + ")";
    }

    double getValue() const
    {
        return value * pow(0.1, precision);
    }

    DataType *clone() const
    {
        return new Double(this->getValue(), this->precision);
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        mutable_out->set_fp64(this->getValue());
    }

    static shared_ptr<Double> makeDefault()
    {
        return make_shared<Double>(0, 0);
    }

  protected:
    inline void checkType(const DataType *other) const;

  private:
    int64_t value;
    int precision;
};

void Double::checkType(const DataType *other) const
{
    DataType::checkType(other);
    auto o = dynamic_cast<const Double *>(other);
    if (o->precision != this->precision)
        throw DoubleWithDifferentPrecisionException();
}

int Double::cmp(const DataType *other) const
{
    checkType(other);
    const Double *o = (const Double *)other;
    return this->value == o->value ? 0
                                   : (this->value < o->value ? -1 : 1);
}

double Double::minius(const DataType *other) const
{
    checkType(other);
    const Double *o = (const Double *)other;
    return this->value - o->value;
}