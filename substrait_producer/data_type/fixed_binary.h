#pragma once
#include "configuration.h"
#include "data_type/data_type.h"
#include "exceptions.h"
#include <boost/dynamic_bitset.hpp>
#include <math.h>
#include <string>

class FixedBinary : public DataType
{
  public:
    FixedBinary(int length)
    {
        this->value = boost::dynamic_bitset<>(length);
        this->type = DATA_TYPE::FIXEDBINARY;
    }

    FixedBinary(boost::dynamic_bitset<> value)
    {
        this->value = value;
        this->type = DATA_TYPE::FIXEDBINARY;
    }

    void set(int offset)
    {
        this->value.set(offset, true);
    }

    void reset(int offset)
    {
        this->value.set(offset, false);
    }

    boost::dynamic_bitset<> getValue() const
    {
        return this->value;
    }

    string getValueString() const
    {
        int num = std::ceil(value.size() / 8.0);
        uint8_t data[num];
        memset(data, 0, num);
        auto set_bit = [&](int offset) {
            data[offset / 8] |= (1 << (offset % 8));
        };
        for (int i = 0; i < value.size(); i++)
        {
            if (value.test(i))
                set_bit(i);
        }
        const char *c_data = reinterpret_cast<const char *>(data);
        std::string str;
        for (int i = 0; i < num; i++)
            str.push_back(c_data[i]);
        return str;
    }

    int cmp(const DataType *other) const
    {
        checkType(other);
        const FixedBinary *o = dynamic_cast<const FixedBinary *>(other);
        return this->value == o->value
                   ? 0
                   : (this->value < o->value ? -1 : 1);
    }

    inline double minius(const DataType *other) const override
    {
        throw Exception("FixedBinary::minius: unimplemented function");
    }

    inline DataType *middle(const DataType *other,
                            double ratio) const override
    {
        throw Exception("FixedBinary::middle: unimplemented function");
    }

    double distance(const DataType *other) const override
    {
        throw Exception(
            "FixedBinary::distance: unimplemented function");
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

        return "fixed_binary(" + getValueString() + ")";
    }

    DataType *clone() const
    {
        auto o = new FixedBinary(value.size());
        o->value = this->value;
        return o;
    }

    void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const
    {
        // if (InputParameter::get()->engine ==
        //     InputParameter::Engine::Arrow)
        //     *mutable_out->mutable_binary() = this->getValueString();
        // else
        //     *mutable_out->mutable_string() = this->getValueString();
        *mutable_out->mutable_binary() = this->getValueString();
    }

  protected:
    void checkType(const DataType *other) const
    {
        DataType::checkType(other);
        auto o = dynamic_cast<const FixedBinary *>(other);
        if (o == nullptr || o->value.size() != this->value.size())
            throw DataTypeNotMatchException();
    }

  private:
    boost::dynamic_bitset<> value;
};