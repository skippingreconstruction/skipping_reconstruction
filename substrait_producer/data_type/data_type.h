#pragma once

#include "exceptions.h"
#include "substrait/plan.pb.h"

enum DATA_TYPE
{
    INTEGER,
    DOUBLE,
    STRINGENUM,
    STRING,
    BOOLEAN,
    FIXEDBINARY
};

void makeSubstraitType(substrait::Type *mutable_out, DATA_TYPE type,
                       bool nullable, bool upper);
DATA_TYPE parseSubstraitType(const substrait::Type *type,
                             bool &nullable);

class DataType
{
  public:
    virtual ~DataType()
    {
    }
    virtual int cmp(const DataType *other) const = 0;
    virtual bool prev() = 0;
    virtual bool next() = 0;
    virtual DataType *clone() const = 0;
    virtual string toString() const = 0;
    virtual double minius(const DataType *other) const = 0;
    /**
     * @brief compute the distance between this and other values. Both
     * values are inclusive
     *
     * @param other
     * @return double must be greater than 0.
     */
    virtual double distance(const DataType *other) const = 0;

    /**
     * @brief compute a point between [this, other]
     *
     * @param other the larger end point
     * @param ratio must be in [0, 1]
     * @return DataType*
     */
    virtual DataType *middle(const DataType *other,
                             double ratio) const = 0;

    inline const DataType *min(const DataType *other) const;
    inline const DataType *max(const DataType *other) const;

    virtual void makeSubstraitLiteral(
        substrait::Expression_Literal *mutable_out) const = 0;

    DATA_TYPE getType() const
    {
        return type;
    }

    static shared_ptr<DataType> parseSubstraitLiteral(
        const substrait::Expression_Literal *serialized);

  protected:
    inline virtual void checkType(const DataType *other) const;
    DATA_TYPE type;
};

const DataType *DataType::min(const DataType *other) const
{
    return this->cmp(other) <= 0 ? this : other;
}

const DataType *DataType::max(const DataType *other) const
{
    return this->cmp(other) >= 0 ? this : other;
}

void DataType::checkType(const DataType *other) const
{
    if (other == nullptr || other->getType() != this->getType())
        throw DataTypeNotMatchException();
}
