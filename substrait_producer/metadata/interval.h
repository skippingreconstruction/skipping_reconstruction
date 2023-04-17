#pragma once
#include "configuration.h"
#include "data_type/data_type_api.h"
#include "metadata/expression.h"
#include <string>

class Interval
{
  public:
    Interval(int left_value, bool left_open, int right_value,
             bool right_open);
    Interval(double left_value, bool left_open, double right_value,
             bool right_open, int precision);
    Interval(const std::string &left_value, bool left_open,
             const std::string &right_value, bool right_open,
             StringEnum::StringEnumList *allStrings);
    Interval(shared_ptr<const DataType> left_value, bool left_open,
             shared_ptr<const DataType> right_value, bool right_open);

    Interval(const Interval &other);
    ~Interval();

    Interval *clone() const;

    void setMin(shared_ptr<const DataType> value, bool open);
    void setMax(shared_ptr<const DataType> value, bool open);
    shared_ptr<DataType> getMin() const;
    shared_ptr<DataType> getMax() const;

    DATA_TYPE getType() const
    {
        return value[0]->getType();
    }

    string toString() const
    {
        return "[" + this->value[0]->toString() + ", " +
               this->value[1]->toString() + "]";
    }

    /**
     * @brief get the relationship of two intervals
     *
     * @param other
     * @return SET_RELATION::EQUAL if the two intervals have the same
     * values; SET_RELATION::SUBSET if this interval is the subset
     * (fully covered) of the other interval; SET_RELATION::SUPERSET if
     * this interval is the superset of the other interval;
     * SET_RELATION::DISJOINT if the two intervals disjoint
     */
    SET_RELATION relationship(const Interval &other) const;

    // bool isIntersect(const Interval &other);
    /**
     * @brief Get the intersection of two Intervals. The relationship of
     * the two intervals cannot be SET_RELATION::DISJOINT
     *
     * @param other
     * @return Interval
     */
    Interval interesct(const Interval &other) const;

    double intersectionRatio(const Interval &other) const;

    /**
     * @brief split the interval to two intervals by the point
     *
     * @param point
     * @param point_target true if the point will belong to the first
     * interval, and false if the point will belong to the second
     * interval
     * @return vector<shared_ptr<Interval>> return an empty set
     * if the point is not in the interval
     */
    vector<shared_ptr<Interval>> split(shared_ptr<DataType> point,
                                       bool point_target) const;

    shared_ptr<FunctionExpression> makeExpression(
        const string &attribute_name) const;

    /**
     * @brief compute the union of a set of intervals. The output
     * interval is the min/max of all intervals
     *
     * @param intervals
     * @return Interval
     */
    static Interval Union(
        const vector<shared_ptr<const Interval>> &intervals);

  private:
    void init(DataType *left_value, bool left_open,
              DataType *right_value, bool right_open);
    DataType *value[2];
};