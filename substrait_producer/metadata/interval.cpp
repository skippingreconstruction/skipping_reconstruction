#include "metadata/interval.h"
#include "exceptions.h"

void Interval::init(DataType *left_value, bool left_open,
                    DataType *right_value, bool right_open)
{
    value[0] = left_value;
    value[1] = right_value;
    if (left_open && !value[0]->next())
        throw IntervalException();
    if (right_open && !value[1]->prev())
        throw IntervalException();
    // the left value cannot be larger than the right value
    if (value[0]->cmp(value[1]) > 0)
        throw IntervalException();
}

Interval::Interval(int left_value, bool left_open, int right_value,
                   bool right_open)
{
    Integer *i1 = new Integer(left_value, 32);
    Integer *i2 = new Integer(right_value, 32);
    init(i1, left_open, i2, right_open);
}

Interval::Interval(double left_value, bool left_open,
                   double right_value, bool right_open, int precision)
{
    Double *d1 = new Double(left_value, precision);
    Double *d2 = new Double(right_value, precision);
    init(d1, left_open, d2, right_open);
}

Interval::Interval(const std::string &left_value, bool left_open,
                   const std::string &right_value, bool right_open,
                   StringEnum::StringEnumList *allStrings)
{
    StringEnum *s1 = new StringEnum(left_value, allStrings);
    StringEnum *s2 = new StringEnum(right_value, allStrings);
    init(s1, left_open, s2, right_open);
}

Interval::Interval(shared_ptr<const DataType> left_value,
                   bool left_open,
                   shared_ptr<const DataType> right_value,
                   bool right_open)
{
    init(left_value->clone(), left_open, right_value->clone(),
         right_open);
}

Interval::Interval(const Interval &other)
{
    this->value[0] = other.value[0]->clone();
    this->value[1] = other.value[1]->clone();
}

Interval::~Interval()
{
    delete this->value[0];
    delete this->value[1];
}

Interval *Interval::clone() const
{
    return new Interval(*this);
}

void Interval::setMax(shared_ptr<const DataType> value, bool open)
{
    this->value[1] = value->clone();
    if (open && !this->value[1]->prev())
        throw Exception("Interval::setMax: cannot get the prev of " +
                        this->value[1]->toString());
}

void Interval::setMin(shared_ptr<const DataType> value, bool open)
{
    this->value[0] = value->clone();
    if (open && !this->value[0]->next())
        throw Exception("Interval::setMin: cannot get the next of " +
                        this->value[0]->toString());
}

shared_ptr<DataType> Interval::getMin() const
{
    return shared_ptr<DataType>(this->value[0]->clone());
}

shared_ptr<DataType> Interval::getMax() const
{
    return shared_ptr<DataType>(this->value[1]->clone());
}

SET_RELATION Interval::relationship(const Interval &other) const
{
    const DataType *max_left = this->value[0]->max(other.value[0]);
    const DataType *min_right = this->value[1]->min(other.value[1]);
    if (max_left->cmp(min_right) > 0)
        return SET_RELATION::DISJOINT;
    int cmp[2] = {this->value[0]->cmp(other.value[0]),
                  this->value[1]->cmp(other.value[1])};
    if (cmp[0] == 0 && cmp[1] == 0)
        return SET_RELATION::EQUAL;
    else if (cmp[0] >= 0 && cmp[1] <= 0)
        return SET_RELATION::SUBSET;
    else if (cmp[0] <= 0 && cmp[1] >= 0)
        return SET_RELATION::SUPERSET;
    else
        return SET_RELATION::INTERSECT;
}

// bool Interval::isIntersect(const Interval &other)
// {
//     DataType *max_left = this->value[0]->max(other.value[0]);
//     DataType *min_right = this->value[1]->min(other.value[1]);
//     return max_left->cmp(min_right) <= 0;
// }

Interval Interval::interesct(const Interval &other) const
{
    if (relationship(other) == SET_RELATION::DISJOINT)
        throw IntervalException();
    const DataType *max_left = this->value[0]->max(other.value[0]);
    const DataType *min_right = this->value[1]->min(other.value[1]);
    return Interval(
        shared_ptr<const DataType>(max_left->clone()), false,
        shared_ptr<const DataType>(min_right->clone()), false);
}

double Interval::intersectionRatio(const Interval &other) const
{
    const Interval &inter = this->interesct(other);
    double dist[2] = {inter.value[1]->distance(inter.value[0]),
                      this->value[1]->distance(this->value[0])};

    if (dist[0] <= 0 || dist[1] <= 0 || dist[0] > dist[1])
        throw Exception(
            "Interval::intersectionRatio: Invalid interval");
    return dist[0] / dist[1];
}

vector<shared_ptr<Interval>> Interval::split(shared_ptr<DataType> point,
                                             bool point_target) const
{
    int rel[] = {this->value[0]->cmp(point.get()),
                 this->value[1]->cmp(point.get())};
    if (rel[0] > 0 || rel[1] < 0 || (rel[0] == 0 && !point_target) ||
        (rel[1] == 0 && point_target))
        return {};

    return {make_shared<Interval>(this->getMin(), false, point,
                                  !point_target),
            make_shared<Interval>(point, point_target, this->getMax(),
                                  false)};
}

Interval Interval::Union(
    const vector<shared_ptr<const Interval>> &intervals)
{
    const DataType *min_value(nullptr), *max_value(nullptr);
    for (auto i : intervals)
    {
        if (min_value != nullptr)
            min_value = min_value->min(i->value[0]);
        else
            min_value = i->value[0];

        if (max_value != nullptr)
            max_value = max_value->max(i->value[1]);
        else
            max_value = i->value[1];
    }
    if (min_value == nullptr || max_value == nullptr)
        throw Exception(
            "Interval::Union: cannot find min or max values");
    return Interval(
        shared_ptr<const DataType>(min_value->clone()), false,
        shared_ptr<const DataType>(max_value->clone()), false);
}

shared_ptr<FunctionExpression> Interval::makeExpression(
    const string &attribute_name) const
{
    shared_ptr<Literal> left_l = make_shared<Literal>(
        "left_" + attribute_name,
        shared_ptr<DataType>(this->value[0]->clone()));
    shared_ptr<Literal> right_l = make_shared<Literal>(
        "right_" + attribute_name,
        shared_ptr<DataType>(this->value[1]->clone()));
    shared_ptr<Attribute> attr =
        make_shared<Attribute>(attribute_name, getType());
    vector<shared_ptr<const Expression>> exps = {
        make_shared<FunctionExpression>(
            "left_filter_" + attribute_name, "gte",
            vector<shared_ptr<const Expression>>{attr, left_l},
            DATA_TYPE::BOOLEAN, false),
        make_shared<FunctionExpression>(
            "right_filter_" + attribute_name, "lte",
            vector<shared_ptr<const Expression>>{attr, right_l},
            DATA_TYPE::BOOLEAN, false)};
    return make_shared<FunctionExpression>("filter_" + attribute_name,
                                           "and", exps,
                                           DATA_TYPE::BOOLEAN, false);
}
