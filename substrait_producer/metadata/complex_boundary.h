#pragma once
#include "metadata/boundary.h"

using namespace std;

class ComplexBoundary
{
  public:
    static shared_ptr<ComplexBoundary> makeComplexBoundary(
        const vector<shared_ptr<const Boundary>> &boundaries,
        int max_intervals_per_attribute);

    ComplexBoundary(const Boundary &b);
    ComplexBoundary(const ComplexBoundary &b) : intervals(b.intervals)
    {
    }

    void keepAttributes(const unordered_set<string> &attributes);

    /**
     * @brief Compute the set relationship of complex and plain
     * boundaries. The complex boundary is the superset of the plain
     * boundary if each attribute has an interval in the complex
     * boundary that is the superset of or equal to the corresponding
     * interval in the plain boundary. The complex is the subset of the
     * plain if each interval in complex is the subset of or equal to
     * the corresponding interval in the plain one. A missing interval
     * is the superset of any other interval.
     *
     * @param boundary
     * @return SET_RELATION
     */
    SET_RELATION relationship(const Boundary &boundary) const;

    shared_ptr<FunctionExpression> makeExpression() const;

    ComplexBoundary intersect(const Boundary &other) const;

    unordered_map<string, vector<shared_ptr<const Interval>>>
    getIntervals() const
    {
        return intervals;
    }

  private:
    ComplexBoundary()
    {
    }
    ComplexBoundary(
        const unordered_map<string, vector<shared_ptr<const Interval>>>
            &intervals)
        : intervals(intervals)
    {
    }

    // each attribute has a set of disjoint intervals
    unordered_map<string, vector<shared_ptr<const Interval>>> intervals;

    /**
     * @brief compute the relationship between an interval and a vector
     * of interval
     *
     * @param complex
     * @param plain
     * @return SET_RELATION DISJOINT: all intervals in the vector are
     * disjoint with the plain; EQUAL: the vector has a single interval
     * and the interval is equal to the plain; SUBSET: all intervals in
     * the vector are the subset of the plain; SUPERSET: an interval is
     * the superset of the plain; or the vector has at least two
     * intervals and one interval is equal to the plain; INTERSECT:
     * other cases
     */
    SET_RELATION relationship(
        const vector<shared_ptr<const Interval>> &complex,
        shared_ptr<const Interval> plain) const;
};