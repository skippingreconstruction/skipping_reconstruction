#pragma once

#include "metadata/boundary.h"
#include "metadata/expression.h"
#include "metadata/schema.h"
#include <string>
#include <vector>

using namespace std;

class Query
{
  public:
    Query(shared_ptr<const Schema> table_schema,
          shared_ptr<const FunctionExpression> filter,
          const vector<shared_ptr<const AggregateExpression>> &measures,
          const string &path);

    shared_ptr<const FunctionExpression> getFilter() const
    {
        return filter;
    }

    vector<shared_ptr<const AggregateExpression>> getMeasures() const
    {
        return measures;
    }

    shared_ptr<const unordered_set<string>> attributesInMeasure(
        int measure_index) const
    {
        return attributes_in_measures[measure_index];
    }

    int numOfMeasures() const
    {
        return measures.size();
    }

    string getPath() const
    {
        return path;
    }

    shared_ptr<const Boundary> getFilterBoundary() const
    {
        return filter_boundary;
    }

    unordered_set<string> getAllReferredAttributes() const;

    string toString() const;

    static vector<shared_ptr<Query>> parseSubstraitQuery(
        const substrait::Plan *serialized,
        shared_ptr<const Schema> table_schema, string table_path);

  private:
    shared_ptr<const Schema> table_schema;
    shared_ptr<const FunctionExpression> filter;
    vector<shared_ptr<const AggregateExpression>> measures;
    const string path;

    shared_ptr<Boundary> filter_boundary;
    vector<shared_ptr<unordered_set<string>>> attributes_in_measures;

    void produceFilterBoundary();
};