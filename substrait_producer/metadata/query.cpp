#include "metadata/query.h"
#include "configuration.h"

Query::Query(
    shared_ptr<const Schema> table_schema,
    shared_ptr<const FunctionExpression> filter,
    const vector<shared_ptr<const AggregateExpression>> &measures,
    const string &path)
    : table_schema(table_schema), filter(filter), measures(measures),
      path(path)
{
    for (auto m : measures)
        attributes_in_measures.push_back(
            make_shared<unordered_set<string>>(m->getAttributes()));
    produceFilterBoundary();
}

unordered_set<string> Query::getAllReferredAttributes() const
{
    unordered_set<string> attributes =
        this->filter_boundary->getAttributes();
    for (auto m : attributes_in_measures)
        attributes.insert(m->begin(), m->end());
    return attributes;
}

void Query::produceFilterBoundary()
{
    static const unordered_map<string, string> operators = {
        {"gt", "lt"},
        {"lt", "gt"},
        {"gte", "lte"},
        {"lte", "gte"},
        {"equal", "equal"}};
    auto sub_exps = this->filter->getSubExpressions("and");
    unordered_map<string, shared_ptr<Interval>> intervals;

    for (auto e : sub_exps)
    {
        auto children = e->getChildren();
        if (children.size() != 2)
            throw Exception("Query::produceFilterBoundary: expect "
                            "binary expression but got " +
                            e->toString());
        shared_ptr<const Attribute> attribute;
        shared_ptr<const Literal> literal;
        int attribute_off = -1;
        for (int i = 0; i < children.size(); i++)
        {
            auto c = children[i];
            auto a = dynamic_pointer_cast<const Attribute>(c);
            if (a)
                if (attribute)
                    throw Exception("Query::produceFilterBoundary: the "
                                    "expression must "
                                    "have a single attribute but got " +
                                    e->toString());
                else
                {
                    attribute = std::move(a);
                    attribute_off = i;
                }
            auto l = dynamic_pointer_cast<const Literal>(c);
            if (l)
                if (literal)
                    throw Exception("Query::produceFilterBoundary: the "
                                    "expression must "
                                    "have a single literval but got " +
                                    e->toString());
                else
                    literal = std::move(l);
        }
        string name = attribute->getName();
        if (intervals.find(name) == intervals.end())
            intervals.emplace(
                name, make_shared<Interval>(getMinValue(name), false,
                                            getMaxValue(name), false));
        auto it = intervals.find(name);
        string op = e->getFunction();
        if (attribute_off != 0) // value op attribute
            if (operators.find(op) == operators.end())
                throw Exception(
                    "Query::produceFilterBoundary: unknow operator " +
                    op);
            else
                op = operators.find(op)->second;
        auto value = literal->getValue();
        if (op == "equal")
        {
            it->second->setMin(value, false);
            it->second->setMax(value, false);
        }
        else if (op == "gt")
            it->second->setMin(value, true);
        else if (op == "lt")
            it->second->setMax(value, true);
        else if (op == "gte")
            it->second->setMin(value, false);
        else if (op == "lte")
            it->second->setMax(value, false);
        else
            throw Exception(
                "Query::getFilterBoundary: unknow operator " + op);
    }

    unordered_map<string, shared_ptr<const Interval>> b;
    for (auto it = intervals.begin(); it != intervals.end(); it++)
        b.emplace(it->first, it->second);
    filter_boundary = make_shared<Boundary>(b);
}

string Query::toString() const
{
    string result = "Query: { \n";
    for (auto m : measures)
        result += "\t measure: " + m->toString() + "\n";
    result += "\t filter: " + this->filter->toString() + "\n";
    result +=
        "\t boundary: " + this->filter_boundary->toString() + "\n";
    result += "}\\End Query";
    return result;
}

vector<shared_ptr<Query>> Query::parseSubstraitQuery(
    const substrait::Plan *serialized,
    shared_ptr<const Schema> table_schema, string table_path)
{
    shared_ptr<unordered_map<int, string>> function_anchor =
        make_shared<unordered_map<int, string>>();
    for (int i = 0; i < serialized->extensions_size(); i++)
    {
        auto &decl = serialized->extensions(i);
        if (!decl.has_extension_function())
            continue;
        function_anchor->emplace(
            decl.extension_function().function_anchor(),
            decl.extension_function().name());
    }
    vector<shared_ptr<Query>> queries;

    for (int i = 0; i < serialized->relations_size(); i++)
    {
        if (!serialized->relations(i).has_rel())
            throw Exception(
                "Query::parseSubstraitQuery: substrait must be Rel");
        auto &rel = serialized->relations(i).rel();
        if (!rel.has_aggregate())
            throw Exception(
                "Query::parseSubstraitQuery: expect AggregateRel");
        auto &agg_rel = rel.aggregate();

        vector<shared_ptr<const AggregateExpression>> measures;
        for (int j = 0; j < agg_rel.measures_size(); j++)
        {
            auto &m = agg_rel.measures(j).measure();
            measures.push_back(
                AggregateExpression::parseSubstraitAggregate(
                    &m, table_schema, function_anchor));
        }

        if (!agg_rel.input().has_filter())
            throw Exception(
                "Query::parseSubstraitQuery: expect FilterRel");
        auto &condition = agg_rel.input().filter().condition();
        auto filter = FunctionExpression::parseSubstraitExpression(
            &condition, table_schema, function_anchor);
        queries.push_back(make_shared<Query>(table_schema, filter,
                                             measures, table_path));
    }
    return queries;
}