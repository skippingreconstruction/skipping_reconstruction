#include "produce_plan/build_substrait.h"
#include "configuration.h"
#include "produce_plan/impl/build_substrait_impl_arrow.h"
#include "produce_plan/impl/build_substrait_impl_velox.h"
#include <memory.h>
#include <unordered_map>

shared_ptr<unordered_map<string, int>> functions;

const unordered_map<string, string> function_overflow_option = {
    {"add", "SILENT"},
    {"subtract", "SILENT"},
    {"multiply", "SILENT"},
    {"divide", "SILENT"}};

void registFunctions(st::Plan *plan)
{
    functions = make_shared<unordered_map<string, int>>();

    unordered_map<string, vector<string>> uri_funcs = {
        {kUDFURI,
         {"reconstruct", "bitmap_or", "bitmap_get", "bitmap_or_scalar",
          "bitmap_and_scalar",
          "bitmap_count"}}, // TODO: bitmap_or_scalar,
                            // bitmap_and_scalar, bitmap_count (count
                            // the number of set bits)
        {kComparisonURI,
         {"equal", "gte", "lte", "lt", "is_not_null",
          "is_null"}}, // is_not_null is not valid in Velox
        {kArithmeticURI, {"sum", "add"}},
        {kBooleanURI, {"or", "and", "not"}}};

    int uri_anchor = 10;
    int func_anchor = 100;
    for (auto it = uri_funcs.begin(); it != uri_funcs.end(); it++)
    {
        auto uri = plan->add_extension_uris();
        uri->set_extension_uri_anchor(uri_anchor);
        uri->set_uri(it->first);

        for (auto &n : it->second)
        {
            auto func =
                plan->add_extensions()->mutable_extension_function();
            func->set_extension_uri_reference(uri_anchor);
            func->set_function_anchor(func_anchor);
            func->set_name(n);
            functions->emplace(n, func_anchor);
            func_anchor++;
        }
        uri_anchor++;
    }
}

int getFunctionAnchor(const string &name)
{
    if (!functions)
        throw Exception("Must regist functions");
    auto it = functions->find(name);
    if (it == functions->end())
        throw Exception("Cannot find function " + name);
    return it->second;
}

string getFunctionOverflowOption(const string &name)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
    {
        auto it = function_overflow_option.find(name);
        if (it == function_overflow_option.end())
            return "";
        else
            return it->second;
    }
    else
        // velox
        return "";
}

void makeBinaryScalarFunction(
    substrait::Expression_ScalarFunction *function,
    const string &function_name, int offset, const DataType *value,
    DATA_TYPE out_type, bool nullable)
{
    int function_anchor = getFunctionAnchor(function_name);
    function->set_function_reference(function_anchor);
    makeSubstraitType(function->mutable_output_type(), out_type,
                      nullable, false);
    auto field =
        function->add_arguments()->mutable_value()->mutable_selection();
    field->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(offset);
    field->mutable_root_reference();
    value->makeSubstraitLiteral(
        function->add_arguments()->mutable_value()->mutable_literal());
}

shared_ptr<FunctionExpression> checkValid(
    const string &name, shared_ptr<const Attribute> attribute)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return make_shared<FunctionExpression>(
            name, "is_not_null",
            vector<shared_ptr<const Expression>>{attribute},
            DATA_TYPE::BOOLEAN, false);
    else
    {
        auto is_valid_exp = make_shared<FunctionExpression>(
            name, "is_null",
            vector<shared_ptr<const Expression>>{attribute},
            DATA_TYPE::BOOLEAN, false);
        return make_shared<FunctionExpression>(
            name, "not",
            vector<shared_ptr<const Expression>>{is_valid_exp},
            DATA_TYPE::BOOLEAN, false);
    }
}

/**
 * @brief Read a parquet file
 *
 * @param rel
 * @param path
 * @param block_id
 * @param base_schema
 */
shared_ptr<Schema> read(st::Rel *rel, const std::string &path,
                        const std::vector<int> &block_id,
                        shared_ptr<const Schema> base_schema)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return readArrow(rel, path, block_id, base_schema);
    else
        return readVelox(rel, path, block_id, base_schema);
}

shared_ptr<Schema> filter(st::Rel *rel,
                          shared_ptr<const Expression> expression,
                          shared_ptr<const Schema> inSchema)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return filterArrow(rel, expression, inSchema);
    else
        return filterVelox(rel, expression, inSchema);
}

shared_ptr<Schema> project(
    st::Rel *rel,
    const vector<shared_ptr<const Expression>> &expression,
    shared_ptr<const Schema> in_schema)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return projectArrow(rel, expression, in_schema);
    else
        return projectVelox(rel, expression, in_schema);
}

shared_ptr<Schema> unionAll(st::Rel *rel,
                            shared_ptr<const Schema> in_schema)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return unionAllArrow(rel, in_schema);
    else
        return unionAllVelox(rel, in_schema);
}

shared_ptr<Schema> aggregate(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    vector<shared_ptr<const AggregateExpression>> measures,
    shared_ptr<const Expression> group)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        return aggregateArrow(rel, in_schema, measures, group);
    else
        return aggregateVelox(rel, in_schema, measures, group);
}

shared_ptr<Schema> exchange(
    st::Rel *rel, shared_ptr<const Schema> in_schema,
    const vector<shared_ptr<const Attribute>> &scatter_attributes)
{
    auto engine = InputParameter::get()->engine;
    if (engine == InputParameter::Engine::Arrow)
        throw Exception(
            "ExchangeRel has not been implemented in Arrow");
    else
        return exchangeVelox(rel, in_schema, scatter_attributes);
}

shared_ptr<Schema> equalJoin(
    st::Rel *rel, shared_ptr<const Schema> left_schema,
    shared_ptr<const Schema> right_schema, const string &left_key,
    const string &right_key, ::substrait::JoinRel_JoinType type,
    vector<int> &left_map, vector<int> &right_map)
{
    auto join_rel = rel->mutable_join();
    join_rel->set_type(type);

    int left_idx(-1), right_idx(-1);
    left_map.clear();
    right_map.clear();
    shared_ptr<Schema> out_schema = make_shared<Schema>();
    for (int i = 0; i < left_schema->size(); i++)
    {
        auto a = (Attribute *)left_schema->get(i)->clone();
        if (a->getName() == left_key)
            left_idx = i;
        a->setName("left_" + a->getName());
        out_schema->add(shared_ptr<Attribute>(a));
        left_map.push_back(i);
    }
    for (int i = 0; i < right_schema->size(); i++)
    {
        auto a = (Attribute *)right_schema->get(i)->clone();
        if (a->getName() == right_key)
            right_idx = i + left_schema->size();
        a->setName("right_" + a->getName());
        out_schema->add(shared_ptr<Attribute>(a));
        right_map.push_back(i + left_schema->size());
    }

    FunctionExpression exp(
        "join_exp", "equal",
        vector<shared_ptr<const Expression>>{
            out_schema->get(left_idx), out_schema->get(right_idx)},
        DATA_TYPE::BOOLEAN, false);
    exp.makeSubstraitExpression(join_rel->mutable_expression(),
                                out_schema);
    return out_schema;
}