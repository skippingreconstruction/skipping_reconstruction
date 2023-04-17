#include "metadata/expression.h"
#include "exceptions.h"
#include "metadata/schema.h"
#include "produce_plan/build_substrait.h"

void Attribute::makeSubstraitExpression(
    substrait::Expression *mutable_out,
    shared_ptr<const Schema> schema) const
{
    auto field = mutable_out->mutable_selection();
    int offset = schema->getOffset(this->name);
    if (offset == -1)
        throw Exception("Cannot find the attribute " + this->name +
                        " in schema");
    field->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(offset);
    field->mutable_root_reference();
}

Expression *FunctionExpression::clone() const
{
    vector<shared_ptr<const Expression>> new_children;
    for (int i = 0; i < this->children.size(); i++)
        new_children.push_back(
            shared_ptr<Expression>(this->children[i]->clone()));
    return new FunctionExpression(this->name, this->op, new_children,
                                  this->type, this->nullable);
}

Expression *AggregateExpression::clone() const
{
    vector<shared_ptr<const Expression>> new_children;
    for (int i = 0; i < this->children.size(); i++)
        new_children.push_back(
            shared_ptr<Expression>(this->children[i]->clone()));
    return new AggregateExpression(this->name, this->op, new_children,
                                   this->type, this->nullable);
}

Expression *IfFunctionExpression::clone() const
{
    vector<shared_ptr<const Expression>> new_children;
    for (int i = 0; i < this->children.size(); i++)
        new_children.push_back(
            shared_ptr<Expression>(this->children[i]->clone()));
    return new IfFunctionExpression(this->name, new_children[0],
                                    new_children[1], new_children[2]);
}

Expression *Literal::clone() const
{
    return new Literal(this->name, this->value);
}

void FunctionExpression::makeSubstraitExpression(
    substrait::Expression *mutable_out,
    shared_ptr<const Schema> schema) const
{
    auto function = mutable_out->mutable_scalar_function();
    int function_anchor = getFunctionAnchor(this->op);
    function->set_function_reference(function_anchor);
    makeSubstraitType(function->mutable_output_type(), this->getType(),
                      this->nullable, false);
    string overflow_option = getFunctionOverflowOption(this->op);
    if (overflow_option.length() > 0)
    {
        *function->add_arguments()
             ->mutable_enum_()
             ->mutable_specified() = overflow_option;
    }
    for (auto arg : children)
        arg->makeSubstraitExpression(
            function->add_arguments()->mutable_value(), schema);
}

bool FunctionExpression::isAndOnly(const string &and_op) const
{
    if (this->op != and_op)
    {
        for (auto e : children)
        {
            if (dynamic_cast<const Attribute *>(e.get()) == nullptr &&
                dynamic_cast<const Literal *>(e.get()) == nullptr)
                return false;
        }
        return true;
    }

    for (auto e : children)
    {
        auto f = dynamic_cast<const FunctionExpression *>(e.get());
        if (f == nullptr || !f->isAndOnly(and_op))
            return false;
    }
    return true;
}

vector<shared_ptr<const FunctionExpression>> FunctionExpression::
    getSubExpressions(const string &and_op) const
{
    if (this->isAndOnly(and_op) == false)
        throw Exception(
            "getSubExpression only works on conjunctive expression");
    vector<shared_ptr<const FunctionExpression>> ret;
    if (this->op != and_op)
    { // get a leaf node
        ret.push_back(make_shared<const FunctionExpression>(*this));
    }
    else
    {
        for (auto e : children)
        {
            auto f = dynamic_cast<const FunctionExpression *>(e.get());
            auto ret_f = f->getSubExpressions(and_op);
            ret.insert(ret.end(), ret_f.begin(), ret_f.end());
        }
    }
    return ret;
}

shared_ptr<FunctionExpression> FunctionExpression::connectExpression(
    const string &name,
    const vector<shared_ptr<const Expression>> &subExpressions,
    bool nullable, bool conjunctive)
{
    if (subExpressions.size() == 0)
        throw Exception("subExpression at least have one element");

    string op_name = "and";
    bool default_value = true;
    if (!conjunctive)
    {
        op_name = "or";
        default_value = false;
    }

    vector<shared_ptr<const Expression>> children{subExpressions[0]};

    if (subExpressions.size() == 1)
        children.push_back(make_shared<Literal>(
            "auxil_exp", make_shared<Boolean>(default_value)));
    else
        children.push_back(connectExpression(
            name,
            vector<shared_ptr<const Expression>>(
                subExpressions.begin() + 1, subExpressions.end()),
            nullable, conjunctive));
    return make_shared<FunctionExpression>(
        name, op_name, children, DATA_TYPE::BOOLEAN, nullable);
}

void IfFunctionExpression::makeSubstraitExpression(
    substrait::Expression *mutable_out,
    shared_ptr<const Schema> schema) const
{
    auto function = mutable_out->mutable_if_then();
    auto ifs = function->add_ifs();
    children[0]->makeSubstraitExpression(ifs->mutable_if_(), schema);
    children[1]->makeSubstraitExpression(ifs->mutable_then(), schema);
    children[2]->makeSubstraitExpression(function->mutable_else_(),
                                         schema);
}

Literal::Literal(const string &name, DATA_TYPE type)
{
    this->name = name;
    this->type = type;
    switch (type)
    {
    case DATA_TYPE::INTEGER:
        this->value = make_shared<Integer>();
        break;
    case DATA_TYPE::BOOLEAN:
        this->value = make_shared<Boolean>();
        break;
    case DATA_TYPE::STRING:
        this->value = make_shared<String>();
        break;
    case DATA_TYPE::DOUBLE:
        this->value = make_shared<Double>();
        break;
    default:
        throw Exception("Can not make default value for data type " +
                        type);
        break;
    }
}

void Literal::makeSubstraitExpression(
    substrait::Expression *mutable_out,
    shared_ptr<const Schema> schema) const
{
    this->value->makeSubstraitLiteral(mutable_out->mutable_literal());
}

shared_ptr<Expression> Expression::parseSubstraitExpression(
    const substrait::Expression *serialized,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const unordered_map<int, string>> function_anchor)
{
    using rex_type = substrait::Expression::RexTypeCase;
    switch (serialized->rex_type_case())
    {
    case rex_type::kLiteral:
        return Literal::parseSubstraitExpression(
            serialized, table_schema, function_anchor);
        break;
    case rex_type::kSelection:
        return Attribute::parseSubstraitExpression(
            serialized, table_schema, function_anchor);
        break;
    case rex_type::kScalarFunction:
        return FunctionExpression::parseSubstraitExpression(
            serialized, table_schema, function_anchor);
        break;
    default:
        throw Exception("Expression::parseSubstraitExpression: unknown "
                        "expression type " +
                        serialized->rex_type_case());
        break;
    }
}

shared_ptr<Attribute> Attribute::parseSubstraitExpression(
    const substrait::Expression *serialized,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const unordered_map<int, string>> function_anchor)
{
    if (!serialized->has_selection())
        throw Exception("Attribute::parseSubstraitExpression: "
                        "substrait expression must be FieldReference");
    auto &a = serialized->selection();
    if (!a.has_direct_reference() ||
        !a.direct_reference().has_struct_field() ||
        !a.has_root_reference())
        throw Exception(
            "Attribute::parseSubstraitExpression: "
            "substrait FieldReference must be "
            "direct_reference.struct_field and root_reference");
    int index = a.direct_reference().struct_field().field();
    auto attr = table_schema->get(index);
    return make_shared<Attribute>(attr->getName(), attr->getType());
}

int Literal::substrait_op_id = 0;

shared_ptr<Literal> Literal::parseSubstraitExpression(
    const substrait::Expression *serialized,
    shared_ptr<const Schema> table_schema,
    shared_ptr<const unordered_map<int, string>> function_anchor)
{
    if (!serialized->has_literal())
        throw Exception("Literal::parseSubstraitExpression: substrait "
                        "expression must be Literal");
    auto &l = serialized->literal();
    return make_shared<Literal>("literal_" +
                                    std::to_string(substrait_op_id++),
                                DataType::parseSubstraitLiteral(&l));
}

int FunctionExpression::substrait_op_id = 0;

shared_ptr<FunctionExpression> FunctionExpression::
    parseSubstraitExpression(
        const substrait::Expression *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor)
{
    if (!serialized->has_scalar_function())
        throw Exception("FunctionExpression::parseSubstraitExpression: "
                        "substrait expression must be ScalarFunction");
    auto &f = serialized->scalar_function();
    if (function_anchor->count(f.function_reference()) == 0)
        throw Exception("FunctionExpression::parseSubstraitExpression: "
                        "unkown function");
    string op = function_anchor->at(f.function_reference());

    vector<shared_ptr<const Expression>> args;
    for (int i = 0; i < f.arguments_size(); i++)
    {
        if (!f.arguments(i).has_value())
            throw Exception(
                "FunctionExpression::parseSubstraitExpression: "
                "arguments must be expression");
        auto &a = f.arguments(i).value();
        args.push_back(Expression::parseSubstraitExpression(
            &a, table_schema, function_anchor));
    }

    auto &t = f.output_type();
    bool nullability;
    DATA_TYPE out_type = parseSubstraitType(&t, nullability);

    return make_shared<FunctionExpression>(
        op + "_" + std::to_string(substrait_op_id++), op, args,
        out_type, nullability);
}

int AggregateExpression::substrait_op_id = 0;

shared_ptr<AggregateExpression> AggregateExpression::
    parseSubstraitAggregate(
        const substrait::AggregateFunction *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor)
{
    if (function_anchor->count(serialized->function_reference()) == 0)
        throw Exception("AggregateExpression::parseSubstraitAggregate: "
                        "unknown function");
    string op = function_anchor->at(serialized->function_reference());

    vector<shared_ptr<const Expression>> args;
    for (int i = 0; i < serialized->arguments_size(); i++)
    {
        if (!serialized->arguments(i).has_value())
            throw Exception(
                "AggregateExpression::parseSubstraitAggregate: "
                "function arguments must be Expression");
        auto &e = serialized->arguments(i).value();
        args.push_back(Expression::parseSubstraitExpression(
            &e, table_schema, function_anchor));
    }

    auto &t = serialized->output_type();
    bool nullability;
    DATA_TYPE out_type = parseSubstraitType(&t, nullability);
    return make_shared<AggregateExpression>(
        op + "_" + std::to_string(substrait_op_id++), op, args,
        out_type, nullability);
}