#pragma once
#include "data_type/data_type_api.h"
#include "substrait/plan.pb.h"
#include <optional>
#include <string.h>
#include <unordered_set>
#include <vector>

using namespace std;

class Schema;

class Expression
{
  public:
    virtual string toString() const = 0;
    DATA_TYPE getType() const
    {
        return type;
    }

    vector<shared_ptr<const Expression>> getChildren() const
    {
        return children;
    }

    string getName() const
    {
        return name;
    }

    void setName(string name)
    {
        this->name = name;
    }

    virtual Expression *clone() const = 0;
    virtual unordered_set<string> getAttributes() const = 0;
    virtual bool equal(shared_ptr<const Expression> other) const = 0;

    virtual void makeSubstraitExpression(
        substrait::Expression *mutable_out,
        shared_ptr<const Schema> schema) const = 0;

    static shared_ptr<Expression> parseSubstraitExpression(
        const substrait::Expression *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor);

  protected:
    vector<shared_ptr<const Expression>> children;
    DATA_TYPE type;
    string name;
};

class Attribute : public Expression
{
  public:
    Attribute(string name, DATA_TYPE type,
              optional<size_t> size = std::nullopt)
    {
        this->name = name;
        this->type = type;
        if (size.has_value())
            this->size = size;
    }

    string toString() const
    {
        return name;
    }

    string getName() const
    {
        return name;
    }

    unordered_set<string> getAttributes() const
    {
        return {name};
    }

    size_t getSize() const
    {
        if (!size.has_value())
            throw Exception("Attribute does not initialize size");
        return size.value();
    }

    Expression *clone() const
    {
        return new Attribute(this->name, this->type);
    }

    bool equal(shared_ptr<const Expression> other) const
    {
        auto o = dynamic_cast<const Attribute *>(other.get());
        if (o == nullptr)
            return false;
        return this->name == o->name && this->type == o->type;
    }

    void makeSubstraitExpression(substrait::Expression *mutable_out,
                                 shared_ptr<const Schema> schema) const;

    static shared_ptr<Attribute> parseSubstraitExpression(
        const substrait::Expression *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor);

  private:
    optional<size_t> size;
};

class FunctionExpression : public Expression
{
  public:
    FunctionExpression(
        const string &name, const string &op,
        const vector<shared_ptr<const Expression>> &children,
        DATA_TYPE type, bool nullable = true)
    {
        this->name = name;
        this->op = op;
        this->children = children;
        this->type = type;
        this->nullable = nullable;
    }

    string toString() const
    {
        string ret = op + "(";
        for (int i = 0; i < children.size(); i++)
        {
            ret += children[i]->toString();
            if (i < children.size() - 1)
                ret += ", ";
        }
        ret += ")";
        return ret;
    }

    string getFunction() const
    {
        return op;
    }

    bool getNullable() const
    {
        return nullable;
    }

    Expression *clone() const;

    bool isAndOnly(const string &and_op) const;
    vector<shared_ptr<const FunctionExpression>> getSubExpressions(
        const string &and_op) const;

    unordered_set<string> getAttributes() const
    {
        unordered_set<string> ret;
        for (auto c : children)
        {
            auto ret_c = c->getAttributes();
            ret.insert(ret_c.begin(), ret_c.end());
        }
        return ret;
    }

    bool equal(shared_ptr<const Expression> other) const
    {
        auto o = dynamic_cast<const FunctionExpression *>(other.get());
        if (o == nullptr)
            return false;
        if (this->op != o->op || this->nullable != o->nullable ||
            this->type != o->type ||
            this->children.size() != o->children.size())
            return false;
        for (int i = 0; i < children.size(); i++)
            if (children[i]->equal(o->children[i]) == false)
                return false;
        return true;
    }

    virtual void makeSubstraitExpression(
        substrait::Expression *mutable_out,
        shared_ptr<const Schema> schema) const;

    static shared_ptr<FunctionExpression> connectExpression(
        const string &name,
        const vector<shared_ptr<const Expression>> &subExpressions,
        bool nullable, bool conjunctive);

    static shared_ptr<FunctionExpression> parseSubstraitExpression(
        const substrait::Expression *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor);

  protected:
    string op;
    bool nullable;
    static int substrait_op_id;
};

class AggregateExpression : public FunctionExpression
{
  public:
    AggregateExpression(
        const string &name, const string &op,
        const vector<shared_ptr<const Expression>> &children,
        DATA_TYPE type, bool nullable = true)
        : FunctionExpression(name, op, children, type, nullable)
    {
    }

    Expression *clone() const;

    void makeSubstraitExpression(substrait::Expression *mutable_out,
                                 shared_ptr<const Schema> schema) const
    {
        throw UnimplementedFunctionException();
    }

    static shared_ptr<AggregateExpression> parseSubstraitAggregate(
        const substrait::AggregateFunction *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor);

  private:
    static int substrait_op_id;
};

class IfFunctionExpression : public FunctionExpression
{
  public:
    IfFunctionExpression(const string &name,
                         shared_ptr<const Expression> if_exp,
                         shared_ptr<const Expression> then_exp,
                         shared_ptr<const Expression> else_exp)
        : FunctionExpression(name, "if_then_else",
                             vector<shared_ptr<const Expression>>{
                                 if_exp, then_exp, else_exp},
                             then_exp->getType(), true)
    {
        if (then_exp->getType() != else_exp->getType())
            throw Exception("Clauses in the if-then-else expression "
                            "must have the same type");
    }

    Expression *clone() const;

    void makeSubstraitExpression(substrait::Expression *mutable_out,
                                 shared_ptr<const Schema> schema) const;
};

class Literal : public Expression
{
  public:
    Literal(const string &name, shared_ptr<DataType> value)
    {
        this->name = name;
        this->value = value;
        this->type = value->getType();
    }

    Literal(const string &name, DATA_TYPE type);

    Expression *clone() const;

    string toString() const
    {
        return value->toString();
    }

    unordered_set<string> getAttributes() const
    {
        return {};
    }

    shared_ptr<const DataType> getValue() const
    {
        return this->value;
    }

    bool equal(shared_ptr<const Expression> other) const
    {
        auto o = dynamic_cast<const Literal *>(other.get());
        if (o == nullptr)
            return false;
        return o->value->cmp(this->value.get()) == 0;
    }

    void makeSubstraitExpression(substrait::Expression *mutable_out,
                                 shared_ptr<const Schema> schema) const;

    static shared_ptr<Literal> parseSubstraitExpression(
        const substrait::Expression *serialized,
        shared_ptr<const Schema> table_schema,
        shared_ptr<const unordered_map<int, string>> function_anchor);

  private:
    shared_ptr<DataType> value;

    static int substrait_op_id;
};