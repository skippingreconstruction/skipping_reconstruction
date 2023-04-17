#include "configuration.h"
#include "data_type/data_type_api.h"

void makeSubstraitType(substrait::Type *mutable_out, DATA_TYPE type,
                       bool nullable, bool upper)
{
    substrait::Type_Nullability n = substrait::Type_Nullability::
        Type_Nullability_NULLABILITY_NULLABLE;
    if (!nullable)
        n = substrait::Type_Nullability::
            Type_Nullability_NULLABILITY_REQUIRED;
    switch (type)
    {
    case DATA_TYPE::INTEGER:
        if (upper)
            mutable_out->mutable_i64()->set_nullability(n);
        else
            mutable_out->mutable_i32()->set_nullability(n);
        break;
    case DATA_TYPE::DOUBLE:
        mutable_out->mutable_fp64()->set_nullability(n);
        break;
    case DATA_TYPE::STRING:
    case DATA_TYPE::STRINGENUM:
        mutable_out->mutable_string()->set_nullability(n);
        break;
    case DATA_TYPE::BOOLEAN:
        mutable_out->mutable_bool_()->set_nullability(n);
        break;
    case DATA_TYPE::FIXEDBINARY: {
        // auto engine = InputParameter::get()->engine;
        // if (engine == InputParameter::Engine::Arrow)
        //     mutable_out->mutable_binary()->set_nullability(n);
        // else
        //     mutable_out->mutable_string()->set_nullability(n);
        mutable_out->mutable_binary()->set_nullability(n);
        break;
    }
    default:
        throw Exception("Cannot make type for " + std::to_string(type));
        break;
    }
}

DATA_TYPE parseSubstraitType(const substrait::Type *type,
                             bool &nullability)
{
    using kind_case = substrait::Type::KindCase;
    auto nullable = substrait::Type_Nullability::
        Type_Nullability_NULLABILITY_NULLABLE;
    switch (type->kind_case())
    {
    case kind_case::kI8:
        nullability = type->i8().nullability() == nullable;
        return DATA_TYPE::INTEGER;
        break;
    case kind_case::kI16:
        nullability = type->i16().nullability() == nullable;
        return DATA_TYPE::INTEGER;
        break;
    case kind_case::kI32:
        nullability = type->i32().nullability() == nullable;
        return DATA_TYPE::INTEGER;
        break;
    case kind_case::kI64:
        nullability = type->i64().nullability() == nullable;
        return DATA_TYPE::INTEGER;
    case kind_case::kBool:
        nullability = type->bool_().nullability() == nullable;
        return DATA_TYPE::BOOLEAN;
    default:
        throw Exception("parseSubstraitType: Unimplemented data type");
    }
}

shared_ptr<DataType> DataType::parseSubstraitLiteral(
    const substrait::Expression_Literal *serialized)
{
    using Literal = substrait::Expression::Literal;
    switch (serialized->literal_type_case())
    {
    case Literal::kI8:
        return make_shared<Integer>(serialized->i8(), 8);
        break;
    case Literal::kI16:
        return make_shared<Integer>(serialized->i16(), 16);
        break;
    case Literal::kI32:
        return make_shared<Integer>(serialized->i32(), 32);
        break;
    case Literal::kI64:
        return make_shared<Integer>(serialized->i64(), 64);
        break;
    case Literal::kDecimal: {
        auto &d = serialized->decimal();
        assert(d.value().size() == sizeof(double));
        double value = 0;
        memcpy(&value, d.value().data(), sizeof(double));
        return make_shared<Double>(value, d.precision());
        break;
    }
    case Literal::kBoolean:
        return make_shared<Boolean>(serialized->boolean());
        break;
    default:
        throw Exception(
            "DataType::parseSubstraitLiteral: Unimplemented data type");
        break;
    }
}