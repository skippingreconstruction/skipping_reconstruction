#include "produce_plan/helper.h"
#include "produce_plan/build_substrait.h"

shared_ptr<FunctionExpression> makeBitmapGet(
    const string &bitmap_attribute_name,
    shared_ptr<const Schema> schema, int offset)
{
    string name =
        "get_" + bitmap_attribute_name + "_" + std::to_string(offset);
    auto attr = schema->get(bitmap_attribute_name);
    shared_ptr<Literal> lo = make_shared<Literal>(
        "offset", make_shared<Integer>(offset, 32));
    return make_shared<FunctionExpression>(
        name, "bitmap_get",
        vector<shared_ptr<const Expression>>{attr, lo},
        DATA_TYPE::BOOLEAN, false);
}

vector<shared_ptr<const ScanParameter>> mergeBeforeRead(
    const vector<shared_ptr<const ScanParameter>> &parameters)
{
    if (InputParameter::get()->engine == InputParameter::Engine::Arrow)
    {
        vector<shared_ptr<ScanParameter>> out;
        for (auto p : parameters)
        {
            bool put = false;
            for (auto o : out)
            {
                if (p->equal(o))
                {
                    put = true;
                    o->block_id.insert(p->block_id.begin(),
                                       p->block_id.end());
                    o->blocks.insert(p->blocks.begin(),
                                     p->blocks.end());
                    break;
                }
            }
            if (!put)
                out.push_back(p->clone());
        }

        vector<shared_ptr<const ScanParameter>> const_out;
        for (auto s : out)
            const_out.push_back(s);
        return const_out;
    }
    else
    {
        return parameters;
    }
}

shared_ptr<Schema> readBlocks(substrait::Rel *read_rel,
                              const boost::dynamic_bitset<> &attributes,
                              const unordered_set<int> &block_id,
                              shared_ptr<const Schema> table_schema,
                              const string &path)
{
    shared_ptr<Schema> read_schema = make_shared<Schema>();
    shared_ptr<Attribute> block_id_attr =
        make_shared<Attribute>(block_id_name, DATA_TYPE::INTEGER);
    read_schema->add(block_id_attr);
    for (auto i = attributes.find_first(); i != attributes.npos;
         i = attributes.find_next(i))
        read_schema->add(table_schema->get(i));
    auto schema_after_read = read(
        read_rel, path, vector<int>(block_id.begin(), block_id.end()),
        read_schema);
    return schema_after_read;
}

shared_ptr<Schema> readForReconstruction(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &reconstruct_attributes)
{
    substrait::Rel *project_rel, *filter_rel, *read_rel;
    project_rel = rel;

    if (parameter->filter)
    {
        filter_rel = project_rel->mutable_project()->mutable_input();
        read_rel = filter_rel->mutable_filter()->mutable_input();
    }
    else
    {
        read_rel = project_rel->mutable_project()->mutable_input();
    }

    // read data blocks
    auto schema_after_read = readBlocks(
        read_rel, parameter->read_attributes, parameter->block_id,
        table_schema, parameter->file_path);

    // filter tuples
    auto schema_after_filter = schema_after_read;
    if (parameter->filter)
        schema_after_filter =
            filter(filter_rel, parameter->filter, schema_after_read);

    // project attributes
    // create bitmap: passed_predicates, evaluated_measures,
    //                  possible_measures
    vector<shared_ptr<const Expression>> project_expression;
    shared_ptr<FixedBinary> passed_preds =
        make_shared<FixedBinary>(parameter->passed_preds);
    shared_ptr<FixedBinary> direct_measures =
        make_shared<FixedBinary>(parameter->direct_meassures);
    // shared_ptr<FixedBinary> possible_measures =
    //     make_shared<FixedBinary>(parameter->possible_measures);
    shared_ptr<FixedBinary> valid_attributes =
        make_shared<FixedBinary>(parameter->project_attributes);
    project_expression.push_back(
        make_shared<Literal>(passed_pred_name, passed_preds));
    project_expression.push_back(
        make_shared<Literal>(direct_measures_name, direct_measures));
    // project_expression.push_back(make_shared<Literal>(
    //     possible_measures_name, possible_measures));
    project_expression.push_back(
        make_shared<Literal>(valid_attribute_name, valid_attributes));
    for (auto i = reconstruct_attributes.find_first();
         i != reconstruct_attributes.npos;
         i = reconstruct_attributes.find_next(i))
    {
        if (parameter->project_attributes.test(i))
            // the attribute has been read
            project_expression.push_back(table_schema->get(i));
        else
            // fill nulls or zeros
            project_expression.push_back(
                make_shared<Literal>(table_schema->get(i)->getName(),
                                     table_schema->get(i)->getType()));
    }
    auto schema_after_project =
        project(project_rel, project_expression, schema_after_filter);
    return schema_after_project;
}

shared_ptr<Schema> readForDirectEval(
    substrait::Rel *rel, shared_ptr<const ScanParameter> parameter,
    shared_ptr<const Schema> table_schema,
    const boost::dynamic_bitset<> &measure_attribtues)
{
    substrait::Rel *project_rel, *filter_rel, *read_rel;
    project_rel = rel;
    if (parameter->filter)
    {
        filter_rel = project_rel->mutable_project()->mutable_input();
        read_rel = filter_rel->mutable_filter()->mutable_input();
    }
    else
    {
        read_rel = project_rel->mutable_project()->mutable_input();
    }
    // read blocks
    auto schema_after_read = readBlocks(
        read_rel, parameter->read_attributes, parameter->block_id,
        table_schema, parameter->file_path);

    // filter blocks
    auto schema_after_filter = schema_after_read;
    if (parameter->filter)
        schema_after_filter =
            filter(filter_rel, parameter->filter, schema_after_read);

    // project attributes
    vector<shared_ptr<const Expression>> project_expression;
    shared_ptr<FixedBinary> direct_measures =
        make_shared<FixedBinary>(parameter->direct_meassures);
    project_expression.push_back(
        make_shared<Literal>(direct_measures_name, direct_measures));
    for (auto i = measure_attribtues.find_first();
         i != measure_attribtues.npos;
         i = measure_attribtues.find_next(i))
    {
        auto attr = table_schema->get(i);
        if (parameter->project_attributes.test(i))
            project_expression.push_back(attr);
        else
            project_expression.push_back(
                make_shared<Literal>(attr->getName(), attr->getType()));
    }
    auto schema_after_project =
        project(project_rel, project_expression, schema_after_read);
    return schema_after_project;
}