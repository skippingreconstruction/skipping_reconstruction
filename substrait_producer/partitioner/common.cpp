#include "partitioner/common.h"
#include "exceptions.h"
#include "partitioner/model.h"
#include <algorithm>

PartitionParameter PartitionParameter::parse(int argc,
                                             char const *argv[])
{
    PartitionParameter p;
    int idx = 1;
    while (idx < argc)
    {
        string op = argv[idx++];
        if (op == "--schema_path")
            p.schema_path = argv[idx++];
        else if (op == "--table_range")
            p.table_range_path = argv[idx++];
        else if (op == "--query_path")
            p.query_path = argv[idx++];
        else if (op == "--validate_query_path")
            p.validation_path = argv[idx++];
        else if (op == "--test_query_path")
            p.test_query_path = argv[idx++];
        else if (op == "--partition_path")
            p.partition_path = argv[idx++];
        else if (op == "--type")
        {
            string type = argv[idx++];
            std::transform(type.begin(), type.end(), type.begin(),
                           ::tolower);
            if (type == "horizontal")
                p.partition_type = Horizontal;
            else if (type == "hierarchical-late")
                p.partition_type = Hierarchical_Late;
            else if (type == "hierarchical-early")
                p.partition_type = Hierarchical_Early;
            else
                throw Exception("PartitionParameter::parse: "
                                "Invalid partition type " +
                                type);
        }
    }
    return p;
}

bool stopByRowNum(shared_ptr<const BlockMeta> block)
{
    return block->getRowNum() <= BLOCK_MIN_ROW_NUM;
}

double estimateCost(
    const vector<shared_ptr<const ScanParameter>> &recons_params,
    const vector<shared_ptr<const ScanParameter>> &direct_params,
    shared_ptr<const Schema> table_schema,
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long),
    bool print_stats)
{
    uint64_t io_size = 0, io_row_num = 0;
    uint64_t recons_tuples(0), valid_cells(0), total_cells(0);

    double io_cost(0), recons_cost(0);

    unordered_map<shared_ptr<const BlockMeta>, boost::dynamic_bitset<>>
        read_attributes_in_direct;
    // estimate I/O size in direct
    for (auto p : direct_params)
    {
        assert(p->blocks.size() == 1);
        auto b = *p->blocks.begin();
        io_size += b->estimateIOSize(
            table_schema->get(p->read_attributes)->getAttributeNames());

        io_row_num += b->getRowNum();
        read_attributes_in_direct[b] = p->read_attributes;
    }

    // estimate I/O and reconstruct in recons_params
    boost::dynamic_bitset<> recons_attributes(table_schema->size());
    for (auto p : recons_params)
    {
        assert(p->blocks.size() == 1);
        auto b = *p->blocks.begin();
        auto read_attributes = p->read_attributes;
        if (read_attributes_in_direct.count(b))
            read_attributes -= read_attributes_in_direct[b];

        io_size += b->estimateIOSize(
            table_schema->get(read_attributes)->getAttributeNames());
        io_row_num += b->getRowNum();

        recons_attributes |= p->project_attributes;
        int tnum = b->getRowNum();
        if (p->filter_boundary)
            tnum = b->estimateRowNum(*p->filter_boundary);
        recons_tuples += tnum;

        int anum = p->project_attributes.count() - 1;
        assert(anum > 0);
        valid_cells += anum * tnum;
    }
    int recons_anum = recons_attributes.count() - 1;
    assert(recons_anum == -1 || recons_anum > 0);
    total_cells = recons_anum * recons_tuples;

    double io_time = predictIOTime(io_size);
    double recons_time =
        aggModel(recons_tuples, total_cells, valid_cells);
    double total_time = io_time + recons_time;

    if (print_stats)
    {
        printf("Query total time: %.2f seconds\n", total_time);
        printf(
            "Query I/O time: %.2f seconds, size: %.2f GB, %.2fM rows\n",
            io_time, (double)io_size / (1024 * 1024 * 1024),
            (double)io_row_num / (1024 * 1024));
        printf(
            "Query reconstruction time: %.2f seconds, %.2fM inserts, "
            "%.2fB total cells, %.2fB valid cells\n",
            recons_time, (double)recons_tuples / (1024 * 1024),
            (double)total_cells / (1024 * 1024 * 1024),
            (double)valid_cells / (1024 * 1024 * 1024));
    }

    return total_time;
}

double estimateCost(
    const vector<shared_ptr<const BlockMeta>> &blocks,
    const unordered_set<shared_ptr<const Query>> &queries,
    shared_ptr<const Schema> table_schema,
    std::pair<vector<shared_ptr<const ScanParameter>>,
              vector<shared_ptr<const ScanParameter>>> (
        *produceParameter)(
        shared_ptr<const Query> query,
        shared_ptr<const Schema> table_schema,
        const vector<shared_ptr<const PartitionMeta>> &partitions),
    double (*aggModel)(unsigned long long, unsigned long long,
                       unsigned long long))
{
    vector<shared_ptr<const PartitionMeta>> partitions;
    for (int i = 0; i < blocks.size(); i++)
    {
        auto p =
            make_shared<PartitionMeta>(std::to_string(i) + ".parquet");
        p->addBlock(shared_ptr<BlockMeta>(blocks[i]->clone()));
        partitions.push_back(p);
    }

    int qid = 0;
    double total_time = 0;
    for (auto q : queries)
    {
        printf("cost of Q%d\t%s:\n", qid++, q->toString().c_str());
        auto params = produceParameter(q, table_schema, partitions);
        total_time += estimateCost(params.second, params.first,
                                   table_schema, aggModel, true);
    }
    printf("Total time is %.2f seconds\n", total_time);
    return total_time;
}