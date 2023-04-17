#include "partitioner/model.h"

double predictIOTime(unsigned long long io_size_bytes)
{
    double size_mb = (double)io_size_bytes / (1024 * 1024);

    // the model for balos
    double coefficient = 0.0111;
    return size_mb * coefficient;
}

double predictAggTimeEarly(unsigned long long insert_num,
                           unsigned long long total_cells,
                           unsigned long long valid_cells)
{
    double insert_m = (double)insert_num / (1024 * 1024);
    double total_m = (double)total_cells / (1024 * 1024);
    double valid_m = (double)valid_cells / (1024 * 1024);
    double coefficients[3] = {0.3172, 0.00419, 0.0263};
    return insert_m * coefficients[0] + total_m * coefficients[1] +
           valid_m * coefficients[2];
}

double predictAggTimeLate(unsigned long long insert_num,
                          unsigned long long total_cells,
                          unsigned long long valid_cells)
{
    double insert_m = (double)insert_num / (1024 * 1024);
    double total_m = (double)total_cells / (1024 * 1024);
    double valid_m = (double)valid_cells / (1024 * 1024);
    double coefficients[3] = {0.7224, 0.01, 0.011};
    return insert_m * coefficients[0] + total_m * coefficients[1] +
           valid_m * coefficients[2];
}