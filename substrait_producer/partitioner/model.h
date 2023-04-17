#pragma once

/**
 * @brief Predict the I/O time in Velox on balos
 *
 * @param io_size_bytes I/O size in bytes
 * @return double predicted time in seconds
 */
double predictIOTime(unsigned long long io_size_bytes);

/**
 * @brief Predict the aggregation time of early reconstruction in Velox
 * (6 threads) on balos
 *
 * @param insert_num number of insert/update operations
 * @param total_cells number of cells in the hash table
 * @param valid_cells number of valid cells in the hash table
 * @return double predicted time in seconds
 */
double predictAggTimeEarly(unsigned long long insert_num,
                           unsigned long long total_cells,
                           unsigned long long valid_cells);

/**
 * @brief Predict the aggregation time of late reconstruction in Velox
 * (6 threads) on balos
 *
 * @param insert_num number of insert/update operations
 * @param total_cells number of cells in the hash table
 * @param valid_cells number of valid cells in the hash table
 * @return double predicted time in seconds
 */
double predictAggTimeLate(unsigned long long insert_num,
                          unsigned long long total_cells,
                          unsigned long long valid_cells);