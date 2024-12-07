import json
import time
import pandas as pd
import logging
from pathlib import Path
import re
from executor import BasicExecutor, QueryLeastExecutor
from sql_service_advance_old import compose_result


class SQLPerformanceTester:
    def __init__(self, base_table_name="meeting"):
        logging.basicConfig(
            filename='sql_test.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        self.data_sizes = ['100w', '200w', '300w', '400w', '500w']
        self.split_strategies = ['n2', 'n3', 'n4']
        self.base_table_name = base_table_name

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def clean_sql(self, sql):
        """Clean SQL statement by removing semicolons and extra whitespace"""
        sql = sql.strip().rstrip(';')
        sql = ' '.join(sql.split())
        return sql

    def get_table_name(self, base_name, table_type, server_num, strategy):
        """Generate correct table name based on components"""
        if table_type == 'relation':
            return f"{base_name}_relation_{strategy}"
        else:
            return f"{base_name}_server{server_num}_{strategy}"

    def prepare_test_config(self, data_size, split_strategy):
        """Prepare test configuration file with error handling"""
        self.logger.info(f"Preparing config for size {data_size} with strategy {split_strategy}")

        template_file = f'table_config_{split_strategy}.json'
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            base_name = f"{self.base_table_name}{data_size}"
            config['real_table_name'] = base_name

            # Store original column configurations for relation table
            relation_columns = None
            for table in config['split_tables']:
                if 'relation' in table['split_table_name']:
                    relation_columns = table['columns']
                    break

            if not relation_columns:
                raise ValueError("No relation table found in configuration")

            # Update table names and verify structure
            for table in config['split_tables']:
                original_name = table['split_table_name']
                if 'relation' in original_name:
                    new_name = self.get_table_name(base_name, 'relation', None, split_strategy)
                    table['split_table_name'] = new_name
                    table['columns'] = relation_columns
                else:
                    server_num = original_name.split('server')[1].split('_')[0]
                    new_name = self.get_table_name(base_name, 'server', server_num, split_strategy)
                    table['split_table_name'] = new_name

            # Write temporary configuration
            temp_config_file = f'temp_{base_name}_{split_strategy}.json'
            with open(temp_config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)

            return temp_config_file

        except Exception as e:
            self.logger.error(f"Error preparing configuration: {str(e)}")
            raise

    def read_sql_queries(self, filename):
        """Read and clean SQL queries from file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                sql_statements = []
                for line in f:
                    if line.strip():
                        sql = re.sub(r'^\d+\.\s*', '', line.strip())
                        sql = self.clean_sql(sql)
                        if sql:
                            sql_statements.append(sql)
                return sql_statements
        except Exception as e:
            self.logger.error(f"Error reading SQL queries: {str(e)}")
            raise

    def adjust_table_names(self, sql, data_size):
        """Adjust table names in SQL query for current test size"""
        for size in ['10w'] + self.data_sizes:
            old_table = f"{self.base_table_name}{size}"
            new_table = f"{self.base_table_name}{data_size}"
            sql = re.sub(rf'\b{old_table}\b', new_table, sql)
        return sql

    def run_test_query(self, config_file, sql):
        """Execute a single test query with proper error handling"""
        try:
            start_time = time.time()

            # Execute the query
            result = compose_result(config_file, sql, BasicExecutor)
            execution_time = time.time() - start_time

            # Handle None result
            if result is None:
                self.logger.info("Query returned no results")
                return pd.DataFrame(), execution_time

            # Handle case where result is already a DataFrame
            if isinstance(result, pd.DataFrame):
                return result, execution_time

            # Try to convert result to DataFrame if it's not already one
            try:
                df_result = pd.DataFrame(result)
                return df_result, execution_time
            except Exception as e:
                self.logger.warning(f"Could not convert result to DataFrame: {str(e)}")
                return pd.DataFrame(), execution_time

        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            raise

    def test_single_scenario(self, data_size, split_strategy):
        """Test a single scenario with comprehensive error handling"""
        self.logger.info(f"Starting test: data_size={data_size}, split_strategy={split_strategy}")

        config_file = None
        results = {
            'total_time': 0,
            'success_count': 0,
            'failure_count': 0,
            'queries_executed': 0,
            'empty_results': 0
        }

        try:
            config_file = self.prepare_test_config(data_size, split_strategy)
            sql_statements = self.read_sql_queries('sqlSimple_100.txt')
            #sql_statements = self.read_sql_queries('sqlComplexTest.txt')

            total_queries = len(sql_statements)
            self.logger.info(f"Found {total_queries} queries to execute")

            for i, sql in enumerate(sql_statements, 1):
                try:
                    test_sql = self.adjust_table_names(sql, data_size)
                    self.logger.info(f"Executing SQL {i}/{total_queries}: {test_sql}")

                    result, execution_time = self.run_test_query(config_file, test_sql)

                    # Track empty results
                    if result.empty:
                        results['empty_results'] += 1
                        self.logger.info(f"Query {i} returned empty result set")

                    results['queries_executed'] += 1
                    results['total_time'] += execution_time
                    results['success_count'] += 1

                    self.logger.info(
                        f"Query {i} completed in {execution_time:.4f} seconds"
                    )

                except Exception as e:
                    results['failure_count'] += 1
                    self.logger.error(f"Failed to execute query {i}: {str(e)}")
                    continue

        finally:
            if config_file and Path(config_file).exists():
                try:
                    Path(config_file).unlink()
                except Exception as e:
                    self.logger.error(f"Failed to clean up temporary file: {str(e)}")

        if results['success_count'] > 0:
            avg_time = results['total_time'] / results['success_count']
            self.logger.info(
                f"Scenario completed: {results['success_count']} successful, "
                f"{results['failure_count']} failed, {results['empty_results']} empty results, "
                f"average time {avg_time:.4f}s"
            )
            return avg_time

        self.logger.warning("No successful queries in this scenario")
        return None

    def run_all_tests(self):
        """Run all test scenarios and save results"""
        results = []

        for data_size in self.data_sizes:
            row = {'数据量': data_size}
            for strategy in self.split_strategies:
                try:
                    self.logger.info(f"Testing {data_size} with {strategy} strategy")
                    avg_time = self.test_single_scenario(data_size, strategy)
                    row[f'{strategy}策略平均耗时(秒)'] = avg_time

                    # Save intermediate results
                    pd.DataFrame([row]).to_excel(
                        f'test_result_{data_size}_{strategy}.xlsx',
                        index=False
                    )

                except Exception as e:
                    self.logger.error(f"Failed scenario {data_size}-{strategy}: {str(e)}")
                    row[f'{strategy}策略平均耗时(秒)'] = None
            results.append(row)

        # Save final results
        try:
            pd.DataFrame(results).to_excel(
                'sql_performance_test_results_Simplest_new_basic.xlsx',
                index=False
            )
            self.logger.info("All tests completed and results saved")
        except Exception as e:
            self.logger.error(f"Failed to save final results: {str(e)}")


def main():
    try:
        tester = SQLPerformanceTester()
        tester.run_all_tests()
    except Exception as e:
        logging.error(f"Program execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()