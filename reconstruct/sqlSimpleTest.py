import json
import time
import pandas as pd
import logging
from pathlib import Path
import re
from executor import BasicExecutor, QueryLeastExecutor
from sql_service_advance_old import compose_result, build_connection
import psutil
import signal
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event


class SQLPerformanceTester:
    def __init__(self, base_table_name="meeting"):
        # 配置日志
        logging.basicConfig(
            filename='sql_test_detailed.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # 基础配置
        self.data_sizes = ['500w']
        self.split_strategies = ['n4']
        self.base_table_name = base_table_name
        self.query_timeout = 60  # 查询超时时间（秒）

        # 性能监控阈值
        self.memory_threshold = 80  # 内存使用率阈值
        self.cpu_threshold = 80  # CPU使用率阈值

        # 添加控制台日志
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)

    def monitor_system_resources(self):
        """监控系统资源使用情况"""
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        return {
            'cpu_usage': cpu_percent,
            'memory_usage': memory_percent
        }

    def validate_database_connection(self):
        """验证数据库连接并检查表存在性"""
        try:
            connection = build_connection()
            return connection
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise

    def validate_table_existence(self, connection, table_name):
        """验证表是否存在并检查表状态"""
        try:
            cursor = connection.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            exists = cursor.fetchone() is not None

            if exists:
                # 检查表状态
                cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
                status = cursor.fetchone()
                self.logger.info(f"Table {table_name} status: Rows={status['Rows']}, "
                                 f"Size={status['Data_length'] / 1024 / 1024:.2f}MB")
            else:
                self.logger.warning(f"Table {table_name} does not exist")

            return exists
        except Exception as e:
            self.logger.error(f"Error checking table {table_name}: {str(e)}")
            return False

    def get_table_name(self, base_name, table_type, server_num, strategy):
        """生成正确的表名"""
        if table_type == 'relation':
            return f"{base_name}_relation_{strategy}"
        return f"{base_name}_server{server_num}_{strategy}"

    def prepare_test_config(self, data_size, split_strategy):
        """准备测试配置并验证数据库环境"""
        self.logger.info(f"Preparing config for size {data_size} with strategy {split_strategy}")

        template_file = f'table_config_{split_strategy}.json'
        connection = None
        try:
            # 读取配置模板
            with open(template_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            base_name = f"{self.base_table_name}{data_size}"
            config['real_table_name'] = base_name

            # 验证数据库连接和表
            connection = self.validate_database_connection()

            # 获取关系表配置
            relation_columns = None
            for table in config['split_tables']:
                if 'relation' in table['split_table_name']:
                    relation_columns = table['columns']
                    break

            # 更新表名和验证表存在性
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

                # 验证表存在性
                if not self.validate_table_existence(connection, new_name):
                    raise Exception(f"Required table {new_name} does not exist")

            # 写入临时配置
            temp_config_file = f'temp_{base_name}_{split_strategy}.json'
            with open(temp_config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)

            return temp_config_file

        finally:
            if connection:
                connection.close()

    def execute_with_timeout(self, config_file, sql):
        """使用超时机制执行查询"""
        stop_event = Event()

        def execute_query():
            try:
                return compose_result(config_file, sql, QueryLeastExecutor)
            except Exception as e:
                self.logger.error(f"Query execution error: {str(e)}")
                raise

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute_query)
            try:
                result = future.result(timeout=self.query_timeout)
                return result
            except TimeoutError:
                stop_event.set()
                self.logger.error(f"Query timed out after {self.query_timeout} seconds")
                raise TimeoutError(f"Query execution timed out after {self.query_timeout} seconds")

    def run_test_query(self, config_file, sql):
        """执行测试查询并监控性能"""
        try:
            # 检查系统资源
            resources = self.monitor_system_resources()
            if resources['cpu_usage'] > self.cpu_threshold or \
                    resources['memory_usage'] > self.memory_threshold:
                self.logger.warning("System resources are under heavy load")
                time.sleep(5)  # 等待系统资源释放

            start_time = time.time()
            result = self.execute_with_timeout(config_file, sql)
            execution_time = time.time() - start_time

            if result is None:
                return pd.DataFrame(), execution_time
            return result, execution_time

        except TimeoutError:
            raise
        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            raise

    def adjust_table_names(self, sql, data_size):
        """调整SQL中的表名"""
        for size in ['10w'] + self.data_sizes:
            old_table = f"{self.base_table_name}{size}"
            new_table = f"{self.base_table_name}{data_size}"
            sql = re.sub(rf'\b{old_table}\b', new_table, sql)
        return sql

    def test_single_scenario(self, data_size, split_strategy):
        """测试单个场景"""
        self.logger.info(f"Starting test: data_size={data_size}, split_strategy={split_strategy}")

        config_file = None
        results = {
            'total_time': 0,
            'success_count': 0,
            'failure_count': 0,
            'queries_executed': 0
        }

        try:
            config_file = self.prepare_test_config(data_size, split_strategy)
            sql_statements = self.read_sql_queries('sqlComplexTest.txt')

            total_queries = len(sql_statements)
            self.logger.info(f"Found {total_queries} queries to execute")

            for i, sql in enumerate(sql_statements, 1):
                try:
                    # 调整查询超时时间基于数据量
                    self.query_timeout = min(60 + (int(data_size[:-1]) // 100) * 10, 300)

                    test_sql = self.adjust_table_names(sql, data_size)
                    self.logger.info(f"Executing SQL {i}/{total_queries}: {test_sql}")

                    result, execution_time = self.run_test_query(config_file, test_sql)

                    results['queries_executed'] += 1
                    results['total_time'] += execution_time
                    results['success_count'] += 1

                    self.logger.info(
                        f"Query {i} completed successfully in {execution_time:.4f} seconds"
                    )

                except Exception as e:
                    results['failure_count'] += 1
                    self.logger.error(f"Failed to execute query {i}: {str(e)}")
                    continue  # 继续执行下一个查询

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
                f"{results['failure_count']} failed, average time {avg_time:.4f}s"
            )
            return avg_time

        self.logger.warning("No successful queries in this scenario")
        return None

    def read_sql_queries(self, filename):
        """读取SQL查询"""
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

    def clean_sql(self, sql):
        """清理SQL语句"""
        sql = sql.strip().rstrip(';')
        sql = ' '.join(sql.split())
        return sql

    def run_all_tests(self):
        """运行所有测试场景"""
        results = []

        for data_size in self.data_sizes:
            row = {'数据量': data_size}
            for strategy in self.split_strategies:
                try:
                    self.logger.info(f"Testing {data_size} with {strategy} strategy")
                    avg_time = self.test_single_scenario(data_size, strategy)
                    row[f'{strategy}策略平均耗时(秒)'] = avg_time

                    # 保存中间结果
                    pd.DataFrame([row]).to_excel(
                        f'test_result_{data_size}_{strategy}.xlsx',
                        index=False
                    )

                except Exception as e:
                    self.logger.error(f"Failed scenario {data_size}-{strategy}: {str(e)}")
                    row[f'{strategy}策略平均耗时(秒)'] = None
            results.append(row)

        # 保存最终结果
        pd.DataFrame(results).to_excel(
            'sql_performance_test_results_complex_new.xlsx',
            index=False
        )
        self.logger.info("All tests completed")


def main():
    try:
        tester = SQLPerformanceTester()
        tester.run_all_tests()
    except Exception as e:
        logging.error(f"Program execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()