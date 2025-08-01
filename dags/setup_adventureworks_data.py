from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

@dag(
    dag_id='setup_adventureworks_data',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'data-population', 'adventureworks']
)
def setup_adventureworks_data():
    
    @task
    def read_sql_file():
        """Read the SQL population script"""
        sql_file_path = '/home/manipulator/Documents/Apps/Airflow-Final/populate_adventureworks_data.sql'
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        return sql_content
    
    @task
    def validate_database_connection():
        """Validate Snowflake connection and database access"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        result = hook.get_first("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        return {
            'database': result[0],
            'schema': result[1], 
            'warehouse': result[2]
        }
    
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema_if_not_exists',
        conn_id='snowflake_conn',
        sql="CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema"
    )
    
    use_schema = SQLExecuteQueryOperator(
        task_id='use_target_schema',
        conn_id='snowflake_conn',
        sql="USE SCHEMA dbt_db.dbt_schema"
    )
    
    @task
    def execute_population_script(sql_content, connection_info):
        """Execute the population script in chunks"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Split SQL into individual statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        executed_count = 0
        for statement in statements:
            if statement and not statement.startswith('--'):
                try:
                    hook.run(statement)
                    executed_count += 1
                except Exception as e:
                    print(f"Warning: Statement failed: {str(e)[:100]}...")
                    continue
        
        return {
            'total_statements': len(statements),
            'executed_successfully': executed_count,
            'database_info': connection_info
        }
    
    @task
    def verify_data_population():
        """Verify that data was populated correctly"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        verification_queries = [
            ("Person_Person", "SELECT COUNT(*) FROM dbt_db.dbt_schema.Person_Person"),
            ("Sales_SalesOrderHeader", "SELECT COUNT(*) FROM dbt_db.dbt_schema.Sales_SalesOrderHeader"),
            ("Production_Product", "SELECT COUNT(*) FROM dbt_db.dbt_schema.Production_Product"),
            ("HumanResources_Employee", "SELECT COUNT(*) FROM dbt_db.dbt_schema.HumanResources_Employee"),
            ("Purchasing_Vendor", "SELECT COUNT(*) FROM dbt_db.dbt_schema.Purchasing_Vendor")
        ]
        
        results = {}
        for table_name, query in verification_queries:
            try:
                count = hook.get_first(query)[0]
                results[table_name] = count
            except Exception as e:
                results[table_name] = f"Error: {str(e)}"
        
        return results
    
    @task
    def generate_setup_report(execution_result, verification_result):
        """Generate final setup report"""
        report = {
            'setup_completed': True,
            'execution_summary': execution_result,
            'data_verification': verification_result,
            'next_steps': [
                'Run sales_analytics_pipeline DAG',
                'Run product_inventory_management DAG', 
                'Run hr_employee_analytics DAG',
                'Run customer_purchasing_insights DAG'
            ]
        }
        
        print("=== Adventure Works Setup Complete ===")
        print(f"Database: {execution_result['database_info']['database']}")
        print(f"Schema: {execution_result['database_info']['schema']}")
        print(f"Statements executed: {execution_result['executed_successfully']}")
        print("Data verification:")
        for table, count in verification_result.items():
            print(f"  {table}: {count} rows")
        
        return report
    
    # Task dependencies
    sql_content = read_sql_file()
    connection_info = validate_database_connection()
    
    create_schema >> use_schema
    
    execution_result = execute_population_script(sql_content, connection_info)
    execution_result.set_upstream(use_schema)
    
    verification_result = verify_data_population()
    verification_result.set_upstream(execution_result)
    
    generate_setup_report(execution_result, verification_result)

setup_adventureworks_data()