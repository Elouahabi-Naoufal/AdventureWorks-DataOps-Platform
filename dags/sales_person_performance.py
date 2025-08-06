"""
Sales Person Performance Analysis DAG
Weekly analysis of sales performance per salesperson from Snowflake database.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='sales_person_performance',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['sales', 'performance', 'weekly'],
    description='Weekly sales person performance analysis',
)
def sales_person_performance():
    
    # Task 1: Create target table
    create_target_table = SQLExecuteQueryOperator(
        task_id='create_target_table',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS weekly_salesperson_metrics (
            week_start_date DATE,
            salesperson_id INTEGER,
            total_orders INTEGER,
            total_revenue DECIMAL(19,4),
            avg_revenue_per_order DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if source data exists for last 7 days"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM SALES_SALESORDERHEADER 
        WHERE OrderDate >= CURRENT_DATE - 7 
          AND OrderDate < CURRENT_DATE
          AND Status = 5;
        """
        
        result = hook.get_first(sql)
        order_count = result[0] if result else 0
        
        logging.info(f"Found {order_count} orders in last 7 days")
        
        if order_count == 0:
            logging.info("No data found - completing successfully")
            return 'no_data'
        
        return 'has_data'
    
    # Task 3: Clean existing data
    cleanup_data = SQLExecuteQueryOperator(
        task_id='cleanup_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        DELETE FROM weekly_salesperson_metrics 
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Extract and aggregate data
    extract_metrics = SQLExecuteQueryOperator(
        task_id='extract_metrics',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO weekly_salesperson_metrics (
            week_start_date,
            salesperson_id,
            total_orders,
            total_revenue,
            avg_revenue_per_order
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as week_start_date,
            h.SalesPersonID as salesperson_id,
            COUNT(DISTINCT h.SalesOrderID) as total_orders,
            SUM(d.LineTotal) as total_revenue,
            AVG(h.TotalDue) as avg_revenue_per_order
        FROM dbt_schema.SALES_SALESORDERHEADER h
        INNER JOIN dbt_schema.SALES_SALESORDERDETAIL d 
            ON h.SalesOrderID = d.SalesOrderID
        WHERE h.OrderDate >= CURRENT_DATE - 7
          AND h.OrderDate < CURRENT_DATE
          AND h.Status = 5
          AND h.SalesPersonID IS NOT NULL
        GROUP BY h.SalesPersonID
        HAVING SUM(d.LineTotal) > 0;
        """,
    )
    
    # Task 5: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate results based on data availability"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data to process")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as salesperson_count,
            SUM(total_revenue) as total_revenue
        FROM weekly_salesperson_metrics
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        result = hook.get_first(sql)
        salesperson_count = result[0] if result else 0
        total_revenue = result[1] if result else 0
        
        logging.info(f"Processed {salesperson_count} salespersons with total revenue: {total_revenue}")
        
        return 'success'
    
    # Task 6: Finalize
    finalize = SQLExecuteQueryOperator(
        task_id='finalize',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS etl_log (
            execution_date DATE,
            dag_id VARCHAR(100),
            status VARCHAR(20),
            records_processed INTEGER,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        INSERT INTO etl_log (execution_date, dag_id, status, records_processed)
        SELECT 
            CURRENT_DATE,
            'sales_person_performance',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM weekly_salesperson_metrics
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_table >> data_check >> cleanup_data
    cleanup_data >> extract_metrics >> validation >> finalize

sales_person_performance()