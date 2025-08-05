"""
Daily Sales ETL Pipeline
Extracts sales data from previous day and creates aggregated summary by product, territory, and salesperson.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='daily_sales_etl',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['sales', 'etl', 'daily'],
    description='Daily ETL for sales aggregation by product, territory, and salesperson',
)
def daily_sales_etl():
    
    # Task 1: Setup - Create schema and table
    setup_warehouse = SQLExecuteQueryOperator(
        task_id='setup_warehouse',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        
        -- Create warehouse schema if not exists
        CREATE SCHEMA IF NOT EXISTS dbt_warehouse;
        USE SCHEMA dbt_warehouse;
        
        -- Create target table
        CREATE TABLE IF NOT EXISTS daily_sales_summary (
            summary_date DATE,
            product_id INTEGER,
            territory_id INTEGER,
            salesperson_id INTEGER,
            total_quantity INTEGER,
            total_revenue DECIMAL(19,4),
            order_count INTEGER,
            avg_order_value DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Create ETL log table
        CREATE TABLE IF NOT EXISTS etl_execution_log (
            execution_date DATE,
            dag_id VARCHAR(100),
            task_id VARCHAR(100),
            status VARCHAR(20),
            records_processed INTEGER,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Data validation - Check source data availability
    @task(task_id='validate_source_data')
    def validate_source_data():
        """Validate that source data exists for the previous day"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Check if source data exists for previous day
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) as order_count
        FROM SALES_SALESORDERHEADER 
        WHERE OrderDate = CURRENT_DATE - 1;
        """
        
        result = hook.get_first(sql)
        order_count = result[0] if result else 0
        
        logging.info(f"Found {order_count} orders for previous day")
        
        if order_count == 0:
            logging.warning("No orders found for previous day - this may be expected for weekends/holidays")
        
        return order_count
    
    # Task 3: Clean previous data for the same date
    cleanup_previous_data = SQLExecuteQueryOperator(
        task_id='cleanup_previous_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Remove any existing data for the same date (for reprocessing)
        DELETE FROM daily_sales_summary 
        WHERE summary_date = CURRENT_DATE - 1;
        
        -- Log cleanup
        INSERT INTO etl_execution_log (execution_date, dag_id, task_id, status)
        VALUES (CURRENT_DATE - 1, 'daily_sales_etl', 'cleanup_previous_data', 'SUCCESS');
        """,
    )
    
    # Task 4: Extract and aggregate sales data
    extract_and_aggregate = SQLExecuteQueryOperator(
        task_id='extract_and_aggregate',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Insert aggregated sales data
        INSERT INTO daily_sales_summary (
            summary_date,
            product_id,
            territory_id,
            salesperson_id,
            total_quantity,
            total_revenue,
            order_count
        )
        SELECT 
            h.OrderDate as summary_date,
            d.ProductID as product_id,
            h.TerritoryID as territory_id,
            h.SalesPersonID as salesperson_id,
            SUM(d.OrderQty) as total_quantity,
            SUM(d.LineTotal) as total_revenue,
            COUNT(DISTINCT h.SalesOrderID) as order_count
        FROM dbt_schema.SALES_SALESORDERHEADER h
        INNER JOIN dbt_schema.SALES_SALESORDERDETAIL d 
            ON h.SalesOrderID = d.SalesOrderID
        WHERE h.OrderDate = CURRENT_DATE - 1
          AND h.Status = 5  -- Only completed orders
        GROUP BY 
            h.OrderDate,
            d.ProductID,
            h.TerritoryID,
            h.SalesPersonID
        HAVING SUM(d.LineTotal) > 0;  -- Only positive revenue
        """,
    )
    
    # Task 5: Data quality checks
    @task(task_id='data_quality_checks')
    def perform_data_quality_checks():
        """Perform data quality validation on loaded data"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Check 1: Record count validation
        count_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT COUNT(*) FROM daily_sales_summary 
        WHERE summary_date = CURRENT_DATE - 1;
        """
        
        record_count = hook.get_first(count_sql)[0]
        logging.info(f"Loaded {record_count} records into daily_sales_summary")
        
        # Check 2: Revenue validation
        revenue_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            SUM(total_revenue) as total_revenue,
            MIN(total_revenue) as min_revenue,
            MAX(total_revenue) as max_revenue
        FROM daily_sales_summary 
        WHERE summary_date = CURRENT_DATE - 1;
        """
        
        revenue_stats = hook.get_first(revenue_sql)
        total_revenue = revenue_stats[0] if revenue_stats[0] else 0
        min_revenue = revenue_stats[1] if revenue_stats[1] else 0
        max_revenue = revenue_stats[2] if revenue_stats[2] else 0
        
        logging.info(f"Revenue stats - Total: {total_revenue}, Min: {min_revenue}, Max: {max_revenue}")
        
        # Validation checks
        if record_count == 0:
            logging.warning("No records loaded - may be expected for weekends/holidays")
        
        if min_revenue < 0:
            raise ValueError(f"Found negative revenue: {min_revenue}")
        
        return {
            'record_count': record_count,
            'total_revenue': float(total_revenue) if total_revenue else 0,
            'min_revenue': float(min_revenue) if min_revenue else 0,
            'max_revenue': float(max_revenue) if max_revenue else 0
        }
    
    # Task 6: Generate summary statistics
    generate_statistics = SQLExecuteQueryOperator(
        task_id='generate_statistics',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Create daily statistics table if not exists
        CREATE TABLE IF NOT EXISTS daily_etl_statistics (
            summary_date DATE,
            total_records INTEGER,
            total_revenue DECIMAL(19,4),
            unique_products INTEGER,
            unique_territories INTEGER,
            unique_salespersons INTEGER,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Insert daily statistics
        INSERT INTO daily_etl_statistics (
            summary_date,
            total_records,
            total_revenue,
            unique_products,
            unique_territories,
            unique_salespersons
        )
        SELECT 
            summary_date,
            COUNT(*) as total_records,
            SUM(total_revenue) as total_revenue,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT territory_id) as unique_territories,
            COUNT(DISTINCT salesperson_id) as unique_salespersons
        FROM daily_sales_summary
        WHERE summary_date = CURRENT_DATE - 1
        GROUP BY summary_date;
        """,
    )
    
    # Task 7: Finalize ETL process
    finalize_etl = SQLExecuteQueryOperator(
        task_id='finalize_etl',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Table statistics are automatically maintained in Snowflake
        
        -- Log successful completion
        INSERT INTO etl_execution_log (execution_date, dag_id, task_id, status, records_processed)
        SELECT 
            CURRENT_DATE - 1,
            'daily_sales_etl',
            'finalize_etl',
            'SUCCESS',
            COUNT(*)
        FROM daily_sales_summary
        WHERE summary_date = CURRENT_DATE - 1;
        """,
    )
    
    # Define task dependencies
    validate_source = validate_source_data()
    quality_checks = perform_data_quality_checks()
    
    setup_warehouse >> validate_source >> cleanup_previous_data
    cleanup_previous_data >> extract_and_aggregate >> quality_checks
    quality_checks >> generate_statistics >> finalize_etl

daily_sales_etl()