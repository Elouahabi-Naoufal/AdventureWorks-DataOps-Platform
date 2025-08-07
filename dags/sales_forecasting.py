"""
Sales Forecasting DAG
Weekly simple sales forecasting based on historical trends.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='sales_forecasting',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['sales', 'forecasting', 'weekly'],
    description='Weekly sales forecasting analysis',
)
def sales_forecasting():
    
    # Task 1: Create target table
    create_target_table = SQLExecuteQueryOperator(
        task_id='create_target_table',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS weekly_sales_forecast (
            forecast_date DATE,
            territory_id INTEGER,
            product_category VARCHAR(50),
            historical_avg DECIMAL(19,4),
            trend_factor DECIMAL(5,2),
            forecast_amount DECIMAL(19,4),
            confidence_level VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if historical sales data exists for forecasting"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM SALES_SALESORDERHEADER 
        WHERE OrderDate >= CURRENT_DATE - 30 
          AND Status = 5;
        """
        
        result = hook.get_first(sql)
        order_count = result[0] if result else 0
        
        logging.info(f"Found {order_count} orders in last 30 days for forecasting")
        
        if order_count < 10:
            logging.info("Insufficient data for forecasting - completing successfully")
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
        
        DELETE FROM weekly_sales_forecast 
        WHERE forecast_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Generate forecast
    generate_forecast = SQLExecuteQueryOperator(
        task_id='generate_forecast',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO weekly_sales_forecast (
            forecast_date,
            territory_id,
            product_category,
            historical_avg,
            trend_factor,
            forecast_amount,
            confidence_level
        )
        WITH historical_data AS (
            SELECT 
                h.TerritoryID,
                COALESCE(pc.Name, 'Unknown') as category,
                AVG(h.TotalDue) as avg_order_value,
                COUNT(*) as order_count,
                STDDEV(h.TotalDue) as std_dev
            FROM dbt_schema.SALES_SALESORDERHEADER h
            LEFT JOIN dbt_schema.SALES_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCT p ON d.ProductID = p.ProductID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTSUBCATEGORY ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTCATEGORY pc ON ps.ProductCategoryID = pc.ProductCategoryID
            WHERE h.OrderDate >= CURRENT_DATE - 30
              AND h.Status = 5
              AND h.TerritoryID IS NOT NULL
            GROUP BY h.TerritoryID, pc.Name
            HAVING COUNT(*) >= 3
        ),
        trend_analysis AS (
            SELECT 
                h.TerritoryID,
                COALESCE(pc.Name, 'Unknown') as category,
                -- Simple trend: compare last 2 weeks vs previous 2 weeks
                AVG(CASE WHEN h.OrderDate >= CURRENT_DATE - 14 THEN h.TotalDue END) as recent_avg,
                AVG(CASE WHEN h.OrderDate < CURRENT_DATE - 14 THEN h.TotalDue END) as older_avg
            FROM dbt_schema.SALES_SALESORDERHEADER h
            LEFT JOIN dbt_schema.SALES_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCT p ON d.ProductID = p.ProductID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTSUBCATEGORY ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTCATEGORY pc ON ps.ProductCategoryID = pc.ProductCategoryID
            WHERE h.OrderDate >= CURRENT_DATE - 30
              AND h.Status = 5
              AND h.TerritoryID IS NOT NULL
            GROUP BY h.TerritoryID, pc.Name
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as forecast_date,
            hd.TerritoryID,
            hd.category,
            hd.avg_order_value as historical_avg,
            CASE 
                WHEN ta.recent_avg > 0 AND ta.older_avg > 0 
                THEN ROUND(ta.recent_avg / ta.older_avg, 2)
                ELSE 1.0 
            END as trend_factor,
            hd.avg_order_value * 
            CASE 
                WHEN ta.recent_avg > 0 AND ta.older_avg > 0 
                THEN ta.recent_avg / ta.older_avg
                ELSE 1.0 
            END as forecast_amount,
            CASE 
                WHEN hd.order_count >= 10 AND hd.std_dev / hd.avg_order_value < 0.5 THEN 'HIGH'
                WHEN hd.order_count >= 5 THEN 'MEDIUM'
                ELSE 'LOW'
            END as confidence_level
        FROM historical_data hd
        LEFT JOIN trend_analysis ta 
            ON hd.TerritoryID = ta.TerritoryID 
            AND hd.category = ta.category;
        """,
    )
    
    # Task 5: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate results based on data availability"""
        if data_status == 'no_data':
            logging.info("Skipping validation - insufficient data for forecasting")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as forecast_count,
            SUM(forecast_amount) as total_forecast,
            COUNT(CASE WHEN confidence_level = 'HIGH' THEN 1 END) as high_confidence
        FROM weekly_sales_forecast
        WHERE forecast_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        result = hook.get_first(sql)
        forecast_count = result[0] if result else 0
        total_forecast = result[1] if result else 0
        high_confidence = result[2] if result else 0
        
        logging.info(f"Generated {forecast_count} forecasts, total: {total_forecast}, high confidence: {high_confidence}")
        
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
            'sales_forecasting',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM weekly_sales_forecast
        WHERE forecast_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_table >> data_check >> cleanup_data
    cleanup_data >> generate_forecast >> validation >> finalize

sales_forecasting()