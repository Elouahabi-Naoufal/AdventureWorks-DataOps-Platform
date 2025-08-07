"""
Currency Exchange Rates Management DAG
Daily processing of currency exchange rates for international sales.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='currency_exchange_rates',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['sales', 'currency', 'daily'],
    description='Daily currency exchange rates processing',
)
def currency_exchange_rates():
    
    # Task 1: Create target table
    create_target_table = SQLExecuteQueryOperator(
        task_id='create_target_table',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS daily_currency_rates (
            rate_date DATE,
            from_currency VARCHAR(3),
            to_currency VARCHAR(3),
            exchange_rate DECIMAL(19,6),
            sales_volume DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if currency rate data exists for yesterday"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM SALES_CURRENCYRATE 
        WHERE CurrencyRateDate >= CURRENT_DATE - 1 
          AND CurrencyRateDate < CURRENT_DATE;
        """
        
        result = hook.get_first(sql)
        rate_count = result[0] if result else 0
        
        logging.info(f"Found {rate_count} currency rates for yesterday")
        
        if rate_count == 0:
            logging.info("No currency data found - completing successfully")
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
        
        DELETE FROM daily_currency_rates 
        WHERE rate_date = CURRENT_DATE - 1;
        """,
    )
    
    # Task 4: Extract and process rates
    extract_rates = SQLExecuteQueryOperator(
        task_id='extract_rates',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO daily_currency_rates (
            rate_date,
            from_currency,
            to_currency,
            exchange_rate,
            sales_volume
        )
        SELECT 
            cr.CurrencyRateDate as rate_date,
            cr.FromCurrencyCode as from_currency,
            cr.ToCurrencyCode as to_currency,
            cr.AverageRate as exchange_rate,
            COALESCE(sales.volume, 0) as sales_volume
        FROM dbt_schema.SALES_CURRENCYRATE cr
        LEFT JOIN (
            SELECT 
                h.CurrencyRateID,
                SUM(h.TotalDue) as volume
            FROM dbt_schema.SALES_SALESORDERHEADER h
            WHERE h.OrderDate >= CURRENT_DATE - 1
              AND h.OrderDate < CURRENT_DATE
              AND h.CurrencyRateID IS NOT NULL
            GROUP BY h.CurrencyRateID
        ) sales ON cr.CurrencyRateID = sales.CurrencyRateID
        WHERE cr.CurrencyRateDate >= CURRENT_DATE - 1
          AND cr.CurrencyRateDate < CURRENT_DATE;
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
            COUNT(*) as rate_count,
            SUM(sales_volume) as total_volume
        FROM daily_currency_rates
        WHERE rate_date = CURRENT_DATE - 1;
        """
        
        result = hook.get_first(sql)
        rate_count = result[0] if result else 0
        total_volume = result[1] if result else 0
        
        logging.info(f"Processed {rate_count} currency rates with total volume: {total_volume}")
        
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
            'currency_exchange_rates',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM daily_currency_rates
        WHERE rate_date = CURRENT_DATE - 1;
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_table >> data_check >> cleanup_data
    cleanup_data >> extract_rates >> validation >> finalize

currency_exchange_rates()