"""
Customer Analytics DAG
Weekly Customer Lifetime Value (CLV) calculation and customer segmentation analysis.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='customer_analytics',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['customer', 'analytics', 'clv', 'segmentation', 'weekly'],
    description='Weekly Customer Lifetime Value and segmentation analysis',
)
def customer_analytics():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Customer Lifetime Value table
        CREATE TABLE IF NOT EXISTS customer_lifetime_value (
            analysis_date DATE,
            customer_id INTEGER,
            total_orders INTEGER,
            total_revenue DECIMAL(19,4),
            avg_order_value DECIMAL(19,4),
            first_order_date DATE,
            last_order_date DATE,
            customer_tenure_days INTEGER,
            predicted_clv DECIMAL(19,4),
            clv_segment VARCHAR(20),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Customer segmentation table
        CREATE TABLE IF NOT EXISTS customer_segmentation (
            analysis_date DATE,
            customer_id INTEGER,
            recency_score INTEGER,
            frequency_score INTEGER,
            monetary_score INTEGER,
            rfm_segment VARCHAR(20),
            segment_description VARCHAR(100),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if customer order data exists for analysis"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(DISTINCT CustomerID) FROM SALES_SALESORDERHEADER 
        WHERE Status = 5 AND CustomerID IS NOT NULL;
        """
        
        result = hook.get_first(sql)
        customer_count = result[0] if result else 0
        
        logging.info(f"Found {customer_count} customers with orders")
        
        if customer_count < 5:
            logging.info("Insufficient customer data for analysis - completing successfully")
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
        
        DELETE FROM customer_lifetime_value 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_segmentation 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Calculate Customer Lifetime Value
    calculate_clv = SQLExecuteQueryOperator(
        task_id='calculate_clv',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_lifetime_value (
            analysis_date,
            customer_id,
            total_orders,
            total_revenue,
            avg_order_value,
            first_order_date,
            last_order_date,
            customer_tenure_days,
            predicted_clv,
            clv_segment
        )
        WITH customer_metrics AS (
            SELECT 
                h.CustomerID,
                COUNT(DISTINCT h.SalesOrderID) as total_orders,
                SUM(h.TotalDue) as total_revenue,
                AVG(h.TotalDue) as avg_order_value,
                MIN(h.OrderDate) as first_order_date,
                MAX(h.OrderDate) as last_order_date,
                DATEDIFF(day, MIN(h.OrderDate), MAX(h.OrderDate)) + 1 as customer_tenure_days
            FROM dbt_schema.SALES_SALESORDERHEADER h
            WHERE h.Status = 5 
              AND h.CustomerID IS NOT NULL
            GROUP BY h.CustomerID
            HAVING COUNT(DISTINCT h.SalesOrderID) >= 1
        ),
        clv_calculation AS (
            SELECT 
                *,
                -- Simple CLV: (Average Order Value × Purchase Frequency × Customer Tenure in years)
                CASE 
                    WHEN customer_tenure_days > 0 
                    THEN avg_order_value * (total_orders / GREATEST(customer_tenure_days / 365.0, 0.1)) * 2
                    ELSE avg_order_value * total_orders
                END as predicted_clv
            FROM customer_metrics
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CustomerID,
            total_orders,
            total_revenue,
            avg_order_value,
            first_order_date,
            last_order_date,
            customer_tenure_days,
            predicted_clv,
            CASE 
                WHEN predicted_clv >= 10000 THEN 'HIGH_VALUE'
                WHEN predicted_clv >= 5000 THEN 'MEDIUM_VALUE'
                WHEN predicted_clv >= 1000 THEN 'LOW_VALUE'
                ELSE 'MINIMAL_VALUE'
            END as clv_segment
        FROM clv_calculation;
        """,
    )
    
    # Task 5: Perform RFM Segmentation
    perform_rfm_segmentation = SQLExecuteQueryOperator(
        task_id='perform_rfm_segmentation',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_segmentation (
            analysis_date,
            customer_id,
            recency_score,
            frequency_score,
            monetary_score,
            rfm_segment,
            segment_description
        )
        WITH customer_rfm AS (
            SELECT 
                h.CustomerID,
                DATEDIFF(day, MAX(h.OrderDate), CURRENT_DATE) as recency_days,
                COUNT(DISTINCT h.SalesOrderID) as frequency,
                SUM(h.TotalDue) as monetary_value
            FROM dbt_schema.SALES_SALESORDERHEADER h
            WHERE h.Status = 5 
              AND h.CustomerID IS NOT NULL
            GROUP BY h.CustomerID
        ),
        rfm_scores AS (
            SELECT 
                CustomerID,
                recency_days,
                frequency,
                monetary_value,
                -- Recency Score (1-5, where 5 is most recent)
                CASE 
                    WHEN recency_days <= 30 THEN 5
                    WHEN recency_days <= 60 THEN 4
                    WHEN recency_days <= 90 THEN 3
                    WHEN recency_days <= 180 THEN 2
                    ELSE 1
                END as recency_score,
                -- Frequency Score (1-5, where 5 is highest frequency)
                CASE 
                    WHEN frequency >= 10 THEN 5
                    WHEN frequency >= 7 THEN 4
                    WHEN frequency >= 4 THEN 3
                    WHEN frequency >= 2 THEN 2
                    ELSE 1
                END as frequency_score,
                -- Monetary Score (1-5, where 5 is highest value)
                CASE 
                    WHEN monetary_value >= 10000 THEN 5
                    WHEN monetary_value >= 5000 THEN 4
                    WHEN monetary_value >= 2000 THEN 3
                    WHEN monetary_value >= 500 THEN 2
                    ELSE 1
                END as monetary_score
            FROM customer_rfm
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CustomerID,
            recency_score,
            frequency_score,
            monetary_score,
            CASE 
                WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'CHAMPIONS'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'LOYAL_CUSTOMERS'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'POTENTIAL_LOYALISTS'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'NEW_CUSTOMERS'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 3 THEN 'PROMISING'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'NEED_ATTENTION'
                WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'CANNOT_LOSE'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'AT_RISK'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'LOST'
                ELSE 'OTHERS'
            END as rfm_segment,
            CASE 
                WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Best customers who buy frequently and recently'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'Regular high-value customers'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Recent customers with potential'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Recent first-time customers'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 3 THEN 'Engaged customers with growth potential'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Valuable customers becoming inactive'
                WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'High-value customers at risk of churning'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'High spenders who stopped buying'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Inactive low-value customers'
                ELSE 'Customers requiring further analysis'
            END as segment_description
        FROM rfm_scores;
        """,
    )
    
    # Task 6: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate results based on data availability"""
        if data_status == 'no_data':
            logging.info("Skipping validation - insufficient data for analysis")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Validate CLV results
        clv_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as total_customers,
            AVG(predicted_clv) as avg_clv,
            COUNT(CASE WHEN clv_segment = 'HIGH_VALUE' THEN 1 END) as high_value_customers
        FROM customer_lifetime_value
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        clv_result = hook.get_first(clv_sql)
        total_customers = clv_result[0] if clv_result else 0
        avg_clv = clv_result[1] if clv_result else 0
        high_value_customers = clv_result[2] if clv_result else 0
        
        # Validate RFM results
        rfm_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as segmented_customers,
            COUNT(CASE WHEN rfm_segment = 'CHAMPIONS' THEN 1 END) as champions,
            COUNT(CASE WHEN rfm_segment = 'LOYAL_CUSTOMERS' THEN 1 END) as loyal_customers
        FROM customer_segmentation
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        rfm_result = hook.get_first(rfm_sql)
        segmented_customers = rfm_result[0] if rfm_result else 0
        champions = rfm_result[1] if rfm_result else 0
        loyal_customers = rfm_result[2] if rfm_result else 0
        
        logging.info(f"CLV Analysis: {total_customers} customers, avg CLV: {avg_clv}, high-value: {high_value_customers}")
        logging.info(f"RFM Segmentation: {segmented_customers} customers, champions: {champions}, loyal: {loyal_customers}")
        
        return 'success'
    
    # Task 7: Generate insights summary
    generate_insights = SQLExecuteQueryOperator(
        task_id='generate_insights',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS customer_analytics_summary (
            analysis_date DATE,
            total_customers INTEGER,
            avg_clv DECIMAL(19,4),
            high_value_customers INTEGER,
            champions INTEGER,
            at_risk_customers INTEGER,
            lost_customers INTEGER,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        INSERT INTO customer_analytics_summary (
            analysis_date,
            total_customers,
            avg_clv,
            high_value_customers,
            champions,
            at_risk_customers,
            lost_customers
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            clv.total_customers,
            clv.avg_clv,
            clv.high_value_customers,
            rfm.champions,
            rfm.at_risk_customers,
            rfm.lost_customers
        FROM (
            SELECT 
                COUNT(*) as total_customers,
                AVG(predicted_clv) as avg_clv,
                COUNT(CASE WHEN clv_segment = 'HIGH_VALUE' THEN 1 END) as high_value_customers
            FROM customer_lifetime_value
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ) clv
        CROSS JOIN (
            SELECT 
                COUNT(CASE WHEN rfm_segment = 'CHAMPIONS' THEN 1 END) as champions,
                COUNT(CASE WHEN rfm_segment = 'AT_RISK' THEN 1 END) as at_risk_customers,
                COUNT(CASE WHEN rfm_segment = 'LOST' THEN 1 END) as lost_customers
            FROM customer_segmentation
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ) rfm;
        """,
    )
    
    # Task 8: Finalize
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
            'customer_analytics',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM customer_lifetime_value
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> [calculate_clv, perform_rfm_segmentation]
    [calculate_clv, perform_rfm_segmentation] >> validation >> generate_insights >> finalize

customer_analytics()