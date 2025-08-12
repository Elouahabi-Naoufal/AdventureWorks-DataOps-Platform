"""
Data Quality Control DAG
Daily data quality checks across all tables with anomaly detection and data validation.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='data_quality_control',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': True,
    },
    tags=['data_quality', 'validation', 'daily'],
    description='Daily data quality control and validation checks',
)
def data_quality_control():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS data_quality_checks (
            check_date DATE,
            table_name VARCHAR(100),
            check_type VARCHAR(50),
            check_description VARCHAR(200),
            expected_value DECIMAL(19,4),
            actual_value DECIMAL(19,4),
            status VARCHAR(20),
            severity VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS data_anomalies (
            detection_date DATE,
            table_name VARCHAR(100),
            anomaly_type VARCHAR(50),
            anomaly_description TEXT,
            severity VARCHAR(10),
            recommended_action TEXT,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check table row counts
    check_row_counts = SQLExecuteQueryOperator(
        task_id='check_row_counts',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO data_quality_checks (
            check_date, table_name, check_type, check_description, 
            expected_value, actual_value, status, severity
        )
        WITH table_counts AS (
            SELECT 'SALES_SALESORDERHEADER' as table_name, COUNT(*) as row_count FROM dbt_schema.SALES_SALESORDERHEADER
            UNION ALL
            SELECT 'SALES_SALESORDERDETAIL', COUNT(*) FROM dbt_schema.SALES_SALESORDERDETAIL
            UNION ALL
            SELECT 'SALES_CUSTOMER', COUNT(*) FROM dbt_schema.SALES_CUSTOMER
            UNION ALL
            SELECT 'PRODUCTION_PRODUCT', COUNT(*) FROM dbt_schema.PRODUCTION_PRODUCT
        ),
        expected_counts AS (
            SELECT 'SALES_SALESORDERHEADER' as table_name, 1000 as expected_min
            UNION ALL
            SELECT 'SALES_SALESORDERDETAIL', 2000
            UNION ALL
            SELECT 'SALES_CUSTOMER', 500
            UNION ALL
            SELECT 'PRODUCTION_PRODUCT', 100
        )
        SELECT 
            CURRENT_DATE,
            tc.table_name,
            'ROW_COUNT',
            'Minimum row count validation',
            ec.expected_min,
            tc.row_count,
            CASE WHEN tc.row_count >= ec.expected_min THEN 'PASS' ELSE 'FAIL' END,
            CASE WHEN tc.row_count >= ec.expected_min THEN 'INFO' ELSE 'HIGH' END
        FROM table_counts tc
        JOIN expected_counts ec ON tc.table_name = ec.table_name;
        """,
    )
    
    # Task 3: Check for null values in critical columns
    check_null_values = SQLExecuteQueryOperator(
        task_id='check_null_values',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO data_quality_checks (
            check_date, table_name, check_type, check_description, 
            expected_value, actual_value, status, severity
        )
        WITH null_checks AS (
            SELECT 
                'SALES_SALESORDERHEADER' as table_name,
                'CustomerID' as column_name,
                COUNT(*) as total_rows,
                COUNT(CustomerID) as non_null_rows
            FROM dbt_schema.SALES_SALESORDERHEADER
            UNION ALL
            SELECT 
                'SALES_SALESORDERDETAIL',
                'ProductID',
                COUNT(*),
                COUNT(ProductID)
            FROM dbt_schema.SALES_SALESORDERDETAIL
            UNION ALL
            SELECT 
                'PRODUCTION_PRODUCT',
                'Name',
                COUNT(*),
                COUNT(Name)
            FROM dbt_schema.PRODUCTION_PRODUCT
        )
        SELECT 
            CURRENT_DATE,
            table_name,
            'NULL_CHECK',
            'Critical column null value check for ' || column_name,
            total_rows,
            non_null_rows,
            CASE WHEN non_null_rows = total_rows THEN 'PASS' ELSE 'FAIL' END,
            CASE WHEN non_null_rows = total_rows THEN 'INFO' ELSE 'MEDIUM' END
        FROM null_checks;
        """,
    )
    
    # Task 4: Check data freshness
    check_data_freshness = SQLExecuteQueryOperator(
        task_id='check_data_freshness',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO data_quality_checks (
            check_date, table_name, check_type, check_description, 
            expected_value, actual_value, status, severity
        )
        WITH freshness_checks AS (
            SELECT 
                'SALES_SALESORDERHEADER' as table_name,
                DATEDIFF(day, MAX(OrderDate), CURRENT_DATE) as days_since_last_record
            FROM dbt_schema.SALES_SALESORDERHEADER
            UNION ALL
            SELECT 
                'SALES_SALESORDERDETAIL',
                DATEDIFF(day, MAX(ModifiedDate), CURRENT_DATE)
            FROM dbt_schema.SALES_SALESORDERDETAIL
        )
        SELECT 
            CURRENT_DATE,
            table_name,
            'FRESHNESS',
            'Data freshness check - days since last record',
            7, -- Expected: data should be no older than 7 days
            days_since_last_record,
            CASE WHEN days_since_last_record <= 7 THEN 'PASS' ELSE 'FAIL' END,
            CASE WHEN days_since_last_record <= 7 THEN 'INFO' ELSE 'HIGH' END
        FROM freshness_checks;
        """,
    )
    
    # Task 5: Detect anomalies
    detect_anomalies = SQLExecuteQueryOperator(
        task_id='detect_anomalies',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO data_anomalies (
            detection_date, table_name, anomaly_type, 
            anomaly_description, severity, recommended_action
        )
        WITH sales_anomalies AS (
            SELECT 
                h.OrderDate,
                COUNT(*) as daily_orders,
                SUM(h.TotalDue) as daily_revenue
            FROM dbt_schema.SALES_SALESORDERHEADER h
            WHERE h.OrderDate >= CURRENT_DATE - 30
            GROUP BY h.OrderDate
        ),
        anomaly_detection AS (
            SELECT 
                AVG(daily_orders) as avg_orders,
                STDDEV(daily_orders) as stddev_orders,
                AVG(daily_revenue) as avg_revenue,
                STDDEV(daily_revenue) as stddev_revenue
            FROM sales_anomalies
        ),
        yesterday_stats AS (
            SELECT 
                daily_orders,
                daily_revenue
            FROM sales_anomalies
            WHERE OrderDate = CURRENT_DATE - 1
        )
        SELECT 
            CURRENT_DATE,
            'SALES_SALESORDERHEADER',
            'ORDER_VOLUME_ANOMALY',
            'Daily order count significantly different from average: ' || 
            COALESCE(ys.daily_orders, 0) || ' vs avg ' || ROUND(ad.avg_orders, 0),
            CASE 
                WHEN ABS(COALESCE(ys.daily_orders, 0) - ad.avg_orders) > (2 * ad.stddev_orders) THEN 'HIGH'
                WHEN ABS(COALESCE(ys.daily_orders, 0) - ad.avg_orders) > ad.stddev_orders THEN 'MEDIUM'
                ELSE 'LOW'
            END,
            'Investigate order processing system and data pipeline'
        FROM anomaly_detection ad
        CROSS JOIN yesterday_stats ys
        WHERE ABS(COALESCE(ys.daily_orders, 0) - ad.avg_orders) > ad.stddev_orders
        
        UNION ALL
        
        SELECT 
            CURRENT_DATE,
            'SALES_SALESORDERHEADER',
            'REVENUE_ANOMALY',
            'Daily revenue significantly different from average: $' || 
            ROUND(COALESCE(ys.daily_revenue, 0), 2) || ' vs avg $' || ROUND(ad.avg_revenue, 2),
            CASE 
                WHEN ABS(COALESCE(ys.daily_revenue, 0) - ad.avg_revenue) > (2 * ad.stddev_revenue) THEN 'HIGH'
                WHEN ABS(COALESCE(ys.daily_revenue, 0) - ad.avg_revenue) > ad.stddev_revenue THEN 'MEDIUM'
                ELSE 'LOW'
            END,
            'Review pricing strategy and transaction processing'
        FROM anomaly_detection ad
        CROSS JOIN yesterday_stats ys
        WHERE ABS(COALESCE(ys.daily_revenue, 0) - ad.avg_revenue) > ad.stddev_revenue;
        """,
    )
    
    # Task 6: Validate results and generate report
    @task(task_id='validate_and_report')
    def validate_and_report():
        """Validate quality checks and generate summary report"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get quality check summary
        quality_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            status,
            severity,
            COUNT(*) as check_count
        FROM data_quality_checks
        WHERE check_date = CURRENT_DATE
        GROUP BY status, severity
        ORDER BY 
            CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            CASE status WHEN 'FAIL' THEN 1 ELSE 2 END;
        """
        
        quality_results = hook.get_records(quality_sql)
        
        # Get anomaly summary
        anomaly_sql = """
        SELECT 
            severity,
            COUNT(*) as anomaly_count
        FROM data_anomalies
        WHERE detection_date = CURRENT_DATE
        GROUP BY severity
        ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;
        """
        
        anomaly_results = hook.get_records(anomaly_sql)
        
        # Log summary
        logging.info("=== DATA QUALITY REPORT ===")
        logging.info("Quality Checks:")
        for status, severity, count in quality_results:
            logging.info(f"  {status} ({severity}): {count} checks")
        
        logging.info("Anomalies Detected:")
        for severity, count in anomaly_results:
            logging.info(f"  {severity}: {count} anomalies")
        
        # Check for critical failures
        critical_failures = [r for r in quality_results if r[0] == 'FAIL' and r[1] == 'HIGH']
        if critical_failures:
            logging.error(f"CRITICAL: {sum(r[2] for r in critical_failures)} high-severity failures detected!")
        
        return 'success'
    
    # Task 7: Finalize
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
            'data_quality_control',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM data_quality_checks
        WHERE check_date = CURRENT_DATE;
        """,
    )
    
    # Define dependencies
    validation = validate_and_report()
    
    create_target_tables >> [check_row_counts, check_null_values, check_data_freshness]
    [check_row_counts, check_null_values, check_data_freshness] >> detect_anomalies
    detect_anomalies >> validation >> finalize

data_quality_control()