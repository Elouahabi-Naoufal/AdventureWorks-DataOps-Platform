"""
Credit Card Analysis DAG
Weekly analysis of credit card usage patterns, fraud detection, and payment trends.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='credit_card_analysis',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['credit_card', 'fraud', 'payment', 'weekly'],
    description='Weekly credit card usage and fraud analysis',
)
def credit_card_analysis():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS credit_card_usage_analysis (
            analysis_date DATE,
            card_type VARCHAR(50),
            total_transactions INTEGER,
            total_amount DECIMAL(19,4),
            avg_transaction_amount DECIMAL(19,4),
            unique_customers INTEGER,
            approval_rate DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS fraud_detection_metrics (
            analysis_date DATE,
            card_type VARCHAR(50),
            suspicious_transactions INTEGER,
            fraud_score_avg DECIMAL(5,2),
            high_risk_customers INTEGER,
            total_risk_amount DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if credit card data exists"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM SALES_CREDITCARD;
        """
        
        result = hook.get_first(sql)
        card_count = result[0] if result else 0
        
        logging.info(f"Found {card_count} credit cards")
        
        if card_count < 5:
            logging.info("Insufficient credit card data - completing successfully")
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
        
        DELETE FROM credit_card_usage_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM fraud_detection_metrics 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Analyze credit card usage
    analyze_card_usage = SQLExecuteQueryOperator(
        task_id='analyze_card_usage',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO credit_card_usage_analysis (
            analysis_date,
            card_type,
            total_transactions,
            total_amount,
            avg_transaction_amount,
            unique_customers,
            approval_rate
        )
        WITH card_transactions AS (
            SELECT 
                cc.CardType,
                COUNT(DISTINCT h.SalesOrderID) as total_transactions,
                SUM(h.TotalDue) as total_amount,
                COUNT(DISTINCT h.CustomerID) as unique_customers,
                COUNT(CASE WHEN h.Status = 5 THEN 1 END) as approved_transactions
            FROM dbt_schema.SALES_CREDITCARD cc
            LEFT JOIN dbt_schema.SALES_SALESORDERHEADER h ON cc.CreditCardID = h.CreditCardID
            WHERE h.OrderDate >= CURRENT_DATE - 7 OR h.OrderDate IS NULL
            GROUP BY cc.CardType
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CardType,
            total_transactions,
            total_amount,
            CASE WHEN total_transactions > 0 THEN total_amount / total_transactions ELSE 0 END as avg_transaction_amount,
            unique_customers,
            CASE WHEN total_transactions > 0 THEN ROUND((approved_transactions * 100.0 / total_transactions), 2) ELSE 0 END as approval_rate
        FROM card_transactions;
        """,
    )
    
    # Task 5: Fraud detection analysis
    fraud_detection_analysis = SQLExecuteQueryOperator(
        task_id='fraud_detection_analysis',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO fraud_detection_metrics (
            analysis_date,
            card_type,
            suspicious_transactions,
            fraud_score_avg,
            high_risk_customers,
            total_risk_amount
        )
        WITH fraud_analysis AS (
            SELECT 
                cc.CardType,
                h.CustomerID,
                h.TotalDue,
                -- Simple fraud scoring based on transaction patterns
                CASE 
                    WHEN h.TotalDue > 10000 THEN 80
                    WHEN h.TotalDue > 5000 THEN 60
                    WHEN h.Status != 5 THEN 70
                    ELSE 20
                END as fraud_score
            FROM dbt_schema.SALES_CREDITCARD cc
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON cc.CreditCardID = h.CreditCardID
            WHERE h.OrderDate >= CURRENT_DATE - 7
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CardType,
            COUNT(CASE WHEN fraud_score >= 60 THEN 1 END) as suspicious_transactions,
            AVG(fraud_score) as fraud_score_avg,
            COUNT(DISTINCT CASE WHEN fraud_score >= 70 THEN CustomerID END) as high_risk_customers,
            SUM(CASE WHEN fraud_score >= 60 THEN TotalDue ELSE 0 END) as total_risk_amount
        FROM fraud_analysis
        GROUP BY CardType;
        """,
    )
    
    # Task 6: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate analysis results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as card_types_analyzed,
            SUM(total_transactions) as total_transactions
        FROM credit_card_usage_analysis
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        result = hook.get_first(sql)
        card_types = result[0] if result else 0
        transactions = result[1] if result else 0
        
        logging.info(f"Analyzed {card_types} card types with {transactions} transactions")
        
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
            'credit_card_analysis',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM credit_card_usage_analysis
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> [analyze_card_usage, fraud_detection_analysis]
    [analyze_card_usage, fraud_detection_analysis] >> validation >> finalize

credit_card_analysis()