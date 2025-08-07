"""
Customer Retention Analysis DAG
Advanced customer retention analysis with cohort analysis, churn prediction, and retention metrics.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='customer_retention',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['customer', 'retention', 'cohort', 'churn', 'weekly'],
    description='Advanced customer retention analysis with cohort and churn prediction',
)
def customer_retention():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Customer cohort analysis table
        CREATE TABLE IF NOT EXISTS customer_cohort_analysis (
            analysis_date DATE,
            cohort_month DATE,
            period_number INTEGER,
            customers_count INTEGER,
            retention_rate DECIMAL(5,2),
            revenue_per_customer DECIMAL(19,4),
            cumulative_revenue DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Customer churn prediction table
        CREATE TABLE IF NOT EXISTS customer_churn_prediction (
            analysis_date DATE,
            customer_id INTEGER,
            days_since_last_order INTEGER,
            total_orders INTEGER,
            avg_days_between_orders DECIMAL(10,2),
            total_spent DECIMAL(19,4),
            avg_order_value DECIMAL(19,4),
            churn_risk_score DECIMAL(5,2),
            churn_risk_category VARCHAR(20),
            predicted_churn_date DATE,
            retention_probability DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Customer lifecycle stages table
        CREATE TABLE IF NOT EXISTS customer_lifecycle_stages (
            analysis_date DATE,
            customer_id INTEGER,
            first_order_date DATE,
            last_order_date DATE,
            customer_age_days INTEGER,
            lifecycle_stage VARCHAR(20),
            stage_description VARCHAR(100),
            recommended_action VARCHAR(200),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if sufficient customer order data exists for retention analysis"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT 
            COUNT(DISTINCT CustomerID) as unique_customers,
            COUNT(DISTINCT DATE_TRUNC('MONTH', OrderDate)) as months_with_data
        FROM SALES_SALESORDERHEADER 
        WHERE Status = 5 AND CustomerID IS NOT NULL;
        """
        
        result = hook.get_first(sql)
        customer_count = result[0] if result else 0
        months_count = result[1] if result and len(result) > 1 else 0
        
        logging.info(f"Found {customer_count} customers across {months_count} months")
        
        if customer_count < 10 or months_count < 2:
            logging.info("Insufficient data for retention analysis - completing successfully")
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
        
        DELETE FROM customer_cohort_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_churn_prediction 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_lifecycle_stages 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Perform cohort analysis
    perform_cohort_analysis = SQLExecuteQueryOperator(
        task_id='perform_cohort_analysis',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_cohort_analysis (
            analysis_date,
            cohort_month,
            period_number,
            customers_count,
            retention_rate,
            revenue_per_customer,
            cumulative_revenue
        )
        WITH customer_cohorts AS (
            SELECT 
                CustomerID,
                DATE_TRUNC('MONTH', MIN(OrderDate)) as cohort_month,
                MIN(OrderDate) as first_order_date
            FROM dbt_schema.SALES_SALESORDERHEADER
            WHERE Status = 5 AND CustomerID IS NOT NULL
            GROUP BY CustomerID
        ),
        customer_orders AS (
            SELECT 
                h.CustomerID,
                c.cohort_month,
                DATE_TRUNC('MONTH', h.OrderDate) as order_month,
                DATEDIFF(month, c.cohort_month, DATE_TRUNC('MONTH', h.OrderDate)) as period_number,
                SUM(h.TotalDue) as monthly_revenue
            FROM dbt_schema.SALES_SALESORDERHEADER h
            JOIN customer_cohorts c ON h.CustomerID = c.CustomerID
            WHERE h.Status = 5
            GROUP BY h.CustomerID, c.cohort_month, DATE_TRUNC('MONTH', h.OrderDate)
        ),
        cohort_data AS (
            SELECT 
                cohort_month,
                period_number,
                COUNT(DISTINCT CustomerID) as customers_count,
                SUM(monthly_revenue) as total_revenue
            FROM customer_orders
            GROUP BY cohort_month, period_number
        ),
        cohort_sizes AS (
            SELECT 
                cohort_month,
                customers_count as cohort_size
            FROM cohort_data
            WHERE period_number = 0
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            cd.cohort_month,
            cd.period_number,
            cd.customers_count,
            ROUND((cd.customers_count * 100.0 / cs.cohort_size), 2) as retention_rate,
            ROUND(cd.total_revenue / cd.customers_count, 2) as revenue_per_customer,
            SUM(cd.total_revenue) OVER (
                PARTITION BY cd.cohort_month 
                ORDER BY cd.period_number 
                ROWS UNBOUNDED PRECEDING
            ) as cumulative_revenue
        FROM cohort_data cd
        JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
        WHERE cd.period_number <= 12  -- Limit to 12 months for performance
        ORDER BY cd.cohort_month, cd.period_number;
        """,
    )
    
    # Task 5: Calculate churn prediction
    calculate_churn_prediction = SQLExecuteQueryOperator(
        task_id='calculate_churn_prediction',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_churn_prediction (
            analysis_date,
            customer_id,
            days_since_last_order,
            total_orders,
            avg_days_between_orders,
            total_spent,
            avg_order_value,
            churn_risk_score,
            churn_risk_category,
            predicted_churn_date,
            retention_probability
        )
        WITH customer_behavior AS (
            SELECT 
                CustomerID,
                COUNT(DISTINCT SalesOrderID) as total_orders,
                SUM(TotalDue) as total_spent,
                AVG(TotalDue) as avg_order_value,
                MIN(OrderDate) as first_order_date,
                MAX(OrderDate) as last_order_date,
                DATEDIFF(day, MAX(OrderDate), CURRENT_DATE) as days_since_last_order,
                CASE 
                    WHEN COUNT(DISTINCT SalesOrderID) > 1 
                    THEN DATEDIFF(day, MIN(OrderDate), MAX(OrderDate)) / (COUNT(DISTINCT SalesOrderID) - 1)
                    ELSE NULL
                END as avg_days_between_orders
            FROM dbt_schema.SALES_SALESORDERHEADER
            WHERE Status = 5 AND CustomerID IS NOT NULL
            GROUP BY CustomerID
        ),
        churn_scoring AS (
            SELECT 
                *,
                -- Churn risk score based on multiple factors (0-100)
                LEAST(100, GREATEST(0,
                    (days_since_last_order * 0.4) +  -- Recency weight
                    (CASE WHEN total_orders = 1 THEN 30 ELSE 0 END) +  -- One-time buyer penalty
                    (CASE WHEN avg_days_between_orders > 90 THEN 20 ELSE 0 END) +  -- Infrequent buyer penalty
                    (CASE WHEN avg_order_value < 100 THEN 15 ELSE 0 END)  -- Low value penalty
                )) as churn_risk_score
            FROM customer_behavior
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CustomerID,
            days_since_last_order,
            total_orders,
            avg_days_between_orders,
            total_spent,
            avg_order_value,
            churn_risk_score,
            CASE 
                WHEN churn_risk_score >= 80 THEN 'CRITICAL'
                WHEN churn_risk_score >= 60 THEN 'HIGH'
                WHEN churn_risk_score >= 40 THEN 'MEDIUM'
                WHEN churn_risk_score >= 20 THEN 'LOW'
                ELSE 'MINIMAL'
            END as churn_risk_category,
            CASE 
                WHEN avg_days_between_orders IS NOT NULL 
                THEN DATEADD(day, avg_days_between_orders * 2, last_order_date)
                ELSE DATEADD(day, 90, last_order_date)
            END as predicted_churn_date,
            ROUND(100 - churn_risk_score, 2) as retention_probability
        FROM churn_scoring
        WHERE days_since_last_order >= 0;
        """,
    )
    
    # Task 6: Analyze customer lifecycle stages
    analyze_lifecycle_stages = SQLExecuteQueryOperator(
        task_id='analyze_lifecycle_stages',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_lifecycle_stages (
            analysis_date,
            customer_id,
            first_order_date,
            last_order_date,
            customer_age_days,
            lifecycle_stage,
            stage_description,
            recommended_action
        )
        WITH customer_lifecycle AS (
            SELECT 
                CustomerID,
                MIN(OrderDate) as first_order_date,
                MAX(OrderDate) as last_order_date,
                COUNT(DISTINCT SalesOrderID) as total_orders,
                SUM(TotalDue) as total_spent,
                DATEDIFF(day, MIN(OrderDate), CURRENT_DATE) as customer_age_days,
                DATEDIFF(day, MAX(OrderDate), CURRENT_DATE) as days_since_last_order
            FROM dbt_schema.SALES_SALESORDERHEADER
            WHERE Status = 5 AND CustomerID IS NOT NULL
            GROUP BY CustomerID
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CustomerID,
            first_order_date,
            last_order_date,
            customer_age_days,
            CASE 
                WHEN customer_age_days <= 30 AND total_orders = 1 THEN 'NEW'
                WHEN customer_age_days <= 90 AND total_orders >= 2 THEN 'DEVELOPING'
                WHEN days_since_last_order <= 30 AND total_orders >= 3 THEN 'ACTIVE'
                WHEN days_since_last_order <= 90 AND total_orders >= 5 THEN 'LOYAL'
                WHEN days_since_last_order <= 180 THEN 'AT_RISK'
                WHEN days_since_last_order <= 365 THEN 'DORMANT'
                ELSE 'LOST'
            END as lifecycle_stage,
            CASE 
                WHEN customer_age_days <= 30 AND total_orders = 1 THEN 'Recently acquired customer, single purchase'
                WHEN customer_age_days <= 90 AND total_orders >= 2 THEN 'Growing relationship, multiple purchases'
                WHEN days_since_last_order <= 30 AND total_orders >= 3 THEN 'Regular active customer'
                WHEN days_since_last_order <= 90 AND total_orders >= 5 THEN 'Highly engaged loyal customer'
                WHEN days_since_last_order <= 180 THEN 'Customer showing signs of disengagement'
                WHEN days_since_last_order <= 365 THEN 'Inactive customer, needs reactivation'
                ELSE 'Customer likely churned, win-back required'
            END as stage_description,
            CASE 
                WHEN customer_age_days <= 30 AND total_orders = 1 THEN 'Send welcome series and product recommendations'
                WHEN customer_age_days <= 90 AND total_orders >= 2 THEN 'Nurture with personalized offers and content'
                WHEN days_since_last_order <= 30 AND total_orders >= 3 THEN 'Maintain engagement with loyalty programs'
                WHEN days_since_last_order <= 90 AND total_orders >= 5 THEN 'Offer VIP treatment and exclusive benefits'
                WHEN days_since_last_order <= 180 THEN 'Send re-engagement campaigns and special offers'
                WHEN days_since_last_order <= 365 THEN 'Launch reactivation campaign with incentives'
                ELSE 'Execute win-back campaign with significant discounts'
            END as recommended_action
        FROM customer_lifecycle;
        """,
    )
    
    # Task 7: Validate retention analysis results
    @task(task_id='validate_retention_results')
    def validate_retention_results():
        """Validate the retention analysis results and log key insights"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Validate cohort analysis
        cohort_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(DISTINCT cohort_month) as cohorts_analyzed,
            AVG(retention_rate) as avg_retention_rate,
            MAX(retention_rate) as max_retention_rate,
            MIN(retention_rate) as min_retention_rate
        FROM customer_cohort_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        cohort_result = hook.get_first(cohort_sql)
        if cohort_result:
            logging.info(f"Cohort Analysis: {cohort_result[0]} cohorts, Avg retention: {cohort_result[1]:.2f}%")
        
        # Validate churn prediction
        churn_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as customers_analyzed,
            AVG(churn_risk_score) as avg_churn_risk,
            COUNT(CASE WHEN churn_risk_category = 'CRITICAL' THEN 1 END) as critical_risk_customers,
            COUNT(CASE WHEN churn_risk_category = 'HIGH' THEN 1 END) as high_risk_customers
        FROM customer_churn_prediction 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        churn_result = hook.get_first(churn_sql)
        if churn_result:
            logging.info(f"Churn Analysis: {churn_result[0]} customers, {churn_result[2]} critical risk, {churn_result[3]} high risk")
        
        # Validate lifecycle stages
        lifecycle_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            lifecycle_stage,
            COUNT(*) as customer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM customer_lifecycle_stages 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY lifecycle_stage
        ORDER BY customer_count DESC;
        """
        
        lifecycle_results = hook.get_records(lifecycle_sql)
        if lifecycle_results:
            logging.info("Customer Lifecycle Distribution:")
            for stage, count, pct in lifecycle_results:
                logging.info(f"  {stage}: {count} customers ({pct}%)")
        
        return "Retention analysis validation completed successfully"
    
    # Define task dependencies
    data_check = check_data_exists()
    
    # Conditional execution based on data availability
    create_target_tables >> data_check
    data_check >> cleanup_data
    cleanup_data >> [perform_cohort_analysis, calculate_churn_prediction, analyze_lifecycle_stages]
    [perform_cohort_analysis, calculate_churn_prediction, analyze_lifecycle_stages] >> validate_retention_results()

customer_retention()