"""
Marketing Campaigns DAG
Weekly marketing campaign performance analysis and customer targeting.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='marketing_campaigns',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'marketing_team',
        'email_on_failure': False,
    },
    tags=['marketing', 'campaigns', 'weekly', 'targeting'],
    description='Weekly marketing campaign analysis and customer targeting',
)
def marketing_campaigns():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS campaign_performance (
            analysis_date DATE,
            campaign_id VARCHAR(50),
            campaign_name VARCHAR(100),
            target_segment VARCHAR(50),
            customers_targeted INTEGER,
            customers_responded INTEGER,
            response_rate DECIMAL(5,2),
            revenue_generated DECIMAL(19,4),
            roi DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS customer_targeting (
            analysis_date DATE,
            customer_id INTEGER,
            target_segment VARCHAR(50),
            campaign_priority INTEGER,
            recommended_products VARCHAR(500),
            expected_response_rate DECIMAL(5,2),
            potential_value DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS marketing_insights (
            analysis_date DATE,
            insight_type VARCHAR(50),
            insight_description TEXT,
            metric_value DECIMAL(19,4),
            recommended_action TEXT,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if customer and sales data exists for marketing analysis"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT 
            COUNT(DISTINCT CustomerID) as customers,
            COUNT(DISTINCT SalesOrderID) as orders
        FROM SALES_SALESORDERHEADER 
        WHERE Status = 5 AND CustomerID IS NOT NULL;
        """
        
        result = hook.get_first(sql)
        customers = result[0] if result else 0
        orders = result[1] if result else 0
        
        logging.info(f"Found {customers} customers with {orders} orders")
        
        if customers < 10:
            logging.info("Insufficient customer data - completing successfully")
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
        
        DELETE FROM campaign_performance 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_targeting 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM marketing_insights 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Analyze campaign performance
    analyze_campaign_performance = SQLExecuteQueryOperator(
        task_id='analyze_campaign_performance',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO campaign_performance (
            analysis_date,
            campaign_id,
            campaign_name,
            target_segment,
            customers_targeted,
            customers_responded,
            response_rate,
            revenue_generated,
            roi
        )
        WITH customer_segments AS (
            SELECT 
                CustomerID,
                CASE 
                    WHEN total_spent >= 10000 THEN 'HIGH_VALUE'
                    WHEN total_spent >= 5000 THEN 'MEDIUM_VALUE'
                    WHEN total_spent >= 1000 THEN 'LOW_VALUE'
                    ELSE 'NEW_CUSTOMER'
                END as segment,
                total_spent,
                total_orders
            FROM (
                SELECT 
                    CustomerID,
                    SUM(TotalDue) as total_spent,
                    COUNT(DISTINCT SalesOrderID) as total_orders
                FROM dbt_schema.SALES_SALESORDERHEADER
                WHERE Status = 5
                GROUP BY CustomerID
            ) customer_stats
        ),
        simulated_campaigns AS (
            SELECT 
                'CAMP_001' as campaign_id,
                'Premium Product Launch' as campaign_name,
                'HIGH_VALUE' as target_segment,
                COUNT(*) as customers_targeted,
                -- Simulate response rates based on segment
                ROUND(COUNT(*) * 0.15) as customers_responded,
                SUM(total_spent * 0.1) as revenue_generated -- Assume 10% incremental revenue
            FROM customer_segments
            WHERE segment = 'HIGH_VALUE'
            
            UNION ALL
            
            SELECT 
                'CAMP_002',
                'Loyalty Rewards Program',
                'MEDIUM_VALUE',
                COUNT(*),
                ROUND(COUNT(*) * 0.12),
                SUM(total_spent * 0.08)
            FROM customer_segments
            WHERE segment = 'MEDIUM_VALUE'
            
            UNION ALL
            
            SELECT 
                'CAMP_003',
                'Welcome Series',
                'NEW_CUSTOMER',
                COUNT(*),
                ROUND(COUNT(*) * 0.08),
                SUM(total_spent * 0.05)
            FROM customer_segments
            WHERE segment = 'NEW_CUSTOMER'
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            campaign_id,
            campaign_name,
            target_segment,
            customers_targeted,
            customers_responded,
            CASE WHEN customers_targeted > 0 THEN ROUND((customers_responded * 100.0 / customers_targeted), 2) ELSE 0 END as response_rate,
            revenue_generated,
            CASE WHEN customers_targeted > 0 THEN ROUND((revenue_generated / (customers_targeted * 50)), 2) ELSE 0 END as roi -- Assume $50 cost per customer
        FROM simulated_campaigns
        WHERE customers_targeted > 0;
        """,
    )
    
    # Task 5: Generate customer targeting recommendations
    generate_customer_targeting = SQLExecuteQueryOperator(
        task_id='generate_customer_targeting',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_targeting (
            analysis_date,
            customer_id,
            target_segment,
            campaign_priority,
            recommended_products,
            expected_response_rate,
            potential_value
        )
        WITH customer_analysis AS (
            SELECT 
                c.CustomerID,
                SUM(h.TotalDue) as total_spent,
                COUNT(DISTINCT h.SalesOrderID) as total_orders,
                MAX(h.OrderDate) as last_order_date,
                DATEDIFF(day, MAX(h.OrderDate), CURRENT_DATE) as days_since_last_order,
                COUNT(DISTINCT d.ProductID) as unique_products_bought
            FROM dbt_schema.SALES_CUSTOMER c
            LEFT JOIN dbt_schema.SALES_SALESORDERHEADER h ON c.CustomerID = h.CustomerID
            LEFT JOIN dbt_schema.SALES_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
            WHERE h.Status = 5 OR h.Status IS NULL
            GROUP BY c.CustomerID
        ),
        customer_segments AS (
            SELECT 
                CustomerID,
                total_spent,
                total_orders,
                days_since_last_order,
                unique_products_bought,
                CASE 
                    WHEN total_spent >= 10000 AND days_since_last_order <= 30 THEN 'VIP_ACTIVE'
                    WHEN total_spent >= 10000 AND days_since_last_order > 30 THEN 'VIP_AT_RISK'
                    WHEN total_spent >= 5000 AND days_since_last_order <= 60 THEN 'LOYAL_ACTIVE'
                    WHEN total_spent >= 5000 AND days_since_last_order > 60 THEN 'LOYAL_AT_RISK'
                    WHEN total_orders >= 3 AND days_since_last_order <= 90 THEN 'REGULAR_ACTIVE'
                    WHEN total_orders >= 3 AND days_since_last_order > 90 THEN 'REGULAR_AT_RISK'
                    WHEN total_orders >= 1 AND days_since_last_order <= 180 THEN 'OCCASIONAL'
                    WHEN total_orders >= 1 AND days_since_last_order > 180 THEN 'DORMANT'
                    ELSE 'NEW_PROSPECT'
                END as target_segment
            FROM customer_analysis
        ),
        targeting_logic AS (
            SELECT 
                CustomerID,
                target_segment,
                CASE 
                    WHEN target_segment IN ('VIP_ACTIVE', 'VIP_AT_RISK') THEN 1
                    WHEN target_segment IN ('LOYAL_ACTIVE', 'LOYAL_AT_RISK') THEN 2
                    WHEN target_segment IN ('REGULAR_ACTIVE', 'REGULAR_AT_RISK') THEN 3
                    WHEN target_segment = 'OCCASIONAL' THEN 4
                    WHEN target_segment = 'DORMANT' THEN 5
                    ELSE 6
                END as campaign_priority,
                CASE 
                    WHEN target_segment LIKE '%VIP%' THEN 'Premium products, exclusive offers, early access'
                    WHEN target_segment LIKE '%LOYAL%' THEN 'Loyalty rewards, cross-sell opportunities'
                    WHEN target_segment LIKE '%REGULAR%' THEN 'Personalized recommendations, bundle offers'
                    WHEN target_segment = 'OCCASIONAL' THEN 'Engagement campaigns, special discounts'
                    WHEN target_segment = 'DORMANT' THEN 'Win-back campaigns, significant incentives'
                    ELSE 'Welcome series, product education'
                END as recommended_products,
                CASE 
                    WHEN target_segment = 'VIP_ACTIVE' THEN 25.0
                    WHEN target_segment = 'VIP_AT_RISK' THEN 20.0
                    WHEN target_segment = 'LOYAL_ACTIVE' THEN 18.0
                    WHEN target_segment = 'LOYAL_AT_RISK' THEN 15.0
                    WHEN target_segment = 'REGULAR_ACTIVE' THEN 12.0
                    WHEN target_segment = 'REGULAR_AT_RISK' THEN 10.0
                    WHEN target_segment = 'OCCASIONAL' THEN 8.0
                    WHEN target_segment = 'DORMANT' THEN 5.0
                    ELSE 3.0
                END as expected_response_rate,
                total_spent * 0.15 as potential_value -- Assume 15% incremental value
            FROM customer_segments cs
            JOIN customer_analysis ca ON cs.CustomerID = ca.CustomerID
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            CustomerID,
            target_segment,
            campaign_priority,
            recommended_products,
            expected_response_rate,
            potential_value
        FROM targeting_logic
        WHERE campaign_priority <= 4; -- Focus on top 4 priority segments
        """,
    )
    
    # Task 6: Generate marketing insights
    generate_marketing_insights = SQLExecuteQueryOperator(
        task_id='generate_marketing_insights',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO marketing_insights (
            analysis_date,
            insight_type,
            insight_description,
            metric_value,
            recommended_action
        )
        -- Campaign performance insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'CAMPAIGN_PERFORMANCE',
            'Best performing campaign by ROI: ' || campaign_name,
            roi,
            'Scale up this campaign and apply learnings to other segments'
        FROM campaign_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY roi DESC
        LIMIT 1
        
        UNION ALL
        
        -- Response rate insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'RESPONSE_RATE',
            'Average response rate across all campaigns',
            AVG(response_rate),
            'Focus on improving response rates through better targeting and messaging'
        FROM campaign_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        
        UNION ALL
        
        -- Customer targeting insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'CUSTOMER_TARGETING',
            'High-priority customers available for targeting',
            COUNT(*),
            'Prioritize campaigns for high-value and at-risk customer segments'
        FROM customer_targeting
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND campaign_priority <= 2
        
        UNION ALL
        
        -- Revenue potential insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'REVENUE_POTENTIAL',
            'Total potential revenue from targeted campaigns',
            SUM(potential_value),
            'Execute targeted campaigns to capture identified revenue opportunities'
        FROM customer_targeting
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 7: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate marketing campaign analysis results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get campaign performance summary
        campaign_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            campaign_name,
            customers_targeted,
            response_rate,
            roi
        FROM campaign_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY roi DESC;
        """
        
        campaign_results = hook.get_records(campaign_sql)
        
        # Get targeting summary
        targeting_sql = """
        SELECT 
            target_segment,
            COUNT(*) as customer_count,
            AVG(expected_response_rate) as avg_response_rate,
            SUM(potential_value) as total_potential
        FROM customer_targeting
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY target_segment
        ORDER BY total_potential DESC;
        """
        
        targeting_results = hook.get_records(targeting_sql)
        
        logging.info("=== MARKETING CAMPAIGN ANALYSIS ===")
        logging.info("Campaign Performance:")
        for name, targeted, response_rate, roi in campaign_results:
            logging.info(f"  {name}: {targeted} targeted, {response_rate}% response, {roi}% ROI")
        
        logging.info("Customer Targeting:")
        for segment, count, avg_response, potential in targeting_results:
            logging.info(f"  {segment}: {count} customers, {avg_response:.1f}% expected response, ${potential:,.2f} potential")
        
        # Get key insights
        insights_sql = """
        SELECT insight_type, insight_description, metric_value
        FROM marketing_insights
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY insight_type;
        """
        
        insights_results = hook.get_records(insights_sql)
        logging.info("Key Insights:")
        for insight_type, description, value in insights_results:
            logging.info(f"  {insight_type}: {description} ({value})")
        
        return 'success'
    
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
            'marketing_campaigns',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM customer_targeting
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> [analyze_campaign_performance, generate_customer_targeting]
    [analyze_campaign_performance, generate_customer_targeting] >> generate_marketing_insights
    generate_marketing_insights >> validation >> finalize

marketing_campaigns()