"""
Sales Territory Analysis DAG
Weekly analysis of sales performance per territory from Snowflake database.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='sales_territory_analysis',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
        'owner': 'analytics_team',
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=['sales', 'territory', 'weekly', 'analytics'],
    description='Weekly sales territory performance analysis',
    max_active_runs=1,
)
def sales_territory_analysis():
    
    # Task 1: Setup target table and infrastructure
    setup_target_table = SQLExecuteQueryOperator(
        task_id='setup_target_table',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Create target table for weekly territory metrics
        CREATE TABLE IF NOT EXISTS weekly_sales_territory_metrics (
            week_start_date DATE,
            territory_id INTEGER,
            territory_name VARCHAR(50),
            territory_group VARCHAR(50),
            total_orders INTEGER,
            total_revenue DECIMAL(19,4),
            avg_revenue_per_order DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (week_start_date, territory_id)
        );
        
        -- Create analysis log table
        CREATE TABLE IF NOT EXISTS territory_analysis_log (
            analysis_date DATE,
            week_start_date DATE,
            territories_processed INTEGER,
            total_revenue DECIMAL(19,4),
            execution_status VARCHAR(20),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Validate source data availability
    @task(task_id='validate_source_data')
    def validate_source_data():
        """Validate source data exists for the analysis period"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Check data availability for last 7 days
        validation_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT 
            COUNT(DISTINCT h.SalesOrderID) as order_count,
            COUNT(DISTINCT h.TerritoryID) as territory_count,
            MIN(h.OrderDate) as min_date,
            MAX(h.OrderDate) as max_date
        FROM SALES_SALESORDERHEADER h
        WHERE h.OrderDate >= CURRENT_DATE - 7
          AND h.OrderDate < CURRENT_DATE;
        """
        
        result = hook.get_first(validation_sql)
        order_count = result[0] if result else 0
        territory_count = result[1] if result else 0
        min_date = result[2] if result else None
        max_date = result[3] if result else None
        
        logging.info(f"Source validation - Orders: {order_count}, Territories: {territory_count}")
        logging.info(f"Date range: {min_date} to {max_date}")
        
        if order_count == 0:
            logging.warning("No orders found in the last 7 days")
        
        return {
            'order_count': order_count,
            'territory_count': territory_count,
            'date_range': f"{min_date} to {max_date}"
        }
    
    # Task 3: Clean existing data for the week
    cleanup_existing_data = SQLExecuteQueryOperator(
        task_id='cleanup_existing_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Calculate week start date (Monday of current week)
        SET week_start = (SELECT DATE_TRUNC('WEEK', CURRENT_DATE));
        
        -- Remove existing data for this week
        DELETE FROM weekly_sales_territory_metrics 
        WHERE week_start_date = $week_start;
        
        -- Log cleanup operation
        INSERT INTO territory_analysis_log (analysis_date, week_start_date, execution_status)
        VALUES (CURRENT_DATE, $week_start, 'CLEANUP_COMPLETE');
        """,
    )
    
    # Task 4: Extract and aggregate territory sales data
    extract_territory_metrics = SQLExecuteQueryOperator(
        task_id='extract_territory_metrics',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Calculate week start date
        SET week_start = (SELECT DATE_TRUNC('WEEK', CURRENT_DATE));
        
        -- Insert weekly territory metrics
        INSERT INTO weekly_sales_territory_metrics (
            week_start_date,
            territory_id,
            territory_name,
            territory_group,
            total_orders,
            total_revenue,
            avg_revenue_per_order
        )
        SELECT 
            $week_start as week_start_date,
            h.TerritoryID as territory_id,
            COALESCE(t.Name, 'Unknown Territory') as territory_name,
            COALESCE(t."Group", 'Unknown Group') as territory_group,
            COUNT(DISTINCT h.SalesOrderID) as total_orders,
            SUM(d.LineTotal) as total_revenue,
            AVG(order_totals.order_total) as avg_revenue_per_order
        FROM dbt_schema.SALES_SALESORDERHEADER h
        INNER JOIN dbt_schema.SALES_SALESORDERDETAIL d 
            ON h.SalesOrderID = d.SalesOrderID
        LEFT JOIN dbt_schema.SALES_SALESTERRITORY t 
            ON h.TerritoryID = t.TerritoryID
        INNER JOIN (
            -- Subquery to calculate total per order
            SELECT 
                h2.SalesOrderID,
                SUM(d2.LineTotal) as order_total
            FROM dbt_schema.SALES_SALESORDERHEADER h2
            INNER JOIN dbt_schema.SALES_SALESORDERDETAIL d2 
                ON h2.SalesOrderID = d2.SalesOrderID
            WHERE h2.OrderDate >= CURRENT_DATE - 7
              AND h2.OrderDate < CURRENT_DATE
              AND h2.Status = 5  -- Completed orders only
            GROUP BY h2.SalesOrderID
        ) order_totals ON h.SalesOrderID = order_totals.SalesOrderID
        WHERE h.OrderDate >= CURRENT_DATE - 7
          AND h.OrderDate < CURRENT_DATE
          AND h.Status = 5  -- Completed orders only
        GROUP BY 
            h.TerritoryID,
            t.Name,
            t."Group"
        HAVING SUM(d.LineTotal) > 0  -- Only territories with positive revenue
        ORDER BY total_revenue DESC;
        """,
    )
    
    # Task 5: Check if processing should continue
    @task(task_id='check_processing_needed')
    def check_processing_needed():
        """Check if there's source data to process"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Check if source data exists for last 7 days
        check_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) as order_count
        FROM SALES_SALESORDERHEADER h
        WHERE h.OrderDate >= CURRENT_DATE - 7
          AND h.OrderDate < CURRENT_DATE
          AND h.Status = 5;
        """
        
        result = hook.get_first(check_sql)
        order_count = result[0] if result else 0
        
        if order_count == 0:
            logging.info("No orders found in last 7 days - marking analysis as complete with no data")
            
            # Log the no-data completion
            log_sql = """
            USE ROLE ACCOUNTADMIN;
            USE DATABASE dbt_db;
            USE SCHEMA dbt_warehouse;
            
            INSERT INTO territory_analysis_log (analysis_date, week_start_date, territories_processed, total_revenue, execution_status)
            VALUES (CURRENT_DATE, DATE_TRUNC('WEEK', CURRENT_DATE), 0, 0, 'NO_DATA_SUCCESS');
            """
            hook.run(log_sql)
            
            return 'skip_processing'
        
        logging.info(f"Found {order_count} orders - proceeding with analysis")
        return 'continue_processing'
    
    # Task 6: Conditional insights generation
    @task(task_id='generate_insights_if_needed')
    def generate_insights_if_needed(processing_status):
        """Generate insights only if data was processed"""
        if processing_status == 'skip_processing':
            logging.info("Skipping insights generation - no data to process")
            return 'skipped'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        insights_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Create territory insights table
        CREATE TABLE IF NOT EXISTS territory_performance_insights (
            week_start_date DATE,
            territory_id INTEGER,
            territory_name VARCHAR(50),
            revenue_rank INTEGER,
            orders_rank INTEGER,
            avg_order_rank INTEGER,
            performance_category VARCHAR(20),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Clear existing insights for this week
        DELETE FROM territory_performance_insights 
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        -- Generate territory performance insights
        INSERT INTO territory_performance_insights (
            week_start_date,
            territory_id,
            territory_name,
            revenue_rank,
            orders_rank,
            avg_order_rank,
            performance_category
        )
        SELECT 
            week_start_date,
            territory_id,
            territory_name,
            ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
            ROW_NUMBER() OVER (ORDER BY total_orders DESC) as orders_rank,
            ROW_NUMBER() OVER (ORDER BY avg_revenue_per_order DESC) as avg_order_rank,
            CASE 
                WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 3 THEN 'TOP_PERFORMER'
                WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) > (SELECT COUNT(*) * 0.8 FROM weekly_sales_territory_metrics WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE)) THEN 'NEEDS_ATTENTION'
                ELSE 'AVERAGE_PERFORMER'
            END as performance_category
        FROM weekly_sales_territory_metrics
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        hook.run(insights_sql)
        logging.info("Territory insights generated successfully")
        return 'completed'
    

    
    # Task 7: Finalize analysis and logging
    finalize_analysis = SQLExecuteQueryOperator(
        task_id='finalize_analysis',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Update analysis log with final results
        INSERT INTO territory_analysis_log (
            analysis_date,
            week_start_date,
            territories_processed,
            total_revenue,
            execution_status
        )
        SELECT 
            CURRENT_DATE,
            DATE_TRUNC('WEEK', CURRENT_DATE),
            COUNT(*),
            SUM(total_revenue),
            'SUCCESS'
        FROM weekly_sales_territory_metrics
        WHERE week_start_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        -- Create summary view for easy access
        CREATE OR REPLACE VIEW v_latest_territory_performance AS
        SELECT 
            m.*,
            i.revenue_rank,
            i.performance_category
        FROM weekly_sales_territory_metrics m
        LEFT JOIN territory_performance_insights i 
            ON m.week_start_date = i.week_start_date 
            AND m.territory_id = i.territory_id
        WHERE m.week_start_date = (
            SELECT MAX(week_start_date) 
            FROM weekly_sales_territory_metrics
        )
        ORDER BY m.total_revenue DESC;
        """,
    )
    
    # Define task dependencies
    source_validation = validate_source_data()
    processing_check = check_processing_needed()
    insights_task = generate_insights_if_needed(processing_check)
    
    setup_target_table >> source_validation >> cleanup_existing_data
    cleanup_existing_data >> processing_check
    processing_check >> extract_territory_metrics >> insights_task >> finalize_analysis

sales_territory_analysis()
