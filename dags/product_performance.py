"""
Product Performance Analysis DAG
Weekly analysis of product sales performance, profitability, and trends.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='product_performance',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'product_team',
        'email_on_failure': False,
    },
    tags=['product', 'performance', 'weekly', 'profitability'],
    description='Weekly product performance and profitability analysis',
)
def product_performance():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS weekly_product_performance (
            analysis_date DATE,
            product_id INTEGER,
            product_name VARCHAR(100),
            category VARCHAR(50),
            subcategory VARCHAR(50),
            units_sold INTEGER,
            revenue DECIMAL(19,4),
            cost_of_goods DECIMAL(19,4),
            gross_profit DECIMAL(19,4),
            profit_margin DECIMAL(5,2),
            avg_selling_price DECIMAL(19,4),
            performance_rank INTEGER,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS product_trends (
            analysis_date DATE,
            product_id INTEGER,
            trend_period VARCHAR(20),
            sales_trend VARCHAR(20),
            revenue_growth DECIMAL(5,2),
            volume_growth DECIMAL(5,2),
            trend_score DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS product_insights (
            analysis_date DATE,
            insight_category VARCHAR(50),
            product_id INTEGER,
            product_name VARCHAR(100),
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
        """Check if product sales data exists"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(DISTINCT d.ProductID) 
        FROM SALES_SALESORDERDETAIL d
        JOIN SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
        WHERE h.OrderDate >= CURRENT_DATE - 7 AND h.Status = 5;
        """
        
        result = hook.get_first(sql)
        product_count = result[0] if result else 0
        
        logging.info(f"Found {product_count} products with sales in last 7 days")
        
        if product_count < 5:
            logging.info("Insufficient product sales data - completing successfully")
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
        
        DELETE FROM weekly_product_performance 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM product_trends 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM product_insights 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Analyze weekly product performance
    analyze_product_performance = SQLExecuteQueryOperator(
        task_id='analyze_product_performance',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO weekly_product_performance (
            analysis_date,
            product_id,
            product_name,
            category,
            subcategory,
            units_sold,
            revenue,
            cost_of_goods,
            gross_profit,
            profit_margin,
            avg_selling_price,
            performance_rank
        )
        WITH product_sales AS (
            SELECT 
                d.ProductID,
                p.Name as product_name,
                COALESCE(pc.Name, 'Unknown') as category,
                COALESCE(ps.Name, 'Unknown') as subcategory,
                SUM(d.OrderQty) as units_sold,
                SUM(d.LineTotal) as revenue,
                SUM(d.OrderQty * p.StandardCost) as cost_of_goods,
                AVG(d.UnitPrice) as avg_selling_price
            FROM dbt_schema.SALES_SALESORDERDETAIL d
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
            JOIN dbt_schema.PRODUCTION_PRODUCT p ON d.ProductID = p.ProductID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTSUBCATEGORY ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTCATEGORY pc ON ps.ProductCategoryID = pc.ProductCategoryID
            WHERE h.OrderDate >= CURRENT_DATE - 7
              AND h.OrderDate < CURRENT_DATE
              AND h.Status = 5
            GROUP BY d.ProductID, p.Name, pc.Name, ps.Name
        ),
        performance_metrics AS (
            SELECT 
                *,
                revenue - cost_of_goods as gross_profit,
                CASE WHEN revenue > 0 THEN ROUND(((revenue - cost_of_goods) / revenue) * 100, 2) ELSE 0 END as profit_margin,
                ROW_NUMBER() OVER (ORDER BY revenue DESC) as performance_rank
            FROM product_sales
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            ProductID,
            product_name,
            category,
            subcategory,
            units_sold,
            revenue,
            cost_of_goods,
            gross_profit,
            profit_margin,
            avg_selling_price,
            performance_rank
        FROM performance_metrics
        WHERE revenue > 0;
        """,
    )
    
    # Task 5: Analyze product trends
    analyze_product_trends = SQLExecuteQueryOperator(
        task_id='analyze_product_trends',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO product_trends (
            analysis_date,
            product_id,
            trend_period,
            sales_trend,
            revenue_growth,
            volume_growth,
            trend_score
        )
        WITH current_week AS (
            SELECT 
                d.ProductID,
                SUM(d.LineTotal) as current_revenue,
                SUM(d.OrderQty) as current_volume
            FROM dbt_schema.SALES_SALESORDERDETAIL d
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
            WHERE h.OrderDate >= CURRENT_DATE - 7
              AND h.OrderDate < CURRENT_DATE
              AND h.Status = 5
            GROUP BY d.ProductID
        ),
        previous_week AS (
            SELECT 
                d.ProductID,
                SUM(d.LineTotal) as previous_revenue,
                SUM(d.OrderQty) as previous_volume
            FROM dbt_schema.SALES_SALESORDERDETAIL d
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
            WHERE h.OrderDate >= CURRENT_DATE - 14
              AND h.OrderDate < CURRENT_DATE - 7
              AND h.Status = 5
            GROUP BY d.ProductID
        ),
        trend_analysis AS (
            SELECT 
                COALESCE(cw.ProductID, pw.ProductID) as ProductID,
                COALESCE(cw.current_revenue, 0) as current_revenue,
                COALESCE(pw.previous_revenue, 0) as previous_revenue,
                COALESCE(cw.current_volume, 0) as current_volume,
                COALESCE(pw.previous_volume, 0) as previous_volume,
                CASE 
                    WHEN COALESCE(pw.previous_revenue, 0) > 0 
                    THEN ROUND(((COALESCE(cw.current_revenue, 0) - pw.previous_revenue) / pw.previous_revenue) * 100, 2)
                    ELSE 0
                END as revenue_growth,
                CASE 
                    WHEN COALESCE(pw.previous_volume, 0) > 0 
                    THEN ROUND(((COALESCE(cw.current_volume, 0) - pw.previous_volume) / pw.previous_volume) * 100, 2)
                    ELSE 0
                END as volume_growth
            FROM current_week cw
            FULL OUTER JOIN previous_week pw ON cw.ProductID = pw.ProductID
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            ProductID,
            'WEEK_OVER_WEEK',
            CASE 
                WHEN revenue_growth > 20 THEN 'STRONG_GROWTH'
                WHEN revenue_growth > 5 THEN 'MODERATE_GROWTH'
                WHEN revenue_growth > -5 THEN 'STABLE'
                WHEN revenue_growth > -20 THEN 'MODERATE_DECLINE'
                ELSE 'STRONG_DECLINE'
            END as sales_trend,
            revenue_growth,
            volume_growth,
            -- Trend score combining revenue and volume growth
            ROUND((revenue_growth * 0.7) + (volume_growth * 0.3), 2) as trend_score
        FROM trend_analysis
        WHERE current_revenue > 0 OR previous_revenue > 0;
        """,
    )
    
    # Task 6: Generate product insights
    generate_product_insights = SQLExecuteQueryOperator(
        task_id='generate_product_insights',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO product_insights (
            analysis_date,
            insight_category,
            product_id,
            product_name,
            insight_description,
            metric_value,
            recommended_action
        )
        -- Top performing products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'TOP_PERFORMER',
            product_id,
            product_name,
            'Top revenue generating product this week',
            revenue,
            'Maintain inventory levels and consider expanding marketing for this product'
        FROM weekly_product_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY revenue DESC
        LIMIT 3
        
        UNION ALL
        
        -- Most profitable products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'HIGH_MARGIN',
            product_id,
            product_name,
            'Highest profit margin product: ' || profit_margin || '%',
            profit_margin,
            'Focus on promoting high-margin products to improve overall profitability'
        FROM weekly_product_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND profit_margin > 0
        ORDER BY profit_margin DESC
        LIMIT 3
        
        UNION ALL
        
        -- Trending products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'TRENDING_UP',
            pt.product_id,
            pp.product_name,
            'Strong growth trend: ' || pt.revenue_growth || '% revenue growth',
            pt.trend_score,
            'Capitalize on momentum with increased marketing and inventory investment'
        FROM product_trends pt
        JOIN weekly_product_performance pp ON pt.product_id = pp.product_id 
            AND pt.analysis_date = pp.analysis_date
        WHERE pt.analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND pt.sales_trend = 'STRONG_GROWTH'
        ORDER BY pt.trend_score DESC
        LIMIT 3
        
        UNION ALL
        
        -- Declining products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'DECLINING',
            pt.product_id,
            pp.product_name,
            'Declining performance: ' || pt.revenue_growth || '% revenue decline',
            pt.trend_score,
            'Investigate causes and consider promotional campaigns or product improvements'
        FROM product_trends pt
        JOIN weekly_product_performance pp ON pt.product_id = pp.product_id 
            AND pt.analysis_date = pp.analysis_date
        WHERE pt.analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND pt.sales_trend IN ('MODERATE_DECLINE', 'STRONG_DECLINE')
        ORDER BY pt.trend_score ASC
        LIMIT 3
        
        UNION ALL
        
        -- Category performance
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'CATEGORY_LEADER',
            product_id,
            product_name,
            'Best performing product in ' || category || ' category',
            revenue,
            'Use as flagship product for category marketing and cross-selling'
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) as category_rank
            FROM weekly_product_performance
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ) ranked
        WHERE category_rank = 1
          AND category != 'Unknown';
        """,
    )
    
    # Task 7: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate product performance analysis results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get performance summary
        performance_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as products_analyzed,
            SUM(revenue) as total_revenue,
            AVG(profit_margin) as avg_profit_margin,
            MAX(revenue) as top_product_revenue
        FROM weekly_product_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        performance_result = hook.get_first(performance_sql)
        if performance_result:
            products, revenue, margin, top_revenue = performance_result
            logging.info(f"Performance Summary: {products} products, ${revenue:,.2f} total revenue")
            logging.info(f"Average margin: {margin:.2f}%, Top product: ${top_revenue:,.2f}")
        
        # Get trend summary
        trend_sql = """
        SELECT 
            sales_trend,
            COUNT(*) as product_count,
            AVG(revenue_growth) as avg_growth
        FROM product_trends
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY sales_trend
        ORDER BY avg_growth DESC;
        """
        
        trend_results = hook.get_records(trend_sql)
        logging.info("Trend Analysis:")
        for trend, count, growth in trend_results:
            logging.info(f"  {trend}: {count} products, {growth:.1f}% avg growth")
        
        # Get top insights
        insights_sql = """
        SELECT 
            insight_category,
            COUNT(*) as insight_count
        FROM product_insights
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY insight_category
        ORDER BY insight_count DESC;
        """
        
        insights_results = hook.get_records(insights_sql)
        logging.info("Generated Insights:")
        for category, count in insights_results:
            logging.info(f"  {category}: {count} insights")
        
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
            'product_performance',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM weekly_product_performance
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> [analyze_product_performance, analyze_product_trends]
    [analyze_product_performance, analyze_product_trends] >> generate_product_insights
    generate_product_insights >> validation >> finalize

product_performance()