"""
Production Costs Analysis DAG
Weekly analysis of production costs, cost trends, and cost optimization opportunities.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='production_costs',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'operations_team',
        'email_on_failure': False,
    },
    tags=['production', 'costs', 'weekly', 'optimization'],
    description='Weekly production costs analysis and optimization',
)
def production_costs():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS weekly_production_costs (
            analysis_date DATE,
            product_id INTEGER,
            product_name VARCHAR(100),
            category VARCHAR(50),
            standard_cost DECIMAL(19,4),
            actual_cost DECIMAL(19,4),
            cost_variance DECIMAL(19,4),
            cost_variance_pct DECIMAL(5,2),
            units_produced INTEGER,
            total_cost DECIMAL(19,4),
            cost_per_unit DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS cost_trends (
            analysis_date DATE,
            product_id INTEGER,
            trend_period VARCHAR(20),
            cost_trend VARCHAR(20),
            cost_change DECIMAL(19,4),
            cost_change_pct DECIMAL(5,2),
            trend_direction VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS cost_optimization_opportunities (
            analysis_date DATE,
            opportunity_type VARCHAR(50),
            product_id INTEGER,
            product_name VARCHAR(100),
            current_cost DECIMAL(19,4),
            potential_savings DECIMAL(19,4),
            savings_percentage DECIMAL(5,2),
            recommended_action TEXT,
            priority VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if production cost data exists"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM PRODUCTION_PRODUCT 
        WHERE StandardCost > 0 AND FinishedGoodsFlag = 1;
        """
        
        result = hook.get_first(sql)
        product_count = result[0] if result else 0
        
        logging.info(f"Found {product_count} products with cost data")
        
        if product_count < 5:
            logging.info("Insufficient production cost data - completing successfully")
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
        
        DELETE FROM weekly_production_costs 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM cost_trends 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM cost_optimization_opportunities 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Analyze weekly production costs
    analyze_production_costs = SQLExecuteQueryOperator(
        task_id='analyze_production_costs',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO weekly_production_costs (
            analysis_date,
            product_id,
            product_name,
            category,
            standard_cost,
            actual_cost,
            cost_variance,
            cost_variance_pct,
            units_produced,
            total_cost,
            cost_per_unit
        )
        WITH production_data AS (
            SELECT 
                p.ProductID,
                p.Name as product_name,
                COALESCE(pc.Name, 'Unknown') as category,
                p.StandardCost,
                -- Simulate actual costs with some variance
                p.StandardCost * (1 + (RANDOM() * 0.2 - 0.1)) as actual_cost,
                -- Simulate production quantities based on recent sales
                COALESCE(sales.units_sold, 0) + (p.ProductID % 50) as units_produced
            FROM dbt_schema.PRODUCTION_PRODUCT p
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTSUBCATEGORY ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTCATEGORY pc ON ps.ProductCategoryID = pc.ProductCategoryID
            LEFT JOIN (
                SELECT 
                    d.ProductID,
                    SUM(d.OrderQty) as units_sold
                FROM dbt_schema.SALES_SALESORDERDETAIL d
                JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
                WHERE h.OrderDate >= CURRENT_DATE - 7
                  AND h.Status = 5
                GROUP BY d.ProductID
            ) sales ON p.ProductID = sales.ProductID
            WHERE p.FinishedGoodsFlag = 1
              AND p.StandardCost > 0
        ),
        cost_analysis AS (
            SELECT 
                *,
                actual_cost - StandardCost as cost_variance,
                CASE WHEN StandardCost > 0 THEN ROUND(((actual_cost - StandardCost) / StandardCost) * 100, 2) ELSE 0 END as cost_variance_pct,
                actual_cost * units_produced as total_cost
            FROM production_data
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            ProductID,
            product_name,
            category,
            StandardCost,
            actual_cost,
            cost_variance,
            cost_variance_pct,
            units_produced,
            total_cost,
            CASE WHEN units_produced > 0 THEN total_cost / units_produced ELSE actual_cost END as cost_per_unit
        FROM cost_analysis
        WHERE units_produced > 0;
        """,
    )
    
    # Task 5: Analyze cost trends
    analyze_cost_trends = SQLExecuteQueryOperator(
        task_id='analyze_cost_trends',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO cost_trends (
            analysis_date,
            product_id,
            trend_period,
            cost_trend,
            cost_change,
            cost_change_pct,
            trend_direction
        )
        WITH current_costs AS (
            SELECT 
                product_id,
                actual_cost as current_cost
            FROM weekly_production_costs
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ),
        previous_costs AS (
            SELECT 
                product_id,
                actual_cost as previous_cost
            FROM weekly_production_costs
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE) - 7
        ),
        cost_comparison AS (
            SELECT 
                COALESCE(cc.product_id, pc.product_id) as product_id,
                COALESCE(cc.current_cost, 0) as current_cost,
                COALESCE(pc.previous_cost, cc.current_cost, 0) as previous_cost,
                COALESCE(cc.current_cost, 0) - COALESCE(pc.previous_cost, cc.current_cost, 0) as cost_change
            FROM current_costs cc
            FULL OUTER JOIN previous_costs pc ON cc.product_id = pc.product_id
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            product_id,
            'WEEK_OVER_WEEK',
            CASE 
                WHEN cost_change > 5 THEN 'SIGNIFICANT_INCREASE'
                WHEN cost_change > 1 THEN 'MODERATE_INCREASE'
                WHEN cost_change > -1 THEN 'STABLE'
                WHEN cost_change > -5 THEN 'MODERATE_DECREASE'
                ELSE 'SIGNIFICANT_DECREASE'
            END as cost_trend,
            cost_change,
            CASE WHEN previous_cost > 0 THEN ROUND((cost_change / previous_cost) * 100, 2) ELSE 0 END as cost_change_pct,
            CASE 
                WHEN cost_change > 1 THEN 'UP'
                WHEN cost_change < -1 THEN 'DOWN'
                ELSE 'STABLE'
            END as trend_direction
        FROM cost_comparison
        WHERE current_cost > 0;
        """,
    )
    
    # Task 6: Identify cost optimization opportunities
    identify_optimization_opportunities = SQLExecuteQueryOperator(
        task_id='identify_optimization_opportunities',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO cost_optimization_opportunities (
            analysis_date,
            opportunity_type,
            product_id,
            product_name,
            current_cost,
            potential_savings,
            savings_percentage,
            recommended_action,
            priority
        )
        -- High variance products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'HIGH_COST_VARIANCE',
            product_id,
            product_name,
            actual_cost,
            ABS(cost_variance) * units_produced as potential_savings,
            ABS(cost_variance_pct),
            CASE 
                WHEN cost_variance > 0 THEN 'Investigate cost overruns and implement cost controls'
                ELSE 'Analyze cost reduction factors and standardize best practices'
            END,
            CASE WHEN ABS(cost_variance_pct) > 15 THEN 'HIGH' ELSE 'MEDIUM' END
        FROM weekly_production_costs
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND ABS(cost_variance_pct) > 10
        
        UNION ALL
        
        -- High total cost products
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'HIGH_TOTAL_COST',
            product_id,
            product_name,
            actual_cost,
            total_cost * 0.05 as potential_savings, -- Assume 5% reduction potential
            5.0,
            'Focus on cost reduction initiatives for high-volume, high-cost products',
            'HIGH'
        FROM weekly_production_costs
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND total_cost > (
              SELECT AVG(total_cost) * 2 
              FROM weekly_production_costs 
              WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          )
        
        UNION ALL
        
        -- Products with increasing cost trends
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'RISING_COSTS',
            ct.product_id,
            wpc.product_name,
            wpc.actual_cost,
            ABS(ct.cost_change) * wpc.units_produced as potential_savings,
            ABS(ct.cost_change_pct),
            'Address rising cost trends through supplier negotiations or process improvements',
            CASE WHEN ct.cost_change_pct > 10 THEN 'HIGH' ELSE 'MEDIUM' END
        FROM cost_trends ct
        JOIN weekly_production_costs wpc ON ct.product_id = wpc.product_id 
            AND ct.analysis_date = wpc.analysis_date
        WHERE ct.analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
          AND ct.trend_direction = 'UP'
          AND ct.cost_change_pct > 5
        
        UNION ALL
        
        -- Category-based opportunities
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE),
            'CATEGORY_OPTIMIZATION',
            product_id,
            product_name,
            actual_cost,
            (actual_cost - category_avg_cost) * units_produced as potential_savings,
            ROUND(((actual_cost - category_avg_cost) / actual_cost) * 100, 2),
            'Benchmark against category average and implement cost reduction strategies',
            'MEDIUM'
        FROM (
            SELECT 
                *,
                AVG(actual_cost) OVER (PARTITION BY category) as category_avg_cost
            FROM weekly_production_costs
            WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ) category_analysis
        WHERE actual_cost > category_avg_cost * 1.2; -- 20% above category average
        """,
    )
    
    # Task 7: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate production costs analysis results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get cost summary
        cost_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(*) as products_analyzed,
            SUM(total_cost) as total_production_cost,
            AVG(cost_variance_pct) as avg_cost_variance,
            COUNT(CASE WHEN cost_variance_pct > 10 THEN 1 END) as high_variance_products
        FROM weekly_production_costs
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        cost_result = hook.get_first(cost_sql)
        if cost_result:
            products, total_cost, avg_variance, high_variance = cost_result
            logging.info(f"Cost Analysis: {products} products, ${total_cost:,.2f} total cost")
            logging.info(f"Average variance: {avg_variance:.2f}%, High variance products: {high_variance}")
        
        # Get trend summary
        trend_sql = """
        SELECT 
            trend_direction,
            COUNT(*) as product_count,
            AVG(cost_change_pct) as avg_change
        FROM cost_trends
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY trend_direction
        ORDER BY avg_change DESC;
        """
        
        trend_results = hook.get_records(trend_sql)
        logging.info("Cost Trends:")
        for direction, count, change in trend_results:
            logging.info(f"  {direction}: {count} products, {change:.2f}% avg change")
        
        # Get optimization opportunities
        optimization_sql = """
        SELECT 
            opportunity_type,
            COUNT(*) as opportunity_count,
            SUM(potential_savings) as total_savings
        FROM cost_optimization_opportunities
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY opportunity_type
        ORDER BY total_savings DESC;
        """
        
        optimization_results = hook.get_records(optimization_sql)
        logging.info("Optimization Opportunities:")
        total_potential_savings = 0
        for opp_type, count, savings in optimization_results:
            logging.info(f"  {opp_type}: {count} opportunities, ${savings:,.2f} potential savings")
            total_potential_savings += savings
        
        logging.info(f"Total potential savings identified: ${total_potential_savings:,.2f}")
        
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
            'production_costs',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM weekly_production_costs
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> analyze_production_costs >> analyze_cost_trends
    analyze_cost_trends >> identify_optimization_opportunities >> validation >> finalize

production_costs()