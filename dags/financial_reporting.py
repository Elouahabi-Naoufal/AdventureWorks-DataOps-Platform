"""
Financial Reporting DAG
Monthly financial reporting with P&L, revenue analysis, and cost breakdowns.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='financial_reporting',
    start_date=datetime(2024, 8, 1),
    schedule='@monthly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'finance_team',
        'email_on_failure': True,
    },
    tags=['financial', 'reporting', 'monthly', 'revenue'],
    description='Monthly financial reporting and analysis',
)
def financial_reporting():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS monthly_revenue_report (
            report_month DATE,
            territory_id INTEGER,
            product_category VARCHAR(50),
            gross_revenue DECIMAL(19,4),
            net_revenue DECIMAL(19,4),
            cost_of_goods DECIMAL(19,4),
            gross_profit DECIMAL(19,4),
            gross_margin DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS profit_loss_summary (
            report_month DATE,
            revenue DECIMAL(19,4),
            cost_of_goods_sold DECIMAL(19,4),
            gross_profit DECIMAL(19,4),
            operating_expenses DECIMAL(19,4),
            net_profit DECIMAL(19,4),
            profit_margin DECIMAL(5,2),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS financial_kpis (
            report_month DATE,
            kpi_name VARCHAR(50),
            kpi_value DECIMAL(19,4),
            kpi_description VARCHAR(200),
            trend_direction VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if financial data exists for the previous month"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM SALES_SALESORDERHEADER 
        WHERE DATE_TRUNC('MONTH', OrderDate) = DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE))
          AND Status = 5;
        """
        
        result = hook.get_first(sql)
        order_count = result[0] if result else 0
        
        logging.info(f"Found {order_count} orders for previous month")
        
        if order_count < 10:
            logging.info("Insufficient financial data - completing successfully")
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
        
        SET report_month = (SELECT DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE)));
        
        DELETE FROM monthly_revenue_report WHERE report_month = $report_month;
        DELETE FROM profit_loss_summary WHERE report_month = $report_month;
        DELETE FROM financial_kpis WHERE report_month = $report_month;
        """,
    )
    
    # Task 4: Generate monthly revenue report
    generate_revenue_report = SQLExecuteQueryOperator(
        task_id='generate_revenue_report',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SET report_month = (SELECT DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE)));
        
        INSERT INTO monthly_revenue_report (
            report_month,
            territory_id,
            product_category,
            gross_revenue,
            net_revenue,
            cost_of_goods,
            gross_profit,
            gross_margin
        )
        WITH revenue_data AS (
            SELECT 
                h.TerritoryID,
                COALESCE(pc.Name, 'Unknown') as product_category,
                SUM(d.LineTotal) as gross_revenue,
                SUM(d.LineTotal * 0.95) as net_revenue, -- Assume 5% discounts/returns
                SUM(d.OrderQty * p.StandardCost) as cost_of_goods
            FROM dbt_schema.SALES_SALESORDERHEADER h
            JOIN dbt_schema.SALES_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCT p ON d.ProductID = p.ProductID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTSUBCATEGORY ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN dbt_schema.PRODUCTION_PRODUCTCATEGORY pc ON ps.ProductCategoryID = pc.ProductCategoryID
            WHERE DATE_TRUNC('MONTH', h.OrderDate) = $report_month
              AND h.Status = 5
            GROUP BY h.TerritoryID, pc.Name
        )
        SELECT 
            $report_month,
            TerritoryID,
            product_category,
            gross_revenue,
            net_revenue,
            cost_of_goods,
            net_revenue - cost_of_goods as gross_profit,
            CASE WHEN net_revenue > 0 THEN ROUND(((net_revenue - cost_of_goods) / net_revenue) * 100, 2) ELSE 0 END as gross_margin
        FROM revenue_data
        WHERE gross_revenue > 0;
        """,
    )
    
    # Task 5: Generate P&L summary
    generate_pl_summary = SQLExecuteQueryOperator(
        task_id='generate_pl_summary',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SET report_month = (SELECT DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE)));
        
        INSERT INTO profit_loss_summary (
            report_month,
            revenue,
            cost_of_goods_sold,
            gross_profit,
            operating_expenses,
            net_profit,
            profit_margin
        )
        WITH pl_data AS (
            SELECT 
                SUM(gross_revenue) as total_revenue,
                SUM(cost_of_goods) as total_cogs,
                SUM(gross_profit) as total_gross_profit
            FROM monthly_revenue_report
            WHERE report_month = $report_month
        ),
        operating_costs AS (
            SELECT 
                total_revenue * 0.25 as estimated_operating_expenses -- Estimate 25% of revenue
            FROM pl_data
        )
        SELECT 
            $report_month,
            pl.total_revenue,
            pl.total_cogs,
            pl.total_gross_profit,
            oc.estimated_operating_expenses,
            pl.total_gross_profit - oc.estimated_operating_expenses as net_profit,
            CASE 
                WHEN pl.total_revenue > 0 
                THEN ROUND(((pl.total_gross_profit - oc.estimated_operating_expenses) / pl.total_revenue) * 100, 2) 
                ELSE 0 
            END as profit_margin
        FROM pl_data pl
        CROSS JOIN operating_costs oc;
        """,
    )
    
    # Task 6: Calculate financial KPIs
    calculate_financial_kpis = SQLExecuteQueryOperator(
        task_id='calculate_financial_kpis',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SET report_month = (SELECT DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE)));
        SET prev_month = (SELECT DATE_TRUNC('MONTH', DATEADD(month, -2, CURRENT_DATE)));
        
        INSERT INTO financial_kpis (
            report_month,
            kpi_name,
            kpi_value,
            kpi_description,
            trend_direction
        )
        WITH current_month_kpis AS (
            SELECT 
                revenue,
                profit_margin,
                net_profit
            FROM profit_loss_summary
            WHERE report_month = $report_month
        ),
        previous_month_kpis AS (
            SELECT 
                revenue as prev_revenue,
                profit_margin as prev_profit_margin,
                net_profit as prev_net_profit
            FROM profit_loss_summary
            WHERE report_month = $prev_month
        ),
        additional_kpis AS (
            SELECT 
                AVG(gross_margin) as avg_gross_margin,
                COUNT(DISTINCT territory_id) as active_territories,
                COUNT(DISTINCT product_category) as active_categories
            FROM monthly_revenue_report
            WHERE report_month = $report_month
        )
        SELECT $report_month, 'TOTAL_REVENUE', cm.revenue, 'Total monthly revenue', 
               CASE WHEN cm.revenue > COALESCE(pm.prev_revenue, 0) THEN 'UP' 
                    WHEN cm.revenue < COALESCE(pm.prev_revenue, 0) THEN 'DOWN' 
                    ELSE 'STABLE' END
        FROM current_month_kpis cm
        LEFT JOIN previous_month_kpis pm ON 1=1
        
        UNION ALL
        
        SELECT $report_month, 'PROFIT_MARGIN', cm.profit_margin, 'Net profit margin percentage',
               CASE WHEN cm.profit_margin > COALESCE(pm.prev_profit_margin, 0) THEN 'UP' 
                    WHEN cm.profit_margin < COALESCE(pm.prev_profit_margin, 0) THEN 'DOWN' 
                    ELSE 'STABLE' END
        FROM current_month_kpis cm
        LEFT JOIN previous_month_kpis pm ON 1=1
        
        UNION ALL
        
        SELECT $report_month, 'NET_PROFIT', cm.net_profit, 'Net profit after all expenses',
               CASE WHEN cm.net_profit > COALESCE(pm.prev_net_profit, 0) THEN 'UP' 
                    WHEN cm.net_profit < COALESCE(pm.prev_net_profit, 0) THEN 'DOWN' 
                    ELSE 'STABLE' END
        FROM current_month_kpis cm
        LEFT JOIN previous_month_kpis pm ON 1=1
        
        UNION ALL
        
        SELECT $report_month, 'AVG_GROSS_MARGIN', ak.avg_gross_margin, 'Average gross margin across all categories', 'STABLE'
        FROM additional_kpis ak
        
        UNION ALL
        
        SELECT $report_month, 'ACTIVE_TERRITORIES', ak.active_territories, 'Number of territories with sales', 'STABLE'
        FROM additional_kpis ak;
        """,
    )
    
    # Task 7: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate financial reporting results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get P&L summary
        pl_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            revenue,
            gross_profit,
            net_profit,
            profit_margin
        FROM profit_loss_summary
        WHERE report_month = DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE));
        """
        
        pl_result = hook.get_first(pl_sql)
        if pl_result:
            revenue, gross_profit, net_profit, margin = pl_result
            logging.info(f"Financial Summary - Revenue: ${revenue:,.2f}, Gross Profit: ${gross_profit:,.2f}")
            logging.info(f"Net Profit: ${net_profit:,.2f}, Margin: {margin}%")
        
        # Get KPI trends
        kpi_sql = """
        SELECT kpi_name, kpi_value, trend_direction
        FROM financial_kpis
        WHERE report_month = DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE))
        ORDER BY kpi_name;
        """
        
        kpi_results = hook.get_records(kpi_sql)
        logging.info("Key Performance Indicators:")
        for kpi_name, kpi_value, trend in kpi_results:
            logging.info(f"  {kpi_name}: {kpi_value:,.2f} ({trend})")
        
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
            'financial_reporting',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM monthly_revenue_report
        WHERE report_month = DATE_TRUNC('MONTH', DATEADD(month, -1, CURRENT_DATE));
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> generate_revenue_report >> generate_pl_summary
    generate_pl_summary >> calculate_financial_kpis >> validation >> finalize

financial_reporting()