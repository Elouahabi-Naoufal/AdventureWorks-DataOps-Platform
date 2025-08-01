from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id='sales_analytics_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['sales', 'analytics', 'snowflake']
)
def sales_analytics_pipeline():
    
    @task
    def get_sales_metrics():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(TotalDue) as total_revenue,
            AVG(TotalDue) as avg_order_value
        FROM dbt_db.dbt_schema.Sales_SalesOrderHeader
        WHERE OrderDate >= CURRENT_DATE - 30
        """
        result = hook.get_first(sql)
        return {
            'total_orders': result[0],
            'total_revenue': float(result[1]),
            'avg_order_value': float(result[2])
        }
    
    @task
    def get_top_products(metrics):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            p.Name,
            SUM(sod.OrderQty) as total_qty,
            SUM(sod.LineTotal) as total_sales
        FROM dbt_db.dbt_schema.Sales_SalesOrderDetail sod
        JOIN dbt_db.dbt_schema.Production_Product p ON sod.ProductID = p.ProductID
        JOIN dbt_db.dbt_schema.Sales_SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
        WHERE soh.OrderDate >= CURRENT_DATE - 30
        GROUP BY p.Name
        ORDER BY total_sales DESC
        LIMIT 10
        """
        results = hook.get_records(sql)
        return [{'product': r[0], 'qty': r[1], 'sales': float(r[2])} for r in results]
    
    @task
    def get_territory_performance():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            st.Name as territory,
            COUNT(soh.SalesOrderID) as order_count,
            SUM(soh.TotalDue) as revenue
        FROM dbt_db.dbt_schema.Sales_SalesOrderHeader soh
        JOIN dbt_db.dbt_schema.Sales_SalesTerritory st ON soh.TerritoryID = st.TerritoryID
        WHERE soh.OrderDate >= CURRENT_DATE - 30
        GROUP BY st.Name
        ORDER BY revenue DESC
        """
        results = hook.get_records(sql)
        return [{'territory': r[0], 'orders': r[1], 'revenue': float(r[2])} for r in results]
    
    @task
    def analyze_customer_segments(metrics, products, territories):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            CASE 
                WHEN soh.TotalDue > 5000 THEN 'High Value'
                WHEN soh.TotalDue > 1000 THEN 'Medium Value'
                ELSE 'Low Value'
            END as segment,
            COUNT(*) as customer_count,
            AVG(soh.TotalDue) as avg_spend
        FROM dbt_db.dbt_schema.Sales_SalesOrderHeader soh
        WHERE soh.OrderDate >= CURRENT_DATE - 30
        GROUP BY segment
        """
        segments = hook.get_records(sql)
        
        return {
            'summary': metrics,
            'top_products': products[:5],
            'territories': territories[:5],
            'segments': [{'segment': s[0], 'count': s[1], 'avg_spend': float(s[2])} for s in segments]
        }
    
    create_summary_table = SQLExecuteQueryOperator(
        task_id='create_summary_table',
        conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE dbt_db.dbt_schema.daily_sales_summary (
            report_date DATE,
            total_orders INTEGER,
            total_revenue DECIMAL(19,4),
            avg_order_value DECIMAL(19,4),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    
    @task
    def insert_summary(analysis_result):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        INSERT INTO dbt_db.dbt_schema.daily_sales_summary 
        (report_date, total_orders, total_revenue, avg_order_value)
        VALUES (CURRENT_DATE, %s, %s, %s)
        """
        hook.run(sql, parameters=[
            analysis_result['summary']['total_orders'],
            analysis_result['summary']['total_revenue'],
            analysis_result['summary']['avg_order_value']
        ])
        return "Summary inserted successfully"
    
    # Task dependencies
    metrics = get_sales_metrics()
    products = get_top_products(metrics)
    territories = get_territory_performance()
    analysis = analyze_customer_segments(metrics, products, territories)
    
    create_summary_table >> insert_summary(analysis)

sales_analytics_pipeline()