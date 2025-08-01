from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id='product_inventory_management',
    start_date=datetime(2024, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
    tags=['inventory', 'products', 'manufacturing']
)
def product_inventory_management():
    
    @task
    def check_low_stock_products():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            p.ProductID,
            p.Name,
            p.SafetyStockLevel,
            p.ReorderPoint,
            COALESCE(SUM(pi.Quantity), 0) as current_stock
        FROM dbt_db.dbt_schema.Production_Product p
        LEFT JOIN dbt_db.dbt_schema.Production_ProductInventory pi ON p.ProductID = pi.ProductID
        WHERE p.FinishedGoodsFlag = TRUE
        GROUP BY p.ProductID, p.Name, p.SafetyStockLevel, p.ReorderPoint
        HAVING current_stock <= p.ReorderPoint
        ORDER BY current_stock ASC
        """
        results = hook.get_records(sql)
        return [{
            'product_id': r[0],
            'name': r[1],
            'safety_stock': r[2],
            'reorder_point': r[3],
            'current_stock': r[4]
        } for r in results]
    
    @task
    def analyze_product_categories():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            pc.Name as category,
            COUNT(p.ProductID) as product_count,
            AVG(p.ListPrice) as avg_price,
            SUM(COALESCE(pi.Quantity, 0)) as total_inventory
        FROM dbt_db.dbt_schema.Production_ProductCategory pc
        JOIN dbt_db.dbt_schema.Production_ProductSubcategory psc ON pc.ProductCategoryID = psc.ProductCategoryID
        JOIN dbt_db.dbt_schema.Production_Product p ON psc.ProductSubcategoryID = p.ProductSubcategoryID
        LEFT JOIN dbt_db.dbt_schema.Production_ProductInventory pi ON p.ProductID = pi.ProductID
        GROUP BY pc.Name
        ORDER BY total_inventory DESC
        """
        results = hook.get_records(sql)
        return [{
            'category': r[0],
            'product_count': r[1],
            'avg_price': float(r[2]) if r[2] else 0,
            'total_inventory': r[3] if r[3] else 0
        } for r in results]
    
    @task
    def get_work_order_status():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            p.Name as product_name,
            wo.WorkOrderID,
            wo.OrderQty,
            wo.StockedQty,
            wo.ScrappedQty,
            wo.StartDate,
            wo.EndDate,
            CASE 
                WHEN wo.EndDate IS NOT NULL THEN 'Completed'
                WHEN wo.StartDate <= CURRENT_DATE THEN 'In Progress'
                ELSE 'Scheduled'
            END as status
        FROM dbt_db.dbt_schema.Production_WorkOrder wo
        JOIN dbt_db.dbt_schema.Production_Product p ON wo.ProductID = p.ProductID
        WHERE wo.StartDate >= CURRENT_DATE - 30
        ORDER BY wo.StartDate DESC
        """
        results = hook.get_records(sql)
        return [{
            'product_name': r[0],
            'work_order_id': r[1],
            'order_qty': r[2],
            'stocked_qty': r[3],
            'scrapped_qty': r[4],
            'start_date': r[5].isoformat() if r[5] else None,
            'end_date': r[6].isoformat() if r[6] else None,
            'status': r[7]
        } for r in results]
    
    @task
    def calculate_inventory_turnover(low_stock, categories):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            p.ProductID,
            p.Name,
            COALESCE(SUM(sod.OrderQty), 0) as units_sold_30d,
            COALESCE(AVG(pi.Quantity), 0) as avg_inventory
        FROM dbt_db.dbt_schema.Production_Product p
        LEFT JOIN dbt_db.dbt_schema.Sales_SalesOrderDetail sod ON p.ProductID = sod.ProductID
        LEFT JOIN dbt_db.dbt_schema.Sales_SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
        LEFT JOIN dbt_db.dbt_schema.Production_ProductInventory pi ON p.ProductID = pi.ProductID
        WHERE soh.OrderDate >= CURRENT_DATE - 30 OR soh.OrderDate IS NULL
        GROUP BY p.ProductID, p.Name
        HAVING avg_inventory > 0
        ORDER BY units_sold_30d DESC
        """
        results = hook.get_records(sql)
        turnover_data = []
        
        for r in results:
            turnover_rate = (r[2] / r[3]) if r[3] > 0 else 0
            turnover_data.append({
                'product_id': r[0],
                'name': r[1],
                'units_sold': r[2],
                'avg_inventory': r[3],
                'turnover_rate': round(turnover_rate, 2)
            })
        
        return {
            'low_stock_count': len(low_stock),
            'category_analysis': categories,
            'turnover_analysis': turnover_data[:20]
        }
    
    create_inventory_alerts = SQLExecuteQueryOperator(
        task_id='create_inventory_alerts',
        conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE dbt_db.dbt_schema.inventory_alerts (
            alert_id INTEGER AUTOINCREMENT PRIMARY KEY,
            product_id INTEGER,
            product_name VARCHAR(50),
            alert_type VARCHAR(20),
            current_stock INTEGER,
            reorder_point INTEGER,
            alert_date DATE DEFAULT CURRENT_DATE,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    
    @task
    def generate_reorder_recommendations(inventory_analysis):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        recommendations = []
        for item in inventory_analysis['turnover_analysis']:
            if item['turnover_rate'] > 2:  # High turnover
                recommended_qty = int(item['avg_inventory'] * 1.5)
                recommendations.append({
                    'product_id': item['product_id'],
                    'product_name': item['name'],
                    'current_turnover': item['turnover_rate'],
                    'recommended_order_qty': recommended_qty,
                    'reason': 'High turnover rate'
                })
        
        # Insert alerts for low stock items
        for item in inventory_analysis.get('low_stock_items', []):
            sql = """
            INSERT INTO dbt_db.dbt_schema.inventory_alerts 
            (product_id, product_name, alert_type, current_stock, reorder_point)
            VALUES (%s, %s, 'LOW_STOCK', %s, %s)
            """
            hook.run(sql, parameters=[
                item['product_id'],
                item['name'],
                item['current_stock'],
                item['reorder_point']
            ])
        
        return recommendations[:10]
    
    # Task dependencies with divergent paths
    low_stock = check_low_stock_products()
    categories = analyze_product_categories()
    work_orders = get_work_order_status()
    
    # Parallel analysis branches
    inventory_analysis = calculate_inventory_turnover(low_stock, categories)
    
    create_inventory_alerts >> generate_reorder_recommendations(inventory_analysis)

product_inventory_management()