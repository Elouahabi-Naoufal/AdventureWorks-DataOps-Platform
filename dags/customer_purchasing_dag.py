from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id='customer_purchasing_insights',
    start_date=datetime(2024, 1, 1),
    schedule='0 10 * * *',  # Daily at 10 AM
    catchup=False,
    tags=['customers', 'purchasing', 'vendors']
)
def customer_purchasing_insights():
    
    @task
    def analyze_customer_behavior():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            c.CustomerID,
            COALESCE(p.FirstName || ' ' || p.LastName, s.Name) as customer_name,
            COUNT(soh.SalesOrderID) as total_orders,
            SUM(soh.TotalDue) as total_spent,
            AVG(soh.TotalDue) as avg_order_value,
            MAX(soh.OrderDate) as last_order_date,
            DATEDIFF('day', MAX(soh.OrderDate), CURRENT_DATE) as days_since_last_order
        FROM dbt_db.dbt_schema.Sales_Customer c
        LEFT JOIN dbt_db.dbt_schema.Person_Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN dbt_db.dbt_schema.Sales_Store s ON c.StoreID = s.BusinessEntityID
        LEFT JOIN dbt_db.dbt_schema.Sales_SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
        WHERE soh.OrderDate >= CURRENT_DATE - 365
        GROUP BY c.CustomerID, customer_name
        HAVING total_orders > 0
        ORDER BY total_spent DESC
        """
        results = hook.get_records(sql)
        return [{
            'customer_id': r[0],
            'customer_name': r[1],
            'total_orders': r[2],
            'total_spent': float(r[3]),
            'avg_order_value': float(r[4]),
            'last_order_date': r[5].isoformat() if r[5] else None,
            'days_since_last_order': r[6] if r[6] else 0
        } for r in results]
    
    @task
    def segment_customers(customer_data):
        # Customer segmentation based on RFM analysis
        segments = {
            'vip': [],
            'loyal': [],
            'at_risk': [],
            'new': []
        }
        
        for customer in customer_data:
            recency = customer['days_since_last_order']
            frequency = customer['total_orders']
            monetary = customer['total_spent']
            
            if monetary > 10000 and frequency > 10 and recency < 30:
                segments['vip'].append(customer)
            elif frequency > 5 and recency < 60:
                segments['loyal'].append(customer)
            elif frequency > 3 and recency > 90:
                segments['at_risk'].append(customer)
            else:
                segments['new'].append(customer)
        
        return segments
    
    @task
    def analyze_vendor_performance():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            v.Name as vendor_name,
            v.CreditRating,
            COUNT(poh.PurchaseOrderID) as total_orders,
            SUM(poh.TotalDue) as total_purchase_value,
            AVG(poh.TotalDue) as avg_order_value,
            AVG(DATEDIFF('day', poh.OrderDate, poh.ShipDate)) as avg_delivery_days
        FROM dbt_db.dbt_schema.Purchasing_Vendor v
        JOIN dbt_db.dbt_schema.Purchasing_PurchaseOrderHeader poh ON v.BusinessEntityID = poh.VendorID
        WHERE poh.OrderDate >= CURRENT_DATE - 180
        GROUP BY v.Name, v.CreditRating
        ORDER BY total_purchase_value DESC
        """
        results = hook.get_records(sql)
        return [{
            'vendor_name': r[0],
            'credit_rating': r[1],
            'total_orders': r[2],
            'total_purchase_value': float(r[3]),
            'avg_order_value': float(r[4]),
            'avg_delivery_days': float(r[5]) if r[5] else 0
        } for r in results]
    
    @task
    def analyze_product_demand():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            p.ProductID,
            p.Name as product_name,
            pc.Name as category,
            SUM(sod.OrderQty) as total_qty_sold,
            SUM(sod.LineTotal) as total_revenue,
            COUNT(DISTINCT soh.CustomerID) as unique_customers,
            AVG(sod.UnitPrice) as avg_selling_price,
            p.StandardCost,
            (AVG(sod.UnitPrice) - p.StandardCost) as profit_margin
        FROM dbt_db.dbt_schema.Production_Product p
        JOIN dbt_db.dbt_schema.Sales_SalesOrderDetail sod ON p.ProductID = sod.ProductID
        JOIN dbt_db.dbt_schema.Sales_SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
        LEFT JOIN dbt_db.dbt_schema.Production_ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        LEFT JOIN dbt_db.dbt_schema.Production_ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        WHERE soh.OrderDate >= CURRENT_DATE - 90
        GROUP BY p.ProductID, p.Name, pc.Name, p.StandardCost
        ORDER BY total_revenue DESC
        """
        results = hook.get_records(sql)
        return [{
            'product_id': r[0],
            'product_name': r[1],
            'category': r[2] if r[2] else 'Uncategorized',
            'total_qty_sold': r[3],
            'total_revenue': float(r[4]),
            'unique_customers': r[5],
            'avg_selling_price': float(r[6]),
            'standard_cost': float(r[7]),
            'profit_margin': float(r[8]) if r[8] else 0
        } for r in results]
    
    @task
    def calculate_purchase_patterns(customer_segments, vendor_data, product_demand):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            EXTRACT(MONTH FROM soh.OrderDate) as order_month,
            EXTRACT(DOW FROM soh.OrderDate) as day_of_week,
            COUNT(*) as order_count,
            AVG(soh.TotalDue) as avg_order_value
        FROM dbt_db.dbt_schema.Sales_SalesOrderHeader soh
        WHERE soh.OrderDate >= CURRENT_DATE - 365
        GROUP BY order_month, day_of_week
        ORDER BY order_month, day_of_week
        """
        seasonal_data = hook.get_records(sql)
        
        patterns = {
            'seasonal_trends': [{
                'month': r[0],
                'day_of_week': r[1],
                'order_count': r[2],
                'avg_order_value': float(r[3])
            } for r in seasonal_data],
            'customer_segments_summary': {
                'vip_count': len(customer_segments['vip']),
                'loyal_count': len(customer_segments['loyal']),
                'at_risk_count': len(customer_segments['at_risk']),
                'new_count': len(customer_segments['new'])
            },
            'top_vendors': vendor_data[:5],
            'top_products': product_demand[:10]
        }
        
        return patterns
    
    @task
    def identify_cross_sell_opportunities(product_demand):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            sod1.ProductID as product_a,
            p1.Name as product_a_name,
            sod2.ProductID as product_b,
            p2.Name as product_b_name,
            COUNT(*) as co_purchase_frequency
        FROM dbt_db.dbt_schema.Sales_SalesOrderDetail sod1
        JOIN dbt_db.dbt_schema.Sales_SalesOrderDetail sod2 ON sod1.SalesOrderID = sod2.SalesOrderID
        JOIN dbt_db.dbt_schema.Production_Product p1 ON sod1.ProductID = p1.ProductID
        JOIN dbt_db.dbt_schema.Production_Product p2 ON sod2.ProductID = p2.ProductID
        JOIN dbt_db.dbt_schema.Sales_SalesOrderHeader soh ON sod1.SalesOrderID = soh.SalesOrderID
        WHERE sod1.ProductID < sod2.ProductID
        AND soh.OrderDate >= CURRENT_DATE - 180
        GROUP BY sod1.ProductID, p1.Name, sod2.ProductID, p2.Name
        HAVING co_purchase_frequency >= 5
        ORDER BY co_purchase_frequency DESC
        LIMIT 20
        """
        results = hook.get_records(sql)
        return [{
            'product_a_id': r[0],
            'product_a_name': r[1],
            'product_b_id': r[2],
            'product_b_name': r[3],
            'co_purchase_frequency': r[4]
        } for r in results]
    
    create_insights_tables = SQLExecuteQueryOperator(
        task_id='create_insights_tables',
        conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE dbt_db.dbt_schema.customer_insights (
            insight_id INTEGER AUTOINCREMENT PRIMARY KEY,
            customer_id INTEGER,
            segment VARCHAR(20),
            total_spent DECIMAL(19,4),
            last_order_days_ago INTEGER,
            risk_score DECIMAL(5,2),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE OR REPLACE TABLE dbt_db.dbt_schema.cross_sell_recommendations (
            recommendation_id INTEGER AUTOINCREMENT PRIMARY KEY,
            product_a_id INTEGER,
            product_b_id INTEGER,
            confidence_score DECIMAL(5,2),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    
    @task
    def generate_business_recommendations(patterns, cross_sell_data):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Insert cross-sell recommendations
        for item in cross_sell_data:
            confidence = min(100, (item['co_purchase_frequency'] / 20) * 100)
            sql = """
            INSERT INTO dbt_db.dbt_schema.cross_sell_recommendations 
            (product_a_id, product_b_id, confidence_score)
            VALUES (%s, %s, %s)
            """
            hook.run(sql, parameters=[
                item['product_a_id'],
                item['product_b_id'],
                confidence
            ])
        
        recommendations = {
            'marketing_focus': 'Target VIP customers with premium products',
            'inventory_priority': [p['product_name'] for p in patterns['top_products'][:5]],
            'vendor_optimization': f"Focus on top {len(patterns['top_vendors'])} vendors",
            'cross_sell_opportunities': len(cross_sell_data),
            'at_risk_customers': patterns['customer_segments_summary']['at_risk_count']
        }
        
        return recommendations
    
    # Complex task dependencies with multiple divergent paths
    customers = analyze_customer_behavior()
    customer_segments = segment_customers(customers)
    
    # Parallel vendor and product analysis
    vendors = analyze_vendor_performance()
    products = analyze_product_demand()
    
    # Pattern analysis combining multiple inputs
    patterns = calculate_purchase_patterns(customer_segments, vendors, products)
    cross_sell = identify_cross_sell_opportunities(products)
    
    # Final recommendations
    create_insights_tables >> generate_business_recommendations(patterns, cross_sell)

customer_purchasing_insights()