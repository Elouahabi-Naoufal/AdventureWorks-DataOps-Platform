"""
Inventory Management DAG
Daily inventory tracking, stock level monitoring, and reorder point analysis.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='inventory_management',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'operations_team',
        'email_on_failure': True,
    },
    tags=['inventory', 'stock', 'daily', 'operations'],
    description='Daily inventory management and stock monitoring',
)
def inventory_management():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        CREATE TABLE IF NOT EXISTS daily_inventory_status (
            status_date DATE,
            product_id INTEGER,
            product_name VARCHAR(100),
            current_stock INTEGER,
            reorder_point INTEGER,
            safety_stock INTEGER,
            days_of_supply DECIMAL(10,2),
            stock_status VARCHAR(20),
            reorder_needed BOOLEAN,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS inventory_movements (
            movement_date DATE,
            product_id INTEGER,
            movement_type VARCHAR(20),
            quantity INTEGER,
            unit_cost DECIMAL(19,4),
            total_value DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        CREATE TABLE IF NOT EXISTS stock_alerts (
            alert_date DATE,
            product_id INTEGER,
            product_name VARCHAR(100),
            alert_type VARCHAR(30),
            current_stock INTEGER,
            recommended_action VARCHAR(200),
            priority VARCHAR(10),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if product and inventory data exists"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT COUNT(*) FROM PRODUCTION_PRODUCT 
        WHERE FinishedGoodsFlag = 1;
        """
        
        result = hook.get_first(sql)
        product_count = result[0] if result else 0
        
        logging.info(f"Found {product_count} finished goods products")
        
        if product_count < 5:
            logging.info("Insufficient product data - completing successfully")
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
        
        DELETE FROM daily_inventory_status WHERE status_date = CURRENT_DATE;
        DELETE FROM inventory_movements WHERE movement_date = CURRENT_DATE;
        DELETE FROM stock_alerts WHERE alert_date = CURRENT_DATE;
        """,
    )
    
    # Task 4: Calculate current inventory status
    calculate_inventory_status = SQLExecuteQueryOperator(
        task_id='calculate_inventory_status',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO daily_inventory_status (
            status_date,
            product_id,
            product_name,
            current_stock,
            reorder_point,
            safety_stock,
            days_of_supply,
            stock_status,
            reorder_needed
        )
        WITH product_demand AS (
            SELECT 
                d.ProductID,
                AVG(d.OrderQty) as avg_daily_demand
            FROM dbt_schema.SALES_SALESORDERDETAIL d
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
            WHERE h.OrderDate >= CURRENT_DATE - 30
              AND h.Status = 5
            GROUP BY d.ProductID
        ),
        inventory_calc AS (
            SELECT 
                p.ProductID,
                p.Name as product_name,
                -- Simulate current stock based on product ID
                (p.ProductID % 1000) + 50 as current_stock,
                -- Calculate reorder point (avg demand * lead time + safety stock)
                COALESCE(pd.avg_daily_demand * 7, 10) + 20 as reorder_point,
                20 as safety_stock,
                CASE 
                    WHEN COALESCE(pd.avg_daily_demand, 1) > 0 
                    THEN ((p.ProductID % 1000) + 50) / COALESCE(pd.avg_daily_demand, 1)
                    ELSE 999
                END as days_of_supply
            FROM dbt_schema.PRODUCTION_PRODUCT p
            LEFT JOIN product_demand pd ON p.ProductID = pd.ProductID
            WHERE p.FinishedGoodsFlag = 1
              AND p.SellStartDate IS NOT NULL
        )
        SELECT 
            CURRENT_DATE,
            ProductID,
            product_name,
            current_stock,
            reorder_point,
            safety_stock,
            days_of_supply,
            CASE 
                WHEN current_stock <= safety_stock THEN 'CRITICAL'
                WHEN current_stock <= reorder_point THEN 'LOW'
                WHEN days_of_supply > 90 THEN 'EXCESS'
                ELSE 'NORMAL'
            END as stock_status,
            CASE WHEN current_stock <= reorder_point THEN TRUE ELSE FALSE END as reorder_needed
        FROM inventory_calc;
        """,
    )
    
    # Task 5: Track inventory movements
    track_inventory_movements = SQLExecuteQueryOperator(
        task_id='track_inventory_movements',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO inventory_movements (
            movement_date,
            product_id,
            movement_type,
            quantity,
            unit_cost,
            total_value
        )
        WITH recent_sales AS (
            SELECT 
                d.ProductID,
                SUM(d.OrderQty) as quantity_sold,
                AVG(p.StandardCost) as unit_cost
            FROM dbt_schema.SALES_SALESORDERDETAIL d
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON d.SalesOrderID = h.SalesOrderID
            JOIN dbt_schema.PRODUCTION_PRODUCT p ON d.ProductID = p.ProductID
            WHERE h.OrderDate = CURRENT_DATE - 1
              AND h.Status = 5
            GROUP BY d.ProductID
        ),
        simulated_receipts AS (
            SELECT 
                ProductID,
                -- Simulate inventory receipts for products with low stock
                CASE WHEN current_stock <= reorder_point THEN 100 ELSE 0 END as quantity_received,
                -- Use standard cost for receipts
                (SELECT AVG(StandardCost) FROM dbt_schema.PRODUCTION_PRODUCT WHERE ProductID = dis.ProductID) as unit_cost
            FROM daily_inventory_status dis
            WHERE status_date = CURRENT_DATE
              AND reorder_needed = TRUE
        )
        SELECT CURRENT_DATE, ProductID, 'SALE', -quantity_sold, unit_cost, -quantity_sold * unit_cost
        FROM recent_sales
        WHERE quantity_sold > 0
        
        UNION ALL
        
        SELECT CURRENT_DATE, ProductID, 'RECEIPT', quantity_received, unit_cost, quantity_received * unit_cost
        FROM simulated_receipts
        WHERE quantity_received > 0;
        """,
    )
    
    # Task 6: Generate stock alerts
    generate_stock_alerts = SQLExecuteQueryOperator(
        task_id='generate_stock_alerts',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO stock_alerts (
            alert_date,
            product_id,
            product_name,
            alert_type,
            current_stock,
            recommended_action,
            priority
        )
        SELECT 
            CURRENT_DATE,
            product_id,
            product_name,
            CASE 
                WHEN stock_status = 'CRITICAL' THEN 'CRITICAL_LOW_STOCK'
                WHEN stock_status = 'LOW' THEN 'LOW_STOCK_WARNING'
                WHEN stock_status = 'EXCESS' THEN 'EXCESS_INVENTORY'
                WHEN days_of_supply < 7 THEN 'STOCKOUT_RISK'
            END as alert_type,
            current_stock,
            CASE 
                WHEN stock_status = 'CRITICAL' THEN 'URGENT: Place emergency order for ' || (reorder_point * 2) || ' units'
                WHEN stock_status = 'LOW' THEN 'Place standard reorder for ' || reorder_point || ' units'
                WHEN stock_status = 'EXCESS' THEN 'Consider promotional pricing to reduce excess inventory'
                WHEN days_of_supply < 7 THEN 'Monitor closely - potential stockout in ' || ROUND(days_of_supply, 1) || ' days'
            END as recommended_action,
            CASE 
                WHEN stock_status = 'CRITICAL' THEN 'HIGH'
                WHEN stock_status = 'LOW' OR days_of_supply < 7 THEN 'MEDIUM'
                WHEN stock_status = 'EXCESS' THEN 'LOW'
            END as priority
        FROM daily_inventory_status
        WHERE status_date = CURRENT_DATE
          AND stock_status IN ('CRITICAL', 'LOW', 'EXCESS')
           OR days_of_supply < 7;
        """,
    )
    
    # Task 7: Validate results
    @task(task_id='validate_results')
    def validate_results(data_status):
        """Validate inventory management results"""
        if data_status == 'no_data':
            logging.info("Skipping validation - no data")
            return 'success'
        
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get inventory status summary
        status_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            stock_status,
            COUNT(*) as product_count,
            SUM(CASE WHEN reorder_needed THEN 1 ELSE 0 END) as reorder_count
        FROM daily_inventory_status
        WHERE status_date = CURRENT_DATE
        GROUP BY stock_status
        ORDER BY 
            CASE stock_status 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'LOW' THEN 2 
                WHEN 'NORMAL' THEN 3 
                ELSE 4 
            END;
        """
        
        status_results = hook.get_records(status_sql)
        
        # Get alerts summary
        alerts_sql = """
        SELECT 
            priority,
            COUNT(*) as alert_count
        FROM stock_alerts
        WHERE alert_date = CURRENT_DATE
        GROUP BY priority
        ORDER BY CASE priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;
        """
        
        alerts_results = hook.get_records(alerts_sql)
        
        logging.info("=== INVENTORY STATUS SUMMARY ===")
        total_products = sum(r[1] for r in status_results)
        total_reorders = sum(r[2] for r in status_results)
        
        for status, count, reorders in status_results:
            logging.info(f"{status}: {count} products ({reorders} need reorder)")
        
        logging.info(f"Total products monitored: {total_products}")
        logging.info(f"Products requiring reorder: {total_reorders}")
        
        logging.info("=== STOCK ALERTS ===")
        for priority, count in alerts_results:
            logging.info(f"{priority} priority alerts: {count}")
        
        # Log critical alerts
        if any(r[0] == 'CRITICAL' for r in status_results):
            critical_count = next(r[1] for r in status_results if r[0] == 'CRITICAL')
            logging.warning(f"ATTENTION: {critical_count} products at critical stock levels!")
        
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
            'inventory_management',
            'SUCCESS',
            COALESCE(COUNT(*), 0)
        FROM daily_inventory_status
        WHERE status_date = CURRENT_DATE;
        """,
    )
    
    # Define dependencies
    data_check = check_data_exists()
    validation = validate_results(data_check)
    
    create_target_tables >> data_check >> cleanup_data
    cleanup_data >> calculate_inventory_status
    calculate_inventory_status >> [track_inventory_movements, generate_stock_alerts]
    [track_inventory_movements, generate_stock_alerts] >> validation >> finalize

inventory_management()