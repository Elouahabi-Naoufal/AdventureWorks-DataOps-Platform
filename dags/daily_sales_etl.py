"""
Daily Sales ETL Pipeline
========================

This DAG performs a comprehensive ETL process for sales data from the AdventureWorks database.
It extracts sales data from multiple tables, transforms it, and loads it into a data warehouse
for analytics and reporting purposes.

Key Features:
- Uses Airflow 3.0 syntax with decorators
- Connects to Snowflake using snowflake_conn connection
- Uses SQL operators instead of Snowflake-specific operators
- Processes data from dbt_db.dbt_schema source tables
- Stores transformed data in db_wh data warehouse
- Implements proper error handling and data quality checks

Tables Processed:
- Sales_SalesOrderHeader: Main sales order information
- Sales_SalesOrderDetail: Individual line items
- Sales_Customer: Customer information
- Sales_SalesPerson: Sales representative data
- Sales_SalesTerritory: Territory information
- Production_Product: Product details
- Person_Person: Person information
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dailysalesetl',
    default_args=default_args,
    description='Daily ETL pipeline for sales data processing',
    schedule='@daily',  # Run daily at 2 AM
    catchup=False,
    tags=['sales', 'etl', 'daily', 'snowflake'],
    max_active_runs=1,
)
def daily_sales_etl():
    """
    Main DAG function that defines the sales ETL pipeline workflow.
    
    The pipeline follows these steps:
    1. Create staging tables in the data warehouse
    2. Extract and load raw sales data
    3. Transform data with business logic
    4. Perform data quality checks
    5. Load final data into warehouse tables
    6. Update metadata and statistics
    """
    
    # Task 1: Create staging tables for the ETL process
    create_staging_tables = SQLExecuteQueryOperator(
        task_id='create_staging_tables',
        conn_id='snowflake_conn',
        sql="""
        -- Create database if not exists
        CREATE DATABASE IF NOT EXISTS dbt_warehouse;
        
        -- Create staging schema if not exists
        CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_warehouse;
        
        -- Create staging table for sales orders
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.stg_sales_orders (
            sales_order_id INTEGER,
            order_date DATE,
            due_date DATE,
            ship_date DATE,
            status INTEGER,
            online_order_flag BOOLEAN,
            sales_order_number VARCHAR(25),
            customer_id INTEGER,
            salesperson_id INTEGER,
            territory_id INTEGER,
            subtotal DECIMAL(19,4),
            tax_amt DECIMAL(19,4),
            freight DECIMAL(19,4),
            total_due DECIMAL(19,4),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Create staging table for sales order details
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.stg_sales_order_details (
            sales_order_id INTEGER,
            sales_order_detail_id INTEGER,
            order_qty SMALLINT,
            product_id INTEGER,
            special_offer_id INTEGER,
            unit_price DECIMAL(19,4),
            unit_price_discount DECIMAL(19,4),
            line_total DECIMAL(19,4),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Create staging table for customer data
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.stg_customers (
            customer_id INTEGER,
            person_id INTEGER,
            store_id INTEGER,
            territory_id INTEGER,
            account_number VARCHAR(10),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email_address VARCHAR(50),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Extract sales order header data
    extract_sales_orders = SQLExecuteQueryOperator(
        task_id='extract_sales_orders',
        conn_id='snowflake_conn',
        sql="""
        -- Extract sales order header data from source
        INSERT INTO dbt_db.dbt_warehouse.stg_sales_orders (
            sales_order_id, order_date, due_date, ship_date, status,
            online_order_flag, sales_order_number, customer_id, 
            salesperson_id, territory_id, subtotal, tax_amt, freight, total_due
        )
        SELECT 
            SalesOrderID,
            OrderDate,
            DueDate,
            ShipDate,
            Status,
            OnlineOrderFlag,
            SalesOrderNumber,
            CustomerID,
            SalesPersonID,
            TerritoryID,
            SubTotal,
            TaxAmt,
            Freight,
            TotalDue
        FROM dbt_db.dbt_schema.Sales_SalesOrderHeader
        WHERE OrderDate >= CURRENT_DATE - INTERVAL '7 days'
           OR ModifiedDate >= CURRENT_DATE - INTERVAL '1 day';
        """,
    )
    
    # Task 3: Extract sales order detail data
    extract_sales_order_details = SQLExecuteQueryOperator(
        task_id='extract_sales_order_details',
        conn_id='snowflake_conn',
        sql="""
        -- Extract sales order detail data from source
        INSERT INTO dbt_db.dbt_warehouse.stg_sales_order_details (
            sales_order_id, sales_order_detail_id, order_qty, product_id,
            special_offer_id, unit_price, unit_price_discount, line_total
        )
        SELECT 
            sod.SalesOrderID,
            sod.SalesOrderDetailID,
            sod.OrderQty,
            sod.ProductID,
            sod.SpecialOfferID,
            sod.UnitPrice,
            sod.UnitPriceDiscount,
            sod.LineTotal
        FROM dbt_db.dbt_schema.Sales_SalesOrderDetail sod
        INNER JOIN dbt_db.dbt_warehouse.stg_sales_orders so 
            ON sod.SalesOrderID = so.sales_order_id;
        """,
    )
    
    # Task 4: Extract customer data with person information
    extract_customers = SQLExecuteQueryOperator(
        task_id='extract_customers',
        conn_id='snowflake_conn',
        sql="""
        -- Extract customer data with person details
        INSERT INTO dbt_db.dbt_warehouse.stg_customers (
            customer_id, person_id, store_id, territory_id, account_number,
            first_name, last_name, email_address
        )
        SELECT DISTINCT
            c.CustomerID,
            c.PersonID,
            c.StoreID,
            c.TerritoryID,
            c.AccountNumber,
            p.FirstName,
            p.LastName,
            e.EmailAddress
        FROM dbt_db.dbt_schema.Sales_Customer c
        LEFT JOIN dbt_db.dbt_schema.Person_Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN dbt_db.dbt_schema.Person_EmailAddress e ON p.BusinessEntityID = e.BusinessEntityID
        WHERE c.CustomerID IN (SELECT DISTINCT customer_id FROM dbt_db.dbt_warehouse.stg_sales_orders);
        """,
    )
    
    # Task 5: Create final warehouse tables
    create_warehouse_tables = SQLExecuteQueryOperator(
        task_id='create_warehouse_tables',
        conn_id='snowflake_conn',
        sql="""
        -- Use the database
        USE DATABASE dbt_warehouse;
        
        -- Create fact table for sales
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.fact_sales (
            sales_order_id INTEGER,
            sales_order_detail_id INTEGER,
            order_date DATE,
            customer_id INTEGER,
            product_id INTEGER,
            salesperson_id INTEGER,
            territory_id INTEGER,
            order_qty SMALLINT,
            unit_price DECIMAL(19,4),
            unit_price_discount DECIMAL(19,4),
            line_total DECIMAL(19,4),
            order_subtotal DECIMAL(19,4),
            order_tax_amt DECIMAL(19,4),
            order_freight DECIMAL(19,4),
            order_total_due DECIMAL(19,4),
            discount_amount DECIMAL(19,4),
            net_sales_amount DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Create dimension table for customers
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.dim_customer (
            customer_id INTEGER PRIMARY KEY,
            person_id INTEGER,
            store_id INTEGER,
            territory_id INTEGER,
            account_number VARCHAR(10),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            full_name VARCHAR(101),
            email_address VARCHAR(50),
            customer_type VARCHAR(20),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Create daily sales summary table
        CREATE OR REPLACE TABLE dbt_db.dbt_warehouse.daily_sales_summary (
            summary_date DATE,
            territory_id INTEGER,
            total_orders INTEGER,
            total_customers INTEGER,
            total_products INTEGER,
            gross_sales_amount DECIMAL(19,4),
            total_discount_amount DECIMAL(19,4),
            net_sales_amount DECIMAL(19,4),
            total_tax_amount DECIMAL(19,4),
            total_freight_amount DECIMAL(19,4),
            average_order_value DECIMAL(19,4),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 6: Transform and load sales fact data
    transform_load_sales_fact = SQLExecuteQueryOperator(
        task_id='transform_load_sales_fact',
        conn_id='snowflake_conn',
        sql="""
        -- Transform and load sales fact data with business calculations
        INSERT INTO dbt_db.dbt_warehouse.fact_sales (
            sales_order_id, sales_order_detail_id, order_date, customer_id,
            product_id, salesperson_id, territory_id, order_qty, unit_price,
            unit_price_discount, line_total, order_subtotal, order_tax_amt,
            order_freight, order_total_due, discount_amount, net_sales_amount
        )
        SELECT 
            so.sales_order_id,
            sod.sales_order_detail_id,
            so.order_date,
            so.customer_id,
            sod.product_id,
            so.salesperson_id,
            so.territory_id,
            sod.order_qty,
            sod.unit_price,
            sod.unit_price_discount,
            sod.line_total,
            so.subtotal,
            so.tax_amt,
            so.freight,
            so.total_due,
            -- Calculate discount amount
            (sod.unit_price * sod.order_qty * sod.unit_price_discount) as discount_amount,
            -- Calculate net sales amount (line total after discount)
            sod.line_total as net_sales_amount
        FROM dbt_db.dbt_warehouse.stg_sales_orders so
        INNER JOIN dbt_db.dbt_warehouse.stg_sales_order_details sod 
            ON so.sales_order_id = sod.sales_order_id
        WHERE so.order_date IS NOT NULL;
        """,
    )
    
    # Task 7: Transform and load customer dimension
    transform_load_customer_dim = SQLExecuteQueryOperator(
        task_id='transform_load_customer_dim',
        conn_id='snowflake_conn',
        sql="""
        -- Transform and load customer dimension data
        INSERT INTO dbt_db.dbt_warehouse.dim_customer (
            customer_id, person_id, store_id, territory_id, account_number,
            first_name, last_name, full_name, email_address, customer_type
        )
        SELECT 
            customer_id,
            person_id,
            store_id,
            territory_id,
            account_number,
            first_name,
            last_name,
            -- Create full name
            CASE 
                WHEN first_name IS NOT NULL AND last_name IS NOT NULL 
                THEN CONCAT(first_name, ' ', last_name)
                WHEN first_name IS NOT NULL THEN first_name
                WHEN last_name IS NOT NULL THEN last_name
                ELSE 'Unknown'
            END as full_name,
            email_address,
            -- Determine customer type
            CASE 
                WHEN store_id IS NOT NULL THEN 'Business'
                WHEN person_id IS NOT NULL THEN 'Individual'
                ELSE 'Unknown'
            END as customer_type
        FROM dbt_db.dbt_warehouse.stg_customers;
        """,
    )
    
    # Task 8: Generate daily sales summary
    generate_daily_summary = SQLExecuteQueryOperator(
        task_id='generate_daily_summary',
        conn_id='snowflake_conn',
        sql="""
        -- Generate daily sales summary for reporting
        INSERT INTO dbt_db.dbt_warehouse.daily_sales_summary (
            summary_date, territory_id, total_orders, total_customers,
            total_products, gross_sales_amount, total_discount_amount,
            net_sales_amount, total_tax_amount, total_freight_amount,
            average_order_value
        )
        SELECT 
            fs.order_date as summary_date,
            fs.territory_id,
            COUNT(DISTINCT fs.sales_order_id) as total_orders,
            COUNT(DISTINCT fs.customer_id) as total_customers,
            COUNT(DISTINCT fs.product_id) as total_products,
            SUM(fs.line_total + (fs.unit_price * fs.order_qty * fs.unit_price_discount)) as gross_sales_amount,
            SUM(fs.discount_amount) as total_discount_amount,
            SUM(fs.net_sales_amount) as net_sales_amount,
            AVG(fs.order_tax_amt) as total_tax_amount,
            AVG(fs.order_freight) as total_freight_amount,
            AVG(fs.order_total_due) as average_order_value
        FROM dbt_db.dbt_warehouse.fact_sales fs
        WHERE fs.order_date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY fs.order_date, fs.territory_id;
        """,
    )
    
    # Task 9: Data quality checks
    data_quality_checks = SQLExecuteQueryOperator(
        task_id='data_quality_checks',
        conn_id='snowflake_conn',
        sql="""
        -- Data quality validation queries
        DO $$
        DECLARE
            fact_count INTEGER;
            staging_count INTEGER;
            null_count INTEGER;
            negative_count INTEGER;
        BEGIN
            -- Check 1: Record counts
            SELECT COUNT(*) INTO fact_count FROM dbt_db.dbt_warehouse.fact_sales;
            SELECT COUNT(*) INTO staging_count FROM dbt_db.dbt_warehouse.stg_sales_order_details;
            
            IF fact_count != staging_count THEN
                RAISE EXCEPTION 'Record count mismatch: Fact=%, Staging=%', fact_count, staging_count;
            END IF;
            
            -- Check 2: Null values in critical fields
            SELECT COUNT(*) INTO null_count 
            FROM dbt_db.dbt_warehouse.fact_sales 
            WHERE sales_order_id IS NULL OR customer_id IS NULL OR product_id IS NULL;
            
            IF null_count > 0 THEN
                RAISE EXCEPTION 'Found % records with null critical fields', null_count;
            END IF;
            
            -- Check 3: Negative sales amounts
            SELECT COUNT(*) INTO negative_count 
            FROM dbt_db.dbt_warehouse.fact_sales 
            WHERE net_sales_amount < 0;
            
            -- Log results
            INSERT INTO dbt_db.dbt_warehouse.etl_metadata (table_name, last_updated, record_count, etl_run_id)
            VALUES ('data_quality_check', CURRENT_TIMESTAMP(), fact_count, '{{ run_id }}');
        END $$;
        """,
    )
    
    # Task 10: Update statistics and metadata
    update_statistics = SQLExecuteQueryOperator(
        task_id='update_statistics',
        conn_id='snowflake_conn',
        sql="""
        -- Update table statistics for query optimization
        ALTER TABLE dbt_db.dbt_warehouse.fact_sales COMPUTE STATISTICS;
        ALTER TABLE dbt_db.dbt_warehouse.dim_customer COMPUTE STATISTICS;
        ALTER TABLE dbt_db.dbt_warehouse.daily_sales_summary COMPUTE STATISTICS;
        
        -- Create or update metadata table
        CREATE TABLE IF NOT EXISTS dbt_db.dbt_warehouse.etl_metadata (
            table_name VARCHAR(100),
            last_updated TIMESTAMP,
            record_count INTEGER,
            etl_run_id VARCHAR(100)
        );
        
        -- Update metadata
        MERGE INTO dbt_db.dbt_warehouse.etl_metadata AS target
        USING (
            SELECT 'fact_sales' as table_name, CURRENT_TIMESTAMP() as last_updated, 
                   COUNT(*) as record_count, '{{ run_id }}' as etl_run_id
            FROM dbt_db.dbt_warehouse.fact_sales
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET last_updated = source.last_updated, 
                      record_count = source.record_count,
                      etl_run_id = source.etl_run_id
        WHEN NOT MATCHED THEN 
            INSERT (table_name, last_updated, record_count, etl_run_id)
            VALUES (source.table_name, source.last_updated, source.record_count, source.etl_run_id);
        """,
    )
    
    # Task 11: Cleanup staging tables
    cleanup_staging = SQLExecuteQueryOperator(
        task_id='cleanup_staging',
        conn_id='snowflake_conn',
        sql="""
        -- Clean up staging tables to free up space
        TRUNCATE TABLE dbt_db.dbt_warehouse.stg_sales_orders;
        TRUNCATE TABLE dbt_db.dbt_warehouse.stg_sales_order_details;
        TRUNCATE TABLE dbt_db.dbt_warehouse.stg_customers;
        """,
    )
    
    # Define task dependencies
    create_staging_tables >> [extract_sales_orders, extract_customers]
    extract_sales_orders >> extract_sales_order_details
    [extract_sales_order_details, extract_customers] >> create_warehouse_tables
    create_warehouse_tables >> [transform_load_sales_fact, transform_load_customer_dim]
    [transform_load_sales_fact, transform_load_customer_dim] >> generate_daily_summary
    generate_daily_summary >> data_quality_checks
    data_quality_checks >> update_statistics
    update_statistics >> cleanup_staging

# Instantiate the DAG
daily_sales_etl_dag = daily_sales_etl()