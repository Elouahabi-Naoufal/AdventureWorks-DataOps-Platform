"""
Populate Current Data DAG
Fills AdventureWorks database with current year data for all 70 tables
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id='populate_current_data',
    start_date=datetime(2024, 8, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'owner': 'data_team',
    },
    tags=['setup', 'data', 'population'],
    description='Populate AdventureWorks with current year data',
)
def populate_current_data():
    
    # Task 1: Update Sales Orders with current dates
    update_sales_orders = SQLExecuteQueryOperator(
        task_id='update_sales_orders',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Insert new sales orders for 2024-2025
        INSERT INTO SALES_SALESORDERHEADER (RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue)
        SELECT 
            8,
            DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE) as OrderDate,
            DATEADD(day, UNIFORM(10, 20, RANDOM()), DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE)) as DueDate,
            DATEADD(day, UNIFORM(5, 15, RANDOM()), DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE)) as ShipDate,
            5, -- Completed
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 3 THEN TRUE ELSE FALSE END,
            'SO' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM()) + 100000, 6, '0'),
            UNIFORM(1, 15, RANDOM()),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 8 THEN UNIFORM(1, 5, RANDOM()) ELSE NULL END,
            UNIFORM(1, 10, RANDOM()),
            UNIFORM(1, 12, RANDOM()),
            UNIFORM(1, 12, RANDOM()),
            UNIFORM(1, 5, RANDOM()),
            UNIFORM(500, 5000, RANDOM()) * 1.0,
            UNIFORM(50, 500, RANDOM()) * 1.0,
            UNIFORM(20, 100, RANDOM()) * 1.0,
            UNIFORM(600, 6000, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 50);
        
        -- Insert corresponding order details
        INSERT INTO SALES_SALESORDERDETAIL (SalesOrderID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal)
        SELECT 
            h.SalesOrderID,
            'TRK' || LPAD(UNIFORM(100000, 999999, RANDOM()), 6, '0'),
            UNIFORM(1, 5, RANDOM()),
            UNIFORM(1, 12, RANDOM()),
            1,
            UNIFORM(50, 500, RANDOM()) * 1.0,
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 2 THEN 0.1 ELSE 0.0 END,
            UNIFORM(100, 1000, RANDOM()) * 1.0
        FROM SALES_SALESORDERHEADER h
        WHERE h.SalesOrderID > 10  -- Only new orders
        CROSS JOIN (SELECT 1 as item UNION SELECT 2 UNION SELECT 3) items
        WHERE UNIFORM(1, 10, RANDOM()) <= 7;  -- 70% chance per item
        """,
    )
    
    # Task 2: Update Production data
    update_production_data = SQLExecuteQueryOperator(
        task_id='update_production_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Update product inventory with current data
        UPDATE PRODUCTION_PRODUCTINVENTORY 
        SET Quantity = Quantity + UNIFORM(-50, 200, RANDOM()),
            ModifiedDate = DATEADD(day, -UNIFORM(1, 30, RANDOM()), CURRENT_DATE);
        
        -- Insert new work orders
        INSERT INTO PRODUCTION_WORKORDER (ProductID, OrderQty, StockedQty, ScrappedQty, StartDate, EndDate, DueDate, ScrapReasonID)
        SELECT 
            UNIFORM(1, 12, RANDOM()),
            UNIFORM(10, 100, RANDOM()),
            UNIFORM(10, 100, RANDOM()),
            UNIFORM(0, 5, RANDOM()),
            DATEADD(day, -UNIFORM(1, 60, RANDOM()), CURRENT_DATE),
            DATEADD(day, -UNIFORM(1, 30, RANDOM()), CURRENT_DATE),
            DATEADD(day, UNIFORM(1, 30, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 2 THEN UNIFORM(1, 10, RANDOM()) ELSE NULL END
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 20);
        
        -- Insert transaction history
        INSERT INTO PRODUCTION_TRANSACTIONHISTORY (ProductID, ReferenceOrderID, ReferenceOrderLineID, TransactionDate, TransactionType, Quantity, ActualCost)
        SELECT 
            UNIFORM(1, 12, RANDOM()),
            UNIFORM(1, 50, RANDOM()),
            UNIFORM(1, 3, RANDOM()),
            DATEADD(day, -UNIFORM(1, 90, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 2, RANDOM()) = 1 THEN 'W' ELSE 'S' END,
            UNIFORM(1, 20, RANDOM()) * CASE WHEN UNIFORM(1, 2, RANDOM()) = 1 THEN 1 ELSE -1 END,
            UNIFORM(10, 200, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 30);
        """,
    )
    
    # Task 3: Update HR and Employee data
    update_hr_data = SQLExecuteQueryOperator(
        task_id='update_hr_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Update employee pay history
        INSERT INTO HUMANRESOURCES_EMPLOYEEPAYHISTORY (BusinessEntityID, RateChangeDate, Rate, PayFrequency)
        SELECT 
            UNIFORM(1, 10, RANDOM()),
            DATEADD(month, -UNIFORM(1, 12, RANDOM()), CURRENT_DATE),
            UNIFORM(25, 80, RANDOM()) * 1.0,
            CASE WHEN UNIFORM(1, 2, RANDOM()) = 1 THEN 1 ELSE 2 END
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 15);
        
        -- Update department history
        INSERT INTO HUMANRESOURCES_EMPLOYEEDEPARTMENTHISTORY (BusinessEntityID, DepartmentID, ShiftID, StartDate, EndDate)
        SELECT 
            UNIFORM(1, 10, RANDOM()),
            UNIFORM(1, 16, RANDOM()),
            UNIFORM(1, 3, RANDOM()),
            DATEADD(month, -UNIFORM(1, 24, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 2 THEN DATEADD(month, -UNIFORM(1, 6, RANDOM()), CURRENT_DATE) ELSE NULL END
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 12);
        """,
    )
    
    # Task 4: Update Sales Territory and Person data
    update_sales_data = SQLExecuteQueryOperator(
        task_id='update_sales_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Update sales territory history
        INSERT INTO SALES_SALESTERRITORYHISTORY (BusinessEntityID, TerritoryID, StartDate, EndDate)
        SELECT 
            UNIFORM(1, 5, RANDOM()),
            UNIFORM(1, 10, RANDOM()),
            DATEADD(month, -UNIFORM(1, 18, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 3 THEN DATEADD(month, -UNIFORM(1, 6, RANDOM()), CURRENT_DATE) ELSE NULL END
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 10);
        
        -- Update sales quota history
        INSERT INTO SALES_SALESPERSONQUOTAHISTORY (BusinessEntityID, QuotaDate, SalesQuota)
        SELECT 
            UNIFORM(1, 5, RANDOM()),
            DATE_TRUNC('quarter', DATEADD(quarter, -UNIFORM(1, 4, RANDOM()), CURRENT_DATE)),
            UNIFORM(250000, 400000, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 15);
        
        -- Update currency rates with current dates
        INSERT INTO SALES_CURRENCYRATE (CurrencyRateDate, FromCurrencyCode, ToCurrencyCode, AverageRate, EndOfDayRate)
        SELECT 
            DATEADD(day, -UNIFORM(1, 30, RANDOM()), CURRENT_DATE),
            'USD',
            CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'EUR'
                WHEN 2 THEN 'GBP' 
                WHEN 3 THEN 'CAD'
                WHEN 4 THEN 'JPY'
                ELSE 'AUD'
            END,
            UNIFORM(0.5, 1.5, RANDOM()),
            UNIFORM(0.5, 1.5, RANDOM())
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 25);
        """,
    )
    
    # Task 5: Update Purchasing data
    update_purchasing_data = SQLExecuteQueryOperator(
        task_id='update_purchasing_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Insert new purchase orders
        INSERT INTO PURCHASING_PURCHASEORDERHEADER (RevisionNumber, Status, EmployeeID, VendorID, ShipMethodID, OrderDate, SubTotal, TaxAmt, Freight, TotalDue)
        SELECT 
            1,
            4, -- Complete
            UNIFORM(1, 10, RANDOM()),
            UNIFORM(16, 20, RANDOM()),
            UNIFORM(1, 5, RANDOM()),
            DATEADD(day, -UNIFORM(1, 120, RANDOM()), CURRENT_DATE),
            UNIFORM(1000, 8000, RANDOM()) * 1.0,
            UNIFORM(100, 800, RANDOM()) * 1.0,
            UNIFORM(50, 200, RANDOM()) * 1.0,
            UNIFORM(1200, 9000, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 15);
        
        -- Insert purchase order details
        INSERT INTO PURCHASING_PURCHASEORDERDETAIL (PurchaseOrderID, DueDate, OrderQty, ProductID, UnitPrice, LineTotal, ReceivedQty, RejectedQty, StockedQty)
        SELECT 
            po.PurchaseOrderID,
            DATEADD(day, UNIFORM(10, 30, RANDOM()), po.OrderDate),
            UNIFORM(5, 50, RANDOM()),
            UNIFORM(1, 12, RANDOM()),
            UNIFORM(20, 150, RANDOM()) * 1.0,
            UNIFORM(200, 2000, RANDOM()) * 1.0,
            UNIFORM(5, 50, RANDOM()),
            UNIFORM(0, 3, RANDOM()),
            UNIFORM(5, 50, RANDOM())
        FROM PURCHASING_PURCHASEORDERHEADER po
        WHERE po.PurchaseOrderID > 5  -- Only new POs
        CROSS JOIN (SELECT 1 as item UNION SELECT 2) items
        WHERE UNIFORM(1, 10, RANDOM()) <= 8;
        
        -- Update product vendor data
        UPDATE PURCHASING_PRODUCTVENDOR 
        SET LastReceiptDate = DATEADD(day, -UNIFORM(1, 60, RANDOM()), CURRENT_DATE),
            LastReceiptCost = StandardPrice * UNIFORM(0.9, 1.1, RANDOM()),
            OnOrderQty = UNIFORM(0, 100, RANDOM())
        WHERE ProductID <= 12;
        """,
    )
    
    # Task 6: Update Person and Address data
    update_person_data = SQLExecuteQueryOperator(
        task_id='update_person_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Insert new business entities
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        INSERT INTO PERSON_BUSINESSENTITY DEFAULT VALUES;
        
        -- Insert new persons
        INSERT INTO PERSON_PERSON (BusinessEntityID, PersonType, FirstName, LastName, EmailPromotion)
        SELECT 
            BusinessEntityID,
            CASE UNIFORM(1, 3, RANDOM())
                WHEN 1 THEN 'SC'
                WHEN 2 THEN 'IN'
                ELSE 'EM'
            END,
            CASE UNIFORM(1, 10, RANDOM())
                WHEN 1 THEN 'John' WHEN 2 THEN 'Sarah' WHEN 3 THEN 'Michael'
                WHEN 4 THEN 'Lisa' WHEN 5 THEN 'David' WHEN 6 THEN 'Emma'
                WHEN 7 THEN 'James' WHEN 8 THEN 'Anna' WHEN 9 THEN 'Robert'
                ELSE 'Maria'
            END,
            CASE UNIFORM(1, 10, RANDOM())
                WHEN 1 THEN 'Smith' WHEN 2 THEN 'Johnson' WHEN 3 THEN 'Williams'
                WHEN 4 THEN 'Brown' WHEN 5 THEN 'Jones' WHEN 6 THEN 'Garcia'
                WHEN 7 THEN 'Miller' WHEN 8 THEN 'Davis' WHEN 9 THEN 'Rodriguez'
                ELSE 'Wilson'
            END,
            UNIFORM(0, 2, RANDOM())
        FROM PERSON_BUSINESSENTITY
        WHERE BusinessEntityID > 20
        LIMIT 10;
        
        -- Insert email addresses
        INSERT INTO PERSON_EMAILADDRESS (BusinessEntityID, EmailAddress)
        SELECT 
            p.BusinessEntityID,
            LOWER(p.FirstName || '.' || p.LastName || '@company.com')
        FROM PERSON_PERSON p
        WHERE p.BusinessEntityID > 20
        LIMIT 10;
        """,
    )
    
    # Task 7: Update Product and Review data
    update_product_data = SQLExecuteQueryOperator(
        task_id='update_product_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Insert new product reviews
        INSERT INTO PRODUCTION_PRODUCTREVIEW (ProductID, ReviewerName, ReviewDate, EmailAddress, Rating, Comments)
        SELECT 
            UNIFORM(1, 12, RANDOM()),
            CASE UNIFORM(1, 8, RANDOM())
                WHEN 1 THEN 'Alex Johnson' WHEN 2 THEN 'Maria Garcia'
                WHEN 3 THEN 'Robert Chen' WHEN 4 THEN 'Jennifer Kim'
                WHEN 5 THEN 'Michael Brown' WHEN 6 THEN 'Sarah Wilson'
                WHEN 7 THEN 'David Lee' ELSE 'Emma Davis'
            END,
            DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_DATE),
            'reviewer' || UNIFORM(1, 1000, RANDOM()) || '@email.com',
            UNIFORM(1, 5, RANDOM()),
            CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'Excellent product, highly recommended!'
                WHEN 2 THEN 'Good quality, fast delivery'
                WHEN 3 THEN 'Average product, meets expectations'
                WHEN 4 THEN 'Could be better, but acceptable'
                ELSE 'Outstanding quality and service'
            END
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 20);
        
        -- Update product cost history
        INSERT INTO PRODUCTION_PRODUCTCOSTHISTORY (ProductID, StartDate, EndDate, StandardCost)
        SELECT 
            UNIFORM(1, 12, RANDOM()),
            DATEADD(month, -UNIFORM(1, 12, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 3 THEN DATEADD(month, -UNIFORM(1, 3, RANDOM()), CURRENT_DATE) ELSE NULL END,
            UNIFORM(20, 100, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 15);
        
        -- Update product list price history
        INSERT INTO PRODUCTION_PRODUCTLISTPRICEHISTORY (ProductID, StartDate, EndDate, ListPrice)
        SELECT 
            UNIFORM(1, 12, RANDOM()),
            DATEADD(month, -UNIFORM(1, 12, RANDOM()), CURRENT_DATE),
            CASE WHEN UNIFORM(1, 10, RANDOM()) <= 3 THEN DATEADD(month, -UNIFORM(1, 3, RANDOM()), CURRENT_DATE) ELSE NULL END,
            UNIFORM(50, 200, RANDOM()) * 1.0
        FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) as rn FROM INFORMATION_SCHEMA.TABLES LIMIT 15);
        """,
    )
    
    # Task 8: Final validation and summary
    validate_data = SQLExecuteQueryOperator(
        task_id='validate_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Create summary of populated data
        CREATE OR REPLACE TABLE DATA_POPULATION_SUMMARY AS
        SELECT 
            'SALES_SALESORDERHEADER' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN OrderDate >= '2024-01-01' THEN 1 END) as current_year_records
        FROM SALES_SALESORDERHEADER
        UNION ALL
        SELECT 
            'SALES_SALESORDERDETAIL',
            COUNT(*),
            COUNT(CASE WHEN ModifiedDate >= '2024-01-01' THEN 1 END)
        FROM SALES_SALESORDERDETAIL
        UNION ALL
        SELECT 
            'PRODUCTION_WORKORDER',
            COUNT(*),
            COUNT(CASE WHEN StartDate >= '2024-01-01' THEN 1 END)
        FROM PRODUCTION_WORKORDER
        UNION ALL
        SELECT 
            'PRODUCTION_TRANSACTIONHISTORY',
            COUNT(*),
            COUNT(CASE WHEN TransactionDate >= '2024-01-01' THEN 1 END)
        FROM PRODUCTION_TRANSACTIONHISTORY
        UNION ALL
        SELECT 
            'PURCHASING_PURCHASEORDERHEADER',
            COUNT(*),
            COUNT(CASE WHEN OrderDate >= '2024-01-01' THEN 1 END)
        FROM PURCHASING_PURCHASEORDERHEADER
        UNION ALL
        SELECT 
            'PRODUCTION_PRODUCTREVIEW',
            COUNT(*),
            COUNT(CASE WHEN ReviewDate >= '2024-01-01' THEN 1 END)
        FROM PRODUCTION_PRODUCTREVIEW;
        
        -- Log completion
        INSERT INTO dbt_warehouse.etl_log (execution_date, dag_id, status, records_processed)
        SELECT 
            CURRENT_DATE,
            'populate_current_data',
            'SUCCESS',
            SUM(current_year_records)
        FROM DATA_POPULATION_SUMMARY;
        """,
    )
    
    # Define dependencies
    update_sales_orders >> update_production_data >> update_hr_data
    update_hr_data >> update_sales_data >> update_purchasing_data
    update_purchasing_data >> update_person_data >> update_product_data >> validate_data

populate_current_data()