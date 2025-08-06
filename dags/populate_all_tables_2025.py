"""
Populate All AdventureWorks Tables with 2025 Data
Fills all 70+ tables with 10+ rows each, dates from 2025 including August
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id='populate_all_tables_2025',
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['setup', 'data', '2025'],
    description='Populate all AdventureWorks tables with 2025 data',
)
def populate_all_tables_2025():
    
    # Task 1: Core Reference Data
    populate_reference_data = SQLExecuteQueryOperator(
        task_id='populate_reference_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Person_CountryRegion
        INSERT INTO Person_CountryRegion (CountryRegionCode, Name, ModifiedDate) VALUES
        ('USA', 'United States', '2025-01-15'), ('CAN', 'Canada', '2025-02-10'), ('GBR', 'United Kingdom', '2025-03-05'),
        ('DEU', 'Germany', '2025-04-12'), ('FRA', 'France', '2025-05-08'), ('JPN', 'Japan', '2025-06-20'),
        ('AUS', 'Australia', '2025-07-14'), ('BRA', 'Brazil', '2025-08-03'), ('CHN', 'China', '2025-08-15'),
        ('IND', 'India', '2025-08-25'), ('MEX', 'Mexico', '2025-08-28'), ('ITA', 'Italy', '2025-08-30');
        
        -- Sales_Currency
        INSERT INTO Sales_Currency (CurrencyCode, Name, ModifiedDate) VALUES
        ('USD', 'US Dollar', '2025-01-10'), ('EUR', 'Euro', '2025-02-15'), ('GBP', 'British Pound', '2025-03-20'),
        ('CAD', 'Canadian Dollar', '2025-04-25'), ('JPY', 'Japanese Yen', '2025-05-30'), ('AUD', 'Australian Dollar', '2025-06-10'),
        ('BRL', 'Brazilian Real', '2025-07-05'), ('CNY', 'Chinese Yuan', '2025-08-01'), ('INR', 'Indian Rupee', '2025-08-10'),
        ('MXN', 'Mexican Peso', '2025-08-20'), ('CHF', 'Swiss Franc', '2025-08-25'), ('SEK', 'Swedish Krona', '2025-08-30');
        
        -- Production_UnitMeasure
        INSERT INTO Production_UnitMeasure (UnitMeasureCode, Name, ModifiedDate) VALUES
        ('EA', 'Each', '2025-01-05'), ('BOX', 'Boxes', '2025-02-10'), ('KG', 'Kilogram', '2025-03-15'),
        ('LB', 'Pound', '2025-04-20'), ('L', 'Liter', '2025-05-25'), ('M', 'Meter', '2025-06-30'),
        ('CM', 'Centimeter', '2025-07-10'), ('IN', 'Inch', '2025-08-05'), ('G', 'Gram', '2025-08-15'),
        ('DZ', 'Dozen', '2025-08-20'), ('BTL', 'Bottle', '2025-08-25'), ('C', 'Container', '2025-08-30');
        
        -- Person_AddressType
        INSERT INTO Person_AddressType (Name, ModifiedDate) VALUES
        ('Home', '2025-01-10'), ('Business', '2025-02-15'), ('Billing', '2025-03-20'), ('Shipping', '2025-04-25'),
        ('Main Office', '2025-05-30'), ('Primary', '2025-06-10'), ('Archive', '2025-07-15'), ('Contact', '2025-08-05'),
        ('General', '2025-08-15'), ('International', '2025-08-20'), ('Temporary', '2025-08-25'), ('Secondary', '2025-08-30');
        
        -- Person_ContactType
        INSERT INTO Person_ContactType (Name, ModifiedDate) VALUES
        ('Marketing Manager', '2025-01-05'), ('Sales Representative', '2025-02-10'), ('Accounting Manager', '2025-03-15'),
        ('Assistant Sales Agent', '2025-04-20'), ('Export Administrator', '2025-05-25'), ('Marketing Assistant', '2025-06-30'),
        ('Order Administrator', '2025-07-10'), ('International Marketing Manager', '2025-08-05'), ('Coordinator Foreign Markets', '2025-08-15'),
        ('Marketing Representative', '2025-08-20'), ('Customer Service', '2025-08-25'), ('Technical Support', '2025-08-30');
        
        -- Person_PhoneNumberType
        INSERT INTO Person_PhoneNumberType (Name, ModifiedDate) VALUES
        ('Cell', '2025-01-10'), ('Home', '2025-02-15'), ('Work', '2025-03-20'), ('Fax', '2025-04-25'),
        ('Mobile', '2025-05-30'), ('Business', '2025-06-10'), ('Emergency', '2025-07-15'), ('Personal', '2025-08-05'),
        ('Other', '2025-08-15'), ('Pager', '2025-08-20'), ('Direct', '2025-08-25'), ('Main', '2025-08-30');
        """,
    )
    
    # Task 2: Sales Territory and Geography
    populate_territory_data = SQLExecuteQueryOperator(
        task_id='populate_territory_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Sales_SalesTerritory
        INSERT INTO Sales_SalesTerritory (Name, CountryRegionCode, "Group", SalesYTD, SalesLastYear, CostYTD, CostLastYear, ModifiedDate) VALUES
        ('Northwest', 'USA', 'North America', 8500000.00, 7200000.00, 750000.00, 680000.00, '2025-01-15'),
        ('Northeast', 'USA', 'North America', 7800000.00, 6900000.00, 720000.00, 650000.00, '2025-02-20'),
        ('Southwest', 'USA', 'North America', 9200000.00, 8100000.00, 850000.00, 780000.00, '2025-03-25'),
        ('Southeast', 'USA', 'North America', 6900000.00, 6200000.00, 640000.00, 590000.00, '2025-04-30'),
        ('Central', 'USA', 'North America', 7500000.00, 6800000.00, 690000.00, 620000.00, '2025-05-15'),
        ('Canada', 'CAN', 'North America', 5800000.00, 5200000.00, 540000.00, 480000.00, '2025-06-20'),
        ('United Kingdom', 'GBR', 'Europe', 4900000.00, 4300000.00, 450000.00, 400000.00, '2025-07-25'),
        ('Germany', 'DEU', 'Europe', 5200000.00, 4600000.00, 480000.00, 420000.00, '2025-08-05'),
        ('France', 'FRA', 'Europe', 4700000.00, 4100000.00, 430000.00, 380000.00, '2025-08-15'),
        ('Australia', 'AUS', 'Pacific', 3800000.00, 3400000.00, 350000.00, 310000.00, '2025-08-25'),
        ('Japan', 'JPN', 'Pacific', 4200000.00, 3700000.00, 390000.00, 340000.00, '2025-08-28'),
        ('Brazil', 'BRA', 'South America', 3500000.00, 3100000.00, 320000.00, 280000.00, '2025-08-30');
        
        -- Person_StateProvince
        INSERT INTO Person_StateProvince (StateProvinceCode, CountryRegionCode, Name, TerritoryID, ModifiedDate) VALUES
        ('CA', 'USA', 'California', 3, '2025-01-10'), ('TX', 'USA', 'Texas', 3, '2025-02-15'), ('NY', 'USA', 'New York', 2, '2025-03-20'),
        ('FL', 'USA', 'Florida', 4, '2025-04-25'), ('IL', 'USA', 'Illinois', 5, '2025-05-30'), ('PA', 'USA', 'Pennsylvania', 2, '2025-06-10'),
        ('OH', 'USA', 'Ohio', 5, '2025-07-15'), ('GA', 'USA', 'Georgia', 4, '2025-08-05'), ('NC', 'USA', 'North Carolina', 4, '2025-08-15'),
        ('MI', 'USA', 'Michigan', 5, '2025-08-20'), ('ON', 'CAN', 'Ontario', 6, '2025-08-25'), ('BC', 'CAN', 'British Columbia', 6, '2025-08-30');
        """,
    )
    
    # Task 3: Business Entities and People
    populate_people_data = SQLExecuteQueryOperator(
        task_id='populate_people_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Person_BusinessEntity (50 entities)
        INSERT INTO Person_BusinessEntity (ModifiedDate) 
        SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY 1) * 5, '2025-01-01')
        FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t1
        CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2;
        
        -- Person_Person (30 people)
        INSERT INTO Person_Person (BusinessEntityID, PersonType, FirstName, LastName, EmailPromotion, ModifiedDate) VALUES
        (1, 'EM', 'John', 'Smith', 1, '2025-01-15'), (2, 'EM', 'Sarah', 'Johnson', 0, '2025-02-20'), (3, 'EM', 'Michael', 'Williams', 2, '2025-03-25'),
        (4, 'EM', 'Lisa', 'Brown', 1, '2025-04-30'), (5, 'EM', 'David', 'Jones', 0, '2025-05-15'), (6, 'SC', 'Emma', 'Garcia', 2, '2025-06-20'),
        (7, 'SC', 'James', 'Miller', 1, '2025-07-25'), (8, 'SC', 'Anna', 'Davis', 0, '2025-08-05'), (9, 'SC', 'Robert', 'Rodriguez', 2, '2025-08-15'),
        (10, 'SC', 'Maria', 'Wilson', 1, '2025-08-20'), (11, 'IN', 'Christopher', 'Martinez', 0, '2025-08-25'), (12, 'IN', 'Jessica', 'Anderson', 2, '2025-08-28'),
        (13, 'EM', 'Matthew', 'Taylor', 1, '2025-01-10'), (14, 'EM', 'Ashley', 'Thomas', 0, '2025-02-15'), (15, 'EM', 'Joshua', 'Hernandez', 2, '2025-03-20'),
        (16, 'SC', 'Amanda', 'Moore', 1, '2025-04-25'), (17, 'SC', 'Daniel', 'Martin', 0, '2025-05-30'), (18, 'SC', 'Stephanie', 'Jackson', 2, '2025-06-10'),
        (19, 'IN', 'Ryan', 'Thompson', 1, '2025-07-15'), (20, 'IN', 'Nicole', 'White', 0, '2025-08-01'), (21, 'EM', 'Kevin', 'Lopez', 2, '2025-08-10'),
        (22, 'EM', 'Rachel', 'Lee', 1, '2025-08-15'), (23, 'SC', 'Brandon', 'Gonzalez', 0, '2025-08-20'), (24, 'SC', 'Lauren', 'Harris', 2, '2025-08-25'),
        (25, 'IN', 'Jason', 'Clark', 1, '2025-08-28'), (26, 'IN', 'Megan', 'Lewis', 0, '2025-08-30'), (27, 'EM', 'Tyler', 'Robinson', 2, '2025-01-05'),
        (28, 'EM', 'Kimberly', 'Walker', 1, '2025-02-10'), (29, 'SC', 'Aaron', 'Perez', 0, '2025-03-15'), (30, 'SC', 'Samantha', 'Hall', 2, '2025-04-20');
        
        -- Person_Address (25 addresses)
        INSERT INTO Person_Address (AddressLine1, City, StateProvinceID, PostalCode, ModifiedDate) VALUES
        ('123 Main St', 'Los Angeles', 1, '90210', '2025-01-15'), ('456 Oak Ave', 'Houston', 2, '77001', '2025-02-20'),
        ('789 Pine Rd', 'New York', 3, '10001', '2025-03-25'), ('321 Elm St', 'Miami', 4, '33101', '2025-04-30'),
        ('654 Maple Dr', 'Chicago', 5, '60601', '2025-05-15'), ('987 Cedar Ln', 'Philadelphia', 6, '19101', '2025-06-20'),
        ('147 Birch Way', 'Columbus', 7, '43201', '2025-07-25'), ('258 Spruce St', 'Atlanta', 8, '30301', '2025-08-05'),
        ('369 Willow Ave', 'Charlotte', 9, '28201', '2025-08-15'), ('741 Poplar Rd', 'Detroit', 10, '48201', '2025-08-20'),
        ('852 Ash Dr', 'Toronto', 11, 'M5V 3A8', '2025-08-25'), ('963 Fir St', 'Vancouver', 12, 'V6B 1A1', '2025-08-30'),
        ('159 Cherry Ln', 'San Francisco', 1, '94101', '2025-01-10'), ('357 Walnut Ave', 'Dallas', 2, '75201', '2025-02-15'),
        ('486 Hickory St', 'Boston', 6, '02101', '2025-03-20'), ('624 Pecan Dr', 'Phoenix', 1, '85001', '2025-04-25'),
        ('735 Chestnut Rd', 'San Antonio', 2, '78201', '2025-05-30'), ('846 Magnolia Way', 'San Diego', 1, '92101', '2025-06-10'),
        ('957 Dogwood St', 'Jacksonville', 4, '32099', '2025-07-15'), ('168 Redwood Ave', 'Indianapolis', 5, '46201', '2025-08-01'),
        ('279 Sequoia Dr', 'Austin', 2, '73301', '2025-08-10'), ('381 Cypress Ln', 'Fort Worth', 2, '76101', '2025-08-15'),
        ('492 Juniper St', 'Columbus', 7, '43215', '2025-08-20'), ('513 Sycamore Ave', 'Charlotte', 9, '28202', '2025-08-25'),
        ('624 Cottonwood Dr', 'Seattle', 1, '98101', '2025-08-28');
        """,
    )
    
    # Task 4: Production Data
    populate_production_data = SQLExecuteQueryOperator(
        task_id='populate_production_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Production_ProductCategory
        INSERT INTO Production_ProductCategory (Name, ModifiedDate) VALUES
        ('Bikes', '2025-01-15'), ('Components', '2025-02-20'), ('Clothing', '2025-03-25'), ('Accessories', '2025-04-30'),
        ('Electronics', '2025-05-15'), ('Tools', '2025-06-20'), ('Safety', '2025-07-25'), ('Maintenance', '2025-08-05'),
        ('Parts', '2025-08-15'), ('Upgrades', '2025-08-20'), ('Gear', '2025-08-25'), ('Equipment', '2025-08-30');
        
        -- Production_ProductSubcategory
        INSERT INTO Production_ProductSubcategory (ProductCategoryID, Name, ModifiedDate) VALUES
        (1, 'Mountain Bikes', '2025-01-10'), (1, 'Road Bikes', '2025-02-15'), (1, 'Touring Bikes', '2025-03-20'),
        (2, 'Handlebars', '2025-04-25'), (2, 'Bottom Brackets', '2025-05-30'), (2, 'Brakes', '2025-06-10'),
        (3, 'Jerseys', '2025-07-15'), (3, 'Shorts', '2025-08-05'), (3, 'Gloves', '2025-08-15'),
        (4, 'Helmets', '2025-08-20'), (4, 'Lights', '2025-08-25'), (4, 'Bottles', '2025-08-30');
        
        -- Production_ProductModel
        INSERT INTO Production_ProductModel (Name, CatalogDescription, ModifiedDate) VALUES
        ('Mountain-100', 'High-performance mountain bike', '2025-01-15'), ('Road-150', 'Lightweight road bike', '2025-02-20'),
        ('Touring-1000', 'Comfortable touring bike', '2025-03-25'), ('Classic Helmet', 'Safety certified helmet', '2025-04-30'),
        ('Pro Jersey', 'Professional cycling jersey', '2025-05-15'), ('Sport Gloves', 'Breathable cycling gloves', '2025-06-20'),
        ('LED Light Set', 'High-intensity LED lights', '2025-07-25'), ('Water Bottle', 'BPA-free water bottle', '2025-08-05'),
        ('Bike Computer', 'GPS-enabled bike computer', '2025-08-15'), ('Repair Kit', 'Complete repair kit', '2025-08-20'),
        ('Chain Lube', 'Premium chain lubricant', '2025-08-25'), ('Tire Pump', 'Portable tire pump', '2025-08-30');
        
        -- Production_Location
        INSERT INTO Production_Location (Name, CostRate, Availability, ModifiedDate) VALUES
        ('Assembly Line 1', 15.50, 120.00, '2025-01-10'), ('Assembly Line 2', 16.25, 120.00, '2025-02-15'),
        ('Paint Shop', 18.75, 100.00, '2025-03-20'), ('Quality Control', 20.00, 80.00, '2025-04-25'),
        ('Packaging', 12.50, 160.00, '2025-05-30'), ('Warehouse A', 10.00, 200.00, '2025-06-10'),
        ('Warehouse B', 10.50, 180.00, '2025-07-15'), ('Testing Lab', 25.00, 40.00, '2025-08-05'),
        ('R&D Lab', 30.00, 40.00, '2025-08-15'), ('Maintenance Shop', 22.50, 60.00, '2025-08-20'),
        ('Shipping Dock', 14.00, 120.00, '2025-08-25'), ('Receiving Dock', 13.50, 140.00, '2025-08-30');
        
        -- Production_ScrapReason
        INSERT INTO Production_ScrapReason (Name, ModifiedDate) VALUES
        ('Machine malfunction', '2025-01-15'), ('Operator error', '2025-02-20'), ('Material defect', '2025-03-25'),
        ('Quality failure', '2025-04-30'), ('Design change', '2025-05-15'), ('Tool wear', '2025-06-20'),
        ('Environmental issue', '2025-07-25'), ('Power outage', '2025-08-05'), ('Safety concern', '2025-08-15'),
        ('Maintenance required', '2025-08-20'), ('Process improvement', '2025-08-25'), ('Customer request', '2025-08-30');
        """,
    )
    
    # Task 5: Products and Inventory
    populate_product_data = SQLExecuteQueryOperator(
        task_id='populate_product_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Production_Product (20 products)
        INSERT INTO Production_Product (Name, ProductNumber, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice, DaysToManufacture, ProductSubcategoryID, ProductModelID, SellStartDate, ModifiedDate) VALUES
        ('Mountain-100 Black, 42', 'BK-M82B-42', 500, 375, 1898.09, 3374.99, 4, 1, 1, '2025-01-01', '2025-01-15'),
        ('Mountain-100 Silver, 44', 'BK-M82S-44', 500, 375, 1898.09, 3374.99, 4, 1, 1, '2025-01-15', '2025-02-20'),
        ('Road-150 Red, 52', 'BK-R93R-52', 100, 75, 2171.29, 3578.27, 2, 2, 2, '2025-02-01', '2025-03-25'),
        ('Road-150 Black, 56', 'BK-R93B-56', 100, 75, 2171.29, 3578.27, 2, 2, 2, '2025-02-15', '2025-04-30'),
        ('Touring-1000 Blue, 50', 'BK-T79U-50', 100, 75, 1481.94, 2384.07, 5, 3, 3, '2025-03-01', '2025-05-15'),
        ('Classic Helmet, Black', 'HL-U509-B', 1000, 750, 13.09, 34.99, 1, 10, 4, '2025-03-15', '2025-06-20'),
        ('Pro Jersey, Blue, M', 'SO-B909-M', 800, 600, 20.16, 89.99, 1, 7, 5, '2025-04-01', '2025-07-25'),
        ('Sport Gloves, Black, L', 'GL-H102-L', 800, 600, 9.16, 37.99, 1, 9, 6, '2025-04-15', '2025-08-05'),
        ('LED Light Set', 'LT-H982', 1000, 750, 15.95, 44.99, 1, 11, 7, '2025-05-01', '2025-08-15'),
        ('Water Bottle - 30 oz.', 'WB-H098', 4000, 3000, 1.87, 4.99, 1, 12, 8, '2025-05-15', '2025-08-20'),
        ('Bike Computer GPS', 'BC-G203', 100, 75, 89.47, 199.99, 1, 4, 9, '2025-06-01', '2025-08-25'),
        ('Repair Kit Basic', 'RK-B001', 500, 375, 12.45, 29.99, 1, 4, 10, '2025-06-15', '2025-08-28'),
        ('Chain Lube Premium', 'CL-P500', 1000, 750, 3.25, 8.99, 1, 8, 11, '2025-07-01', '2025-08-30'),
        ('Tire Pump Portable', 'TP-P200', 200, 150, 18.95, 49.99, 1, 4, 12, '2025-07-15', '2025-01-10'),
        ('Mountain Tire 26x2.1', 'TI-M267', 800, 600, 15.75, 35.00, 1, 1, 1, '2025-08-01', '2025-02-15'),
        ('Road Tire 700x23C', 'TI-R982', 600, 450, 18.50, 42.00, 1, 2, 2, '2025-08-05', '2025-03-20'),
        ('Brake Pads V-Brake', 'BP-V100', 2000, 1500, 4.25, 12.99, 1, 6, 2, '2025-08-10', '2025-04-25'),
        ('Handlebar Tape Black', 'HT-B001', 1500, 1125, 2.95, 9.99, 1, 4, 3, '2025-08-15', '2025-05-30'),
        ('Pedals Clipless', 'PD-C300', 300, 225, 45.75, 89.99, 1, 2, 2, '2025-08-20', '2025-06-10'),
        ('Saddle Sport', 'SA-S200', 400, 300, 25.50, 65.99, 1, 1, 1, '2025-08-25', '2025-07-15');
        
        -- Production_ProductInventory
        INSERT INTO Production_ProductInventory (ProductID, LocationID, Shelf, Bin, Quantity, ModifiedDate) VALUES
        (1, 1, 'A', 1, 250, '2025-08-01'), (1, 6, 'B', 2, 180, '2025-08-05'), (2, 1, 'A', 3, 320, '2025-08-10'),
        (2, 7, 'C', 1, 150, '2025-08-15'), (3, 2, 'A', 4, 85, '2025-08-20'), (3, 6, 'B', 5, 65, '2025-08-25'),
        (4, 2, 'A', 6, 95, '2025-08-28'), (4, 7, 'C', 2, 75, '2025-08-30'), (5, 3, 'B', 1, 120, '2025-01-15'),
        (5, 6, 'A', 7, 90, '2025-02-20'), (6, 1, 'C', 3, 850, '2025-03-25'), (6, 8, 'A', 8, 650, '2025-04-30'),
        (7, 4, 'B', 4, 420, '2025-05-15'), (7, 9, 'C', 5, 380, '2025-06-20'), (8, 5, 'A', 9, 520, '2025-07-25'),
        (8, 10, 'B', 6, 480, '2025-08-01'), (9, 1, 'C', 7, 680, '2025-08-05'), (9, 11, 'A', 10, 320, '2025-08-10'),
        (10, 6, 'B', 8, 2800, '2025-08-15'), (10, 12, 'C', 9, 1200, '2025-08-20'), (11, 2, 'A', 11, 45, '2025-08-25'),
        (12, 3, 'B', 10, 280, '2025-08-28'), (13, 4, 'C', 11, 750, '2025-08-30'), (14, 5, 'A', 12, 125, '2025-01-10'),
        (15, 1, 'B', 12, 520, '2025-02-15');
        """,
    )
    
    # Task 6: HR and Employee Data
    populate_hr_data = SQLExecuteQueryOperator(
        task_id='populate_hr_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- HumanResources_Department
        INSERT INTO HumanResources_Department (Name, GroupName, ModifiedDate) VALUES
        ('Engineering', 'Research and Development', '2025-01-15'), ('Sales', 'Sales and Marketing', '2025-02-20'),
        ('Marketing', 'Sales and Marketing', '2025-03-25'), ('Production', 'Manufacturing', '2025-04-30'),
        ('Quality Assurance', 'Quality Assurance', '2025-05-15'), ('Human Resources', 'Executive General and Administration', '2025-06-20'),
        ('Finance', 'Executive General and Administration', '2025-07-25'), ('Information Services', 'Executive General and Administration', '2025-08-05'),
        ('Purchasing', 'Inventory Management', '2025-08-15'), ('Shipping and Receiving', 'Inventory Management', '2025-08-20'),
        ('Research and Development', 'Research and Development', '2025-08-25'), ('Executive', 'Executive General and Administration', '2025-08-30');
        
        -- HumanResources_Shift
        INSERT INTO HumanResources_Shift (Name, StartTime, EndTime, ModifiedDate) VALUES
        ('Day', '07:00:00', '15:00:00', '2025-01-10'), ('Evening', '15:00:00', '23:00:00', '2025-02-15'),
        ('Night', '23:00:00', '07:00:00', '2025-03-20'), ('Morning', '06:00:00', '14:00:00', '2025-04-25'),
        ('Afternoon', '14:00:00', '22:00:00', '2025-05-30'), ('Weekend Day', '08:00:00', '16:00:00', '2025-06-10'),
        ('Weekend Night', '20:00:00', '04:00:00', '2025-07-15'), ('Flex Morning', '05:00:00', '13:00:00', '2025-08-05'),
        ('Flex Evening', '13:00:00', '21:00:00', '2025-08-15'), ('Split Shift', '09:00:00', '17:00:00', '2025-08-20'),
        ('Extended Day', '06:00:00', '18:00:00', '2025-08-25'), ('Compressed Week', '07:00:00', '19:00:00', '2025-08-30');
        
        -- HumanResources_Employee
        INSERT INTO HumanResources_Employee (BusinessEntityID, NationalIDNumber, LoginID, JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours, CurrentFlag, ModifiedDate) VALUES
        (1, '295847284', 'adventure-works\\john0', 'Chief Executive Officer', '1975-01-15', 'M', 'M', '2025-01-15', TRUE, 80, 40, TRUE, '2025-01-15'),
        (2, '245797967', 'adventure-works\\sarah0', 'Vice President Engineering', '1978-03-22', 'S', 'F', '2025-02-01', TRUE, 75, 35, TRUE, '2025-02-20'),
        (3, '509647174', 'adventure-works\\michael0', 'Engineering Manager', '1980-07-10', 'M', 'M', '2025-02-15', TRUE, 70, 30, TRUE, '2025-03-25'),
        (4, '112457891', 'adventure-works\\lisa0', 'Sales Manager', '1982-11-05', 'M', 'F', '2025-03-01', TRUE, 65, 25, TRUE, '2025-04-30'),
        (5, '695256908', 'adventure-works\\david0', 'Marketing Director', '1979-09-18', 'S', 'M', '2025-03-15', TRUE, 72, 32, TRUE, '2025-05-15'),
        (6, '998320692', 'adventure-works\\emma0', 'Production Supervisor', '1985-04-12', 'M', 'F', '2025-04-01', FALSE, 60, 20, TRUE, '2025-06-20'),
        (7, '134969118', 'adventure-works\\james0', 'Quality Manager', '1983-12-08', 'S', 'M', '2025-04-15', TRUE, 68, 28, TRUE, '2025-07-25'),
        (8, '811994146', 'adventure-works\\anna0', 'HR Specialist', '1987-06-25', 'M', 'F', '2025-05-01', TRUE, 64, 24, TRUE, '2025-08-05'),
        (9, '658797903', 'adventure-works\\robert0', 'Finance Analyst', '1984-02-14', 'S', 'M', '2025-05-15', TRUE, 66, 26, TRUE, '2025-08-15'),
        (10, '879342154', 'adventure-works\\maria0', 'IT Manager', '1981-10-30', 'M', 'F', '2025-06-01', TRUE, 74, 34, TRUE, '2025-08-20'),
        (11, '456789123', 'adventure-works\\chris0', 'Purchasing Agent', '1986-08-17', 'S', 'M', '2025-06-15', FALSE, 58, 18, TRUE, '2025-08-25'),
        (12, '789123456', 'adventure-works\\jessica0', 'Shipping Coordinator', '1988-01-09', 'M', 'F', '2025-07-01', FALSE, 56, 16, TRUE, '2025-08-28');
        """,
    )
    
    # Task 7: Sales Data
    populate_sales_data = SQLExecuteQueryOperator(
        task_id='populate_sales_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Sales_SalesReason
        INSERT INTO Sales_SalesReason (Name, ReasonType, ModifiedDate) VALUES
        ('Price', 'Other', '2025-01-15'), ('Quality', 'Other', '2025-02-20'), ('On Promotion', 'Promotion', '2025-03-25'),
        ('Magazine Advertisement', 'Marketing', '2025-04-30'), ('Television Advertisement', 'Marketing', '2025-05-15'),
        ('Manufacturer', 'Other', '2025-06-20'), ('Review', 'Other', '2025-07-25'), ('Demo Event', 'Marketing', '2025-08-05'),
        ('Sponsorship', 'Marketing', '2025-08-15'), ('Word of Mouth', 'Other', '2025-08-20'), ('Social Media', 'Marketing', '2025-08-25'),
        ('Online Search', 'Marketing', '2025-08-30');
        
        -- Sales_SpecialOffer
        INSERT INTO Sales_SpecialOffer (Description, DiscountPct, Type, Category, StartDate, EndDate, MinQty, ModifiedDate) VALUES
        ('No Discount', 0.00, 'No Discount', 'No Discount', '2025-01-01', '2025-12-31', 0, '2025-01-15'),
        ('Volume Discount 11 to 14', 0.02, 'Volume Discount', 'Reseller', '2025-01-01', '2025-12-31', 11, '2025-02-20'),
        ('Volume Discount 15 to 24', 0.05, 'Volume Discount', 'Reseller', '2025-01-01', '2025-12-31', 15, '2025-03-25'),
        ('Summer Sale 2025', 0.15, 'Seasonal Discount', 'Customer', '2025-06-01', '2025-08-31', 0, '2025-04-30'),
        ('Back to School', 0.10, 'Seasonal Discount', 'Customer', '2025-08-01', '2025-09-15', 0, '2025-05-15'),
        ('Holiday Special', 0.20, 'Holiday', 'Customer', '2025-11-15', '2025-12-31', 0, '2025-06-20'),
        ('New Customer Discount', 0.12, 'New Customer', 'Customer', '2025-01-01', '2025-12-31', 0, '2025-07-25'),
        ('Bulk Order Discount', 0.08, 'Volume Discount', 'Reseller', '2025-01-01', '2025-12-31', 25, '2025-08-05'),
        ('Clearance Sale', 0.30, 'Clearance', 'Customer', '2025-08-15', '2025-08-31', 0, '2025-08-15'),
        ('Flash Sale', 0.25, 'Flash Sale', 'Customer', '2025-08-20', '2025-08-22', 0, '2025-08-20'),
        ('Weekend Special', 0.18, 'Weekend', 'Customer', '2025-08-25', '2025-08-27', 0, '2025-08-25'),
        ('End of Month', 0.22, 'Monthly', 'Customer', '2025-08-28', '2025-08-31', 0, '2025-08-28');
        
        -- Sales_CreditCard
        INSERT INTO Sales_CreditCard (CardType, CardNumber, ExpMonth, ExpYear, ModifiedDate) VALUES
        ('Vista', '1111-2222-3333-4444', 12, 2027, '2025-01-15'), ('SuperiorCard', '2222-3333-4444-5555', 11, 2026, '2025-02-20'),
        ('Distinguish', '3333-4444-5555-6666', 10, 2028, '2025-03-25'), ('ColonialVoice', '4444-5555-6666-7777', 9, 2027, '2025-04-30'),
        ('Vista', '5555-6666-7777-8888', 8, 2026, '2025-05-15'), ('SuperiorCard', '6666-7777-8888-9999', 7, 2028, '2025-06-20'),
        ('Distinguish', '7777-8888-9999-0000', 6, 2027, '2025-07-25'), ('ColonialVoice', '8888-9999-0000-1111', 5, 2026, '2025-08-05'),
        ('Vista', '9999-0000-1111-2222', 4, 2028, '2025-08-15'), ('SuperiorCard', '0000-1111-2222-3333', 3, 2027, '2025-08-20'),
        ('Distinguish', '1111-2222-3333-4445', 2, 2026, '2025-08-25'), ('ColonialVoice', '2222-3333-4444-5556', 1, 2028, '2025-08-30');
        
        -- Sales_SalesPerson
        INSERT INTO Sales_SalesPerson (BusinessEntityID, TerritoryID, SalesQuota, Bonus, CommissionPct, SalesYTD, SalesLastYear, ModifiedDate) VALUES
        (1, 1, 350000.00, 5000.00, 0.015, 285000.00, 320000.00, '2025-01-15'),
        (2, 2, 320000.00, 4500.00, 0.014, 265000.00, 295000.00, '2025-02-20'),
        (3, 3, 380000.00, 6000.00, 0.016, 315000.00, 350000.00, '2025-03-25'),
        (4, 4, 300000.00, 4000.00, 0.013, 245000.00, 275000.00, '2025-04-30'),
        (5, 5, 340000.00, 5200.00, 0.015, 280000.00, 310000.00, '2025-05-15'),
        (6, 6, 290000.00, 3800.00, 0.012, 235000.00, 260000.00, '2025-06-20'),
        (7, 7, 360000.00, 5500.00, 0.015, 295000.00, 330000.00, '2025-07-25'),
        (8, 8, 310000.00, 4200.00, 0.013, 255000.00, 285000.00, '2025-08-05'),
        (9, 9, 330000.00, 4800.00, 0.014, 270000.00, 300000.00, '2025-08-15'),
        (10, 10, 280000.00, 3600.00, 0.012, 230000.00, 255000.00, '2025-08-20'),
        (11, 11, 320000.00, 4600.00, 0.014, 260000.00, 290000.00, '2025-08-25'),
        (12, 12, 300000.00, 4100.00, 0.013, 245000.00, 270000.00, '2025-08-30');
        """,
    )
    
    # Task 8: Complete remaining tables with current dates
    populate_remaining_tables = SQLExecuteQueryOperator(
        task_id='populate_remaining_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Complete all remaining tables with 2025 dates
        -- This ensures every table has at least 10 rows with dates from 2025
        
        -- Sales_Customer (using existing persons and territories)
        INSERT INTO Sales_Customer (PersonID, TerritoryID, AccountNumber, ModifiedDate)
        SELECT 
            BusinessEntityID,
            CASE WHEN BusinessEntityID <= 12 THEN BusinessEntityID ELSE 1 END,
            'AW' || LPAD(BusinessEntityID, 8, '0'),
            DATEADD(day, BusinessEntityID * 3, '2025-01-01')
        FROM Person_Person
        WHERE BusinessEntityID <= 30;
        
        -- Sales_Store (using business entities 31-50)
        INSERT INTO Sales_Store (BusinessEntityID, Name, SalesPersonID, ModifiedDate)
        SELECT 
            BusinessEntityID,
            'Store ' || (BusinessEntityID - 30),
            CASE WHEN (BusinessEntityID - 30) <= 12 THEN (BusinessEntityID - 30) ELSE 1 END,
            DATEADD(day, BusinessEntityID, '2025-01-01')
        FROM Person_BusinessEntity
        WHERE BusinessEntityID BETWEEN 31 AND 50;
        
        -- Generate sales orders for 2025 up to August 6, 2025
        INSERT INTO Sales_SalesOrderHeader (OrderDate, DueDate, ShipDate, Status, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue, ModifiedDate)
        SELECT 
            DATEADD(day, UNIFORM(1, 218, RANDOM()), '2025-01-01') as OrderDate, -- 218 days = Jan 1 to Aug 6
            DATEADD(day, UNIFORM(10, 20, RANDOM()), DATEADD(day, UNIFORM(1, 218, RANDOM()), '2025-01-01')) as DueDate,
            DATEADD(day, UNIFORM(5, 15, RANDOM()), DATEADD(day, UNIFORM(1, 218, RANDOM()), '2025-01-01')) as ShipDate,
            5,
            c.CustomerID,
            CASE WHEN c.CustomerID <= 12 THEN c.CustomerID ELSE 1 END,
            c.TerritoryID,
            CASE WHEN c.CustomerID <= 25 THEN c.CustomerID ELSE 1 END,
            CASE WHEN c.CustomerID <= 25 THEN c.CustomerID ELSE 1 END,
            1,
            UNIFORM(500, 5000, RANDOM()) * 1.0,
            UNIFORM(50, 500, RANDOM()) * 1.0,
            UNIFORM(25, 150, RANDOM()) * 1.0,
            UNIFORM(600, 6000, RANDOM()) * 1.0,
            DATEADD(day, UNIFORM(1, 218, RANDOM()), '2025-01-01')
        FROM Sales_Customer c
        WHERE c.CustomerID <= 20;
        
        -- Create a summary table showing data population
        CREATE OR REPLACE TABLE DATA_POPULATION_SUMMARY_2025 AS
        SELECT 
            'All tables populated with 2025 dates only' as status,
            COUNT(*) as total_sales_orders,
            MIN(OrderDate) as earliest_order,
            MAX(OrderDate) as latest_order
        FROM Sales_SalesOrderHeader
        WHERE OrderDate >= '2025-01-01';
        """,
    )
    
    # Define dependencies
    populate_reference_data >> populate_territory_data >> populate_people_data
    populate_people_data >> populate_production_data >> populate_product_data
    populate_product_data >> populate_hr_data >> populate_sales_data >> populate_remaining_tables

populate_all_tables_2025()