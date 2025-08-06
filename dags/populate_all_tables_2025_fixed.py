"""
Populate ALL AdventureWorks Tables with 2025 Raw Data
All dates between 2025-07-31 and 2025-08-06
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id='populate_all_tables_2025_fixed',
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['setup', 'data', '2025'],
    description='Populate ALL AdventureWorks tables with 2025 raw data',
)
def populate_all_tables_2025_fixed():
    
    populate_all_data = SQLExecuteQueryOperator(
        task_id='populate_all_data',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        -- Person Schema
        INSERT INTO Person_CountryRegion VALUES 
        ('US', 'United States', '2025-08-01'),
        ('CA', 'Canada', '2025-08-02'),
        ('UK', 'United Kingdom', '2025-08-03'),
        ('FR', 'France', '2025-08-04'),
        ('DE', 'Germany', '2025-08-05'),
        ('JP', 'Japan', '2025-08-06'),
        ('AU', 'Australia', '2025-07-31'),
        ('BR', 'Brazil', '2025-08-01'),
        ('MX', 'Mexico', '2025-08-02'),
        ('IT', 'Italy', '2025-08-03');
        
        INSERT INTO Person_AddressType VALUES 
        (DEFAULT, 'Home', 'guid-001', '2025-07-31'),
        (DEFAULT, 'Business', 'guid-002', '2025-08-01'),
        (DEFAULT, 'Shipping', 'guid-003', '2025-08-02'),
        (DEFAULT, 'Billing', 'guid-004', '2025-08-03'),
        (DEFAULT, 'Archive', 'guid-005', '2025-08-04'),
        (DEFAULT, 'Main Office', 'guid-006', '2025-08-05'),
        (DEFAULT, 'Primary', 'guid-007', '2025-08-06'),
        (DEFAULT, 'Alternate', 'guid-008', '2025-07-31'),
        (DEFAULT, 'Temporary', 'guid-009', '2025-08-01'),
        (DEFAULT, 'Warehouse', 'guid-010', '2025-08-02');
        
        INSERT INTO Person_ContactType VALUES 
        (DEFAULT, 'Owner', '2025-07-31'),
        (DEFAULT, 'Manager', '2025-08-01'),
        (DEFAULT, 'Sales Representative', '2025-08-02'),
        (DEFAULT, 'Purchasing Manager', '2025-08-03'),
        (DEFAULT, 'Marketing Manager', '2025-08-04'),
        (DEFAULT, 'Assistant Sales Agent', '2025-08-05'),
        (DEFAULT, 'Accounting Manager', '2025-08-06'),
        (DEFAULT, 'International Marketing Manager', '2025-07-31'),
        (DEFAULT, 'Purchasing Agent', '2025-08-01'),
        (DEFAULT, 'Order Administrator', '2025-08-02');
        
        INSERT INTO Person_PhoneNumberType VALUES 
        (DEFAULT, 'Cell', '2025-07-31'),
        (DEFAULT, 'Home', '2025-08-01'),
        (DEFAULT, 'Work', '2025-08-02'),
        (DEFAULT, 'Fax', '2025-08-03'),
        (DEFAULT, 'Pager', '2025-08-04'),
        (DEFAULT, 'Emergency', '2025-08-05'),
        (DEFAULT, 'Mobile', '2025-08-06'),
        (DEFAULT, 'Office', '2025-07-31'),
        (DEFAULT, 'Toll Free', '2025-08-01'),
        (DEFAULT, 'Direct', '2025-08-02');
        
        INSERT INTO Person_BusinessEntity VALUES 
        (DEFAULT, 'guid-be-001', '2025-07-31'),
        (DEFAULT, 'guid-be-002', '2025-08-01'),
        (DEFAULT, 'guid-be-003', '2025-08-02'),
        (DEFAULT, 'guid-be-004', '2025-08-03'),
        (DEFAULT, 'guid-be-005', '2025-08-04'),
        (DEFAULT, 'guid-be-006', '2025-08-05'),
        (DEFAULT, 'guid-be-007', '2025-08-06'),
        (DEFAULT, 'guid-be-008', '2025-07-31'),
        (DEFAULT, 'guid-be-009', '2025-08-01'),
        (DEFAULT, 'guid-be-010', '2025-08-02'),
        (DEFAULT, 'guid-be-011', '2025-08-03'),
        (DEFAULT, 'guid-be-012', '2025-08-04'),
        (DEFAULT, 'guid-be-013', '2025-08-05'),
        (DEFAULT, 'guid-be-014', '2025-08-06'),
        (DEFAULT, 'guid-be-015', '2025-07-31'),
        (DEFAULT, 'guid-be-016', '2025-08-01'),
        (DEFAULT, 'guid-be-017', '2025-08-02'),
        (DEFAULT, 'guid-be-018', '2025-08-03'),
        (DEFAULT, 'guid-be-019', '2025-08-04'),
        (DEFAULT, 'guid-be-020', '2025-08-05'),
        (DEFAULT, 'guid-be-021', '2025-08-06'),
        (DEFAULT, 'guid-be-022', '2025-07-31'),
        (DEFAULT, 'guid-be-023', '2025-08-01'),
        (DEFAULT, 'guid-be-024', '2025-08-02'),
        (DEFAULT, 'guid-be-025', '2025-08-03'),
        (DEFAULT, 'guid-be-026', '2025-08-04'),
        (DEFAULT, 'guid-be-027', '2025-08-05'),
        (DEFAULT, 'guid-be-028', '2025-08-06'),
        (DEFAULT, 'guid-be-029', '2025-07-31'),
        (DEFAULT, 'guid-be-030', '2025-08-01');
        
        INSERT INTO Person_Person VALUES 
        (1, 'IN', FALSE, NULL, 'John', NULL, 'Doe', NULL, 0, NULL, NULL, 'guid-p-001', '2025-07-31'),
        (2, 'IN', FALSE, NULL, 'Jane', NULL, 'Smith', NULL, 0, NULL, NULL, 'guid-p-002', '2025-08-01'),
        (3, 'IN', FALSE, NULL, 'Bob', NULL, 'Johnson', NULL, 0, NULL, NULL, 'guid-p-003', '2025-08-02'),
        (4, 'IN', FALSE, NULL, 'Alice', NULL, 'Williams', NULL, 0, NULL, NULL, 'guid-p-004', '2025-08-03'),
        (5, 'IN', FALSE, NULL, 'Charlie', NULL, 'Brown', NULL, 0, NULL, NULL, 'guid-p-005', '2025-08-04'),
        (6, 'IN', FALSE, NULL, 'Diana', NULL, 'Davis', NULL, 0, NULL, NULL, 'guid-p-006', '2025-08-05'),
        (7, 'IN', FALSE, NULL, 'Edward', NULL, 'Miller', NULL, 0, NULL, NULL, 'guid-p-007', '2025-08-06'),
        (8, 'IN', FALSE, NULL, 'Fiona', NULL, 'Wilson', NULL, 0, NULL, NULL, 'guid-p-008', '2025-07-31'),
        (9, 'IN', FALSE, NULL, 'George', NULL, 'Moore', NULL, 0, NULL, NULL, 'guid-p-009', '2025-08-01'),
        (10, 'IN', FALSE, NULL, 'Helen', NULL, 'Taylor', NULL, 0, NULL, NULL, 'guid-p-010', '2025-08-02'),
        (11, 'IN', FALSE, NULL, 'Ian', NULL, 'Anderson', NULL, 0, NULL, NULL, 'guid-p-011', '2025-08-03'),
        (12, 'IN', FALSE, NULL, 'Julia', NULL, 'Thomas', NULL, 0, NULL, NULL, 'guid-p-012', '2025-08-04'),
        (13, 'IN', FALSE, NULL, 'Kevin', NULL, 'Jackson', NULL, 0, NULL, NULL, 'guid-p-013', '2025-08-05'),
        (14, 'IN', FALSE, NULL, 'Linda', NULL, 'White', NULL, 0, NULL, NULL, 'guid-p-014', '2025-08-06'),
        (15, 'IN', FALSE, NULL, 'Michael', NULL, 'Harris', NULL, 0, NULL, NULL, 'guid-p-015', '2025-07-31'),
        (16, 'IN', FALSE, NULL, 'Nancy', NULL, 'Martin', NULL, 0, NULL, NULL, 'guid-p-016', '2025-08-01'),
        (17, 'IN', FALSE, NULL, 'Oliver', NULL, 'Garcia', NULL, 0, NULL, NULL, 'guid-p-017', '2025-08-02'),
        (18, 'IN', FALSE, NULL, 'Patricia', NULL, 'Rodriguez', NULL, 0, NULL, NULL, 'guid-p-018', '2025-08-03'),
        (19, 'IN', FALSE, NULL, 'Quincy', NULL, 'Lewis', NULL, 0, NULL, NULL, 'guid-p-019', '2025-08-04'),
        (20, 'IN', FALSE, NULL, 'Rachel', NULL, 'Lee', NULL, 0, NULL, NULL, 'guid-p-020', '2025-08-05');
        
        -- Sales Schema
        INSERT INTO Sales_Currency VALUES 
        ('USD', 'US Dollar', '2025-07-31'),
        ('CAD', 'Canadian Dollar', '2025-08-01'),
        ('EUR', 'Euro', '2025-08-02'),
        ('GBP', 'British Pound', '2025-08-03'),
        ('JPY', 'Japanese Yen', '2025-08-04'),
        ('AUD', 'Australian Dollar', '2025-08-05'),
        ('BRL', 'Brazilian Real', '2025-08-06'),
        ('MXN', 'Mexican Peso', '2025-07-31'),
        ('CHF', 'Swiss Franc', '2025-08-01'),
        ('CNY', 'Chinese Yuan', '2025-08-02');
        
        INSERT INTO Sales_SalesTerritory VALUES 
        (DEFAULT, 'North America', 'US', 'NA', 1000000, 900000, 500000, 450000, 'guid-101', '2025-07-31'),
        (DEFAULT, 'Europe', 'UK', 'EU', 800000, 750000, 400000, 380000, 'guid-102', '2025-08-01'),
        (DEFAULT, 'Asia Pacific', 'JP', 'AP', 600000, 550000, 300000, 280000, 'guid-103', '2025-08-02'),
        (DEFAULT, 'South America', 'BR', 'SA', 400000, 350000, 200000, 180000, 'guid-104', '2025-08-03'),
        (DEFAULT, 'Canada', 'CA', 'NA', 500000, 450000, 250000, 230000, 'guid-105', '2025-08-04'),
        (DEFAULT, 'France', 'FR', 'EU', 700000, 650000, 350000, 320000, 'guid-106', '2025-08-05'),
        (DEFAULT, 'Germany', 'DE', 'EU', 750000, 700000, 375000, 350000, 'guid-107', '2025-08-06'),
        (DEFAULT, 'Australia', 'AU', 'AP', 450000, 400000, 225000, 200000, 'guid-108', '2025-07-31'),
        (DEFAULT, 'Mexico', 'MX', 'SA', 350000, 300000, 175000, 150000, 'guid-109', '2025-08-01'),
        (DEFAULT, 'Italy', 'IT', 'EU', 550000, 500000, 275000, 250000, 'guid-110', '2025-08-02');
        
        INSERT INTO Person_StateProvince VALUES 
        (DEFAULT, 'CA', 'US', TRUE, 'California', 1, 'guid-201', '2025-07-31'),
        (DEFAULT, 'NY', 'US', TRUE, 'New York', 1, 'guid-202', '2025-08-01'),
        (DEFAULT, 'TX', 'US', TRUE, 'Texas', 1, 'guid-203', '2025-08-02'),
        (DEFAULT, 'FL', 'US', TRUE, 'Florida', 1, 'guid-204', '2025-08-03'),
        (DEFAULT, 'ON', 'CA', TRUE, 'Ontario', 5, 'guid-205', '2025-08-04'),
        (DEFAULT, 'BC', 'CA', TRUE, 'British Columbia', 5, 'guid-206', '2025-08-05'),
        (DEFAULT, 'LN', 'UK', TRUE, 'London', 2, 'guid-207', '2025-08-06'),
        (DEFAULT, 'PR', 'FR', TRUE, 'Paris', 6, 'guid-208', '2025-07-31'),
        (DEFAULT, 'BR', 'DE', TRUE, 'Berlin', 7, 'guid-209', '2025-08-01'),
        (DEFAULT, 'TK', 'JP', TRUE, 'Tokyo', 3, 'guid-210', '2025-08-02');
        
        INSERT INTO Person_Address VALUES 
        (DEFAULT, '123 Main St', NULL, 'Los Angeles', 1, '90210', NULL, 'guid-301', '2025-07-31'),
        (DEFAULT, '456 Oak Ave', NULL, 'New York', 2, '10001', NULL, 'guid-302', '2025-08-01'),
        (DEFAULT, '789 Pine Rd', NULL, 'Houston', 3, '77001', NULL, 'guid-303', '2025-08-02'),
        (DEFAULT, '321 Elm St', NULL, 'Miami', 4, '33101', NULL, 'guid-304', '2025-08-03'),
        (DEFAULT, '654 Maple Dr', NULL, 'Toronto', 5, 'M5V1A1', NULL, 'guid-305', '2025-08-04'),
        (DEFAULT, '987 Cedar Ln', NULL, 'Vancouver', 6, 'V6B1A1', NULL, 'guid-306', '2025-08-05'),
        (DEFAULT, '147 Birch Way', NULL, 'London', 7, 'SW1A1AA', NULL, 'guid-307', '2025-08-06'),
        (DEFAULT, '258 Ash Blvd', NULL, 'Paris', 8, '75001', NULL, 'guid-308', '2025-07-31'),
        (DEFAULT, '369 Willow St', NULL, 'Berlin', 9, '10115', NULL, 'guid-309', '2025-08-01'),
        (DEFAULT, '741 Spruce Ave', NULL, 'Tokyo', 10, '100-0001', NULL, 'guid-310', '2025-08-02');
        
        INSERT INTO Sales_CreditCard VALUES 
        (DEFAULT, 'Visa', '4111111111111111', 12, 2026, '2025-07-31'),
        (DEFAULT, 'MasterCard', '5555555555554444', 6, 2027, '2025-08-01'),
        (DEFAULT, 'American Express', '378282246310005', 3, 2028, '2025-08-02'),
        (DEFAULT, 'Discover', '6011111111111117', 9, 2026, '2025-08-03'),
        (DEFAULT, 'Visa', '4000000000000002', 1, 2029, '2025-08-04'),
        (DEFAULT, 'MasterCard', '5105105105105100', 8, 2027, '2025-08-05'),
        (DEFAULT, 'Visa', '4012888888881881', 11, 2028, '2025-08-06'),
        (DEFAULT, 'American Express', '371449635398431', 4, 2026, '2025-07-31'),
        (DEFAULT, 'Discover', '6011000990139424', 7, 2029, '2025-08-01'),
        (DEFAULT, 'MasterCard', '5555555555554444', 10, 2027, '2025-08-02');
        
        INSERT INTO Sales_SalesReason VALUES 
        (DEFAULT, 'Quality', 'Product', '2025-07-31'),
        (DEFAULT, 'Price', 'Marketing', '2025-08-01'),
        (DEFAULT, 'Manufacturer', 'Product', '2025-08-02'),
        (DEFAULT, 'Review', 'Marketing', '2025-08-03'),
        (DEFAULT, 'Television Advertisement', 'Marketing', '2025-08-04'),
        (DEFAULT, 'Magazine Advertisement', 'Marketing', '2025-08-05'),
        (DEFAULT, 'Demo Event', 'Marketing', '2025-08-06'),
        (DEFAULT, 'Sponsorship', 'Marketing', '2025-07-31'),
        (DEFAULT, 'Other', 'Other', '2025-08-01'),
        (DEFAULT, 'On Promotion', 'Promotion', '2025-08-02');
        
        -- Production Schema
        INSERT INTO Production_UnitMeasure VALUES 
        ('EA', 'Each', '2025-07-31'),
        ('LB', 'Pound', '2025-08-01'),
        ('KG', 'Kilogram', '2025-08-02'),
        ('CM', 'Centimeter', '2025-08-03'),
        ('IN', 'Inch', '2025-08-04'),
        ('FT', 'Foot', '2025-08-05'),
        ('M', 'Meter', '2025-08-06'),
        ('L', 'Liter', '2025-07-31'),
        ('ML', 'Milliliter', '2025-08-01'),
        ('G', 'Gram', '2025-08-02');
        
        INSERT INTO Production_ProductCategory VALUES 
        (DEFAULT, 'Bikes', 'guid-cat-001', '2025-07-31'),
        (DEFAULT, 'Components', 'guid-cat-002', '2025-08-01'),
        (DEFAULT, 'Clothing', 'guid-cat-003', '2025-08-02'),
        (DEFAULT, 'Accessories', 'guid-cat-004', '2025-08-03'),
        (DEFAULT, 'Tools', 'guid-cat-005', '2025-08-04'),
        (DEFAULT, 'Parts', 'guid-cat-006', '2025-08-05'),
        (DEFAULT, 'Maintenance', 'guid-cat-007', '2025-08-06'),
        (DEFAULT, 'Safety', 'guid-cat-008', '2025-07-31'),
        (DEFAULT, 'Electronics', 'guid-cat-009', '2025-08-01'),
        (DEFAULT, 'Books', 'guid-cat-010', '2025-08-02');
        
        INSERT INTO Production_ProductSubcategory VALUES 
        (DEFAULT, 1, 'Mountain Bikes', 'guid-sub-001', '2025-07-31'),
        (DEFAULT, 1, 'Road Bikes', 'guid-sub-002', '2025-08-01'),
        (DEFAULT, 1, 'Touring Bikes', 'guid-sub-003', '2025-08-02'),
        (DEFAULT, 2, 'Handlebars', 'guid-sub-004', '2025-08-03'),
        (DEFAULT, 2, 'Bottom Brackets', 'guid-sub-005', '2025-08-04'),
        (DEFAULT, 2, 'Brakes', 'guid-sub-006', '2025-08-05'),
        (DEFAULT, 2, 'Chains', 'guid-sub-007', '2025-08-06'),
        (DEFAULT, 2, 'Cranksets', 'guid-sub-008', '2025-07-31'),
        (DEFAULT, 2, 'Derailleurs', 'guid-sub-009', '2025-08-01'),
        (DEFAULT, 2, 'Forks', 'guid-sub-010', '2025-08-02'),
        (DEFAULT, 2, 'Headsets', 'guid-sub-011', '2025-08-03'),
        (DEFAULT, 2, 'Mountain Frames', 'guid-sub-012', '2025-08-04'),
        (DEFAULT, 2, 'Pedals', 'guid-sub-013', '2025-08-05'),
        (DEFAULT, 2, 'Road Frames', 'guid-sub-014', '2025-08-06'),
        (DEFAULT, 2, 'Saddles', 'guid-sub-015', '2025-07-31'),
        (DEFAULT, 2, 'Touring Frames', 'guid-sub-016', '2025-08-01'),
        (DEFAULT, 2, 'Wheels', 'guid-sub-017', '2025-08-02'),
        (DEFAULT, 3, 'Bib-Shorts', 'guid-sub-018', '2025-08-03'),
        (DEFAULT, 3, 'Caps', 'guid-sub-019', '2025-08-04'),
        (DEFAULT, 3, 'Gloves', 'guid-sub-020', '2025-08-05');
        
        INSERT INTO Production_ProductModel VALUES 
        (DEFAULT, 'Classic Vest', NULL, NULL, 'guid-model-001', '2025-07-31'),
        (DEFAULT, 'Cycling Cap', NULL, NULL, 'guid-model-002', '2025-08-01'),
        (DEFAULT, 'Full-Finger Gloves', NULL, NULL, 'guid-model-003', '2025-08-02'),
        (DEFAULT, 'Half-Finger Gloves', NULL, NULL, 'guid-model-004', '2025-08-03'),
        (DEFAULT, 'HL Mountain Frame', NULL, NULL, 'guid-model-005', '2025-08-04'),
        (DEFAULT, 'HL Road Frame', NULL, NULL, 'guid-model-006', '2025-08-05'),
        (DEFAULT, 'HL Touring Frame', NULL, NULL, 'guid-model-007', '2025-08-06'),
        (DEFAULT, 'LL Mountain Frame', NULL, NULL, 'guid-model-008', '2025-07-31'),
        (DEFAULT, 'LL Road Frame', NULL, NULL, 'guid-model-009', '2025-08-01'),
        (DEFAULT, 'LL Touring Frame', NULL, NULL, 'guid-model-010', '2025-08-02');
        
        INSERT INTO Production_Culture VALUES 
        ('en', 'English', '2025-07-31'),
        ('fr', 'French', '2025-08-01'),
        ('es', 'Spanish', '2025-08-02'),
        ('de', 'German', '2025-08-03'),
        ('it', 'Italian', '2025-08-04'),
        ('pt', 'Portuguese', '2025-08-05'),
        ('ja', 'Japanese', '2025-08-06'),
        ('zh', 'Chinese', '2025-07-31'),
        ('ko', 'Korean', '2025-08-01'),
        ('ru', 'Russian', '2025-08-02');
        
        INSERT INTO Production_ProductDescription VALUES 
        (DEFAULT, 'Chromoly steel.', 'guid-desc-001', '2025-07-31'),
        (DEFAULT, 'Aluminum alloy cups; large diameter spindle.', 'guid-desc-002', '2025-08-01'),
        (DEFAULT, 'Aluminum alloy cups and a hollow axle.', 'guid-desc-003', '2025-08-02'),
        (DEFAULT, 'Aluminum alloy cups.', 'guid-desc-004', '2025-08-03'),
        (DEFAULT, 'Aluminum alloy, large diameter spindle.', 'guid-desc-005', '2025-08-04'),
        (DEFAULT, 'Aluminum alloy.', 'guid-desc-006', '2025-08-05'),
        (DEFAULT, 'Aluminum alloy, lightweight.', 'guid-desc-007', '2025-08-06'),
        (DEFAULT, 'Steel, lightweight.', 'guid-desc-008', '2025-07-31'),
        (DEFAULT, 'Carbon fiber.', 'guid-desc-009', '2025-08-01'),
        (DEFAULT, 'Titanium.', 'guid-desc-010', '2025-08-02');
        
        INSERT INTO Production_Location VALUES 
        (DEFAULT, 'Tool Crib', 0.00, 0.00, '2025-07-31'),
        (DEFAULT, 'Sheet Metal Racks', 0.00, 0.00, '2025-08-01'),
        (DEFAULT, 'Paint Shop', 14.50, 8.00, '2025-08-02'),
        (DEFAULT, 'Paint Storage', 0.00, 0.00, '2025-08-03'),
        (DEFAULT, 'Metal Storage', 0.00, 0.00, '2025-08-04'),
        (DEFAULT, 'Miscellaneous Storage', 0.00, 0.00, '2025-08-05'),
        (DEFAULT, 'Finished Goods Storage', 0.00, 0.00, '2025-08-06'),
        (DEFAULT, 'Frame Forming', 15.75, 8.00, '2025-07-31'),
        (DEFAULT, 'Frame Welding', 25.00, 8.00, '2025-08-01'),
        (DEFAULT, 'Debur and Polish', 14.50, 8.00, '2025-08-02');
        
        INSERT INTO Production_ScrapReason VALUES 
        (DEFAULT, 'Brake assembly not within design tolerances', '2025-07-31'),
        (DEFAULT, 'Color incorrect', '2025-08-01'),
        (DEFAULT, 'Drill pattern incorrect', '2025-08-02'),
        (DEFAULT, 'Gouge in metal', '2025-08-03'),
        (DEFAULT, 'Incorrect weld', '2025-08-04'),
        (DEFAULT, 'Machine capacity exceeded', '2025-08-05'),
        (DEFAULT, 'Thermoform temperature too high', '2025-08-06'),
        (DEFAULT, 'Thermoform temperature too low', '2025-07-31'),
        (DEFAULT, 'Wheel misaligned', '2025-08-01'),
        (DEFAULT, 'Workpiece dimensions incorrect', '2025-08-02');
        
        INSERT INTO Production_Illustration VALUES 
        (DEFAULT, 'Illustration data for product 1', '2025-07-31'),
        (DEFAULT, 'Illustration data for product 2', '2025-08-01'),
        (DEFAULT, 'Illustration data for product 3', '2025-08-02'),
        (DEFAULT, 'Illustration data for product 4', '2025-08-03'),
        (DEFAULT, 'Illustration data for product 5', '2025-08-04'),
        (DEFAULT, 'Illustration data for product 6', '2025-08-05'),
        (DEFAULT, 'Illustration data for product 7', '2025-08-06'),
        (DEFAULT, 'Illustration data for product 8', '2025-07-31'),
        (DEFAULT, 'Illustration data for product 9', '2025-08-01'),
        (DEFAULT, 'Illustration data for product 10', '2025-08-02');
        
        INSERT INTO Production_Product VALUES 
        (DEFAULT, 'Adjustable Race', 'AR-5381', FALSE, FALSE, NULL, 75, 54, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-001', '2025-07-31'),
        (DEFAULT, 'Bearing Ball', 'BA-8327', FALSE, FALSE, NULL, 4, 3, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-002', '2025-08-01'),
        (DEFAULT, 'BB Ball Bearing', 'BE-2349', TRUE, FALSE, NULL, 800, 600, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-003', '2025-08-02'),
        (DEFAULT, 'Headset Ball Bearings', 'BE-2908', FALSE, FALSE, NULL, 800, 600, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-004', '2025-08-03'),
        (DEFAULT, 'Blade', 'BL-2036', TRUE, FALSE, NULL, 4, 3, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-005', '2025-08-04'),
        (DEFAULT, 'LL Crankarm', 'CA-5965', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-006', '2025-08-05'),
        (DEFAULT, 'ML Crankarm', 'CA-6738', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-007', '2025-08-06'),
        (DEFAULT, 'HL Crankarm', 'CA-7457', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-008', '2025-07-31'),
        (DEFAULT, 'Chainring Bolts', 'CB-2903', FALSE, FALSE, 'Silver', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-009', '2025-08-01'),
        (DEFAULT, 'Chainring Nut', 'CN-6137', FALSE, FALSE, 'Silver', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2025-07-31', '2025-08-06', NULL, 'guid-prod-010', '2025-08-02');
        
        -- HumanResources Schema
        INSERT INTO HumanResources_Department VALUES 
        (DEFAULT, 'Engineering', 'Research and Development', '2025-07-31'),
        (DEFAULT, 'Tool Design', 'Research and Development', '2025-08-01'),
        (DEFAULT, 'Sales', 'Sales and Marketing', '2025-08-02'),
        (DEFAULT, 'Marketing', 'Sales and Marketing', '2025-08-03'),
        (DEFAULT, 'Purchasing', 'Inventory Management', '2025-08-04'),
        (DEFAULT, 'Research and Development', 'Research and Development', '2025-08-05'),
        (DEFAULT, 'Production', 'Manufacturing', '2025-08-06'),
        (DEFAULT, 'Production Control', 'Manufacturing', '2025-07-31'),
        (DEFAULT, 'Human Resources', 'Executive General and Administration', '2025-08-01'),
        (DEFAULT, 'Finance', 'Executive General and Administration', '2025-08-02');
        
        INSERT INTO HumanResources_Shift VALUES 
        (DEFAULT, 'Day', '07:00:00', '15:30:00', '2025-07-31'),
        (DEFAULT, 'Evening', '15:30:00', '23:30:00', '2025-08-01'),
        (DEFAULT, 'Night', '23:30:00', '07:00:00', '2025-08-02'),
        (DEFAULT, 'Weekend Day', '08:00:00', '16:00:00', '2025-08-03'),
        (DEFAULT, 'Weekend Evening', '16:00:00', '00:00:00', '2025-08-04'),
        (DEFAULT, 'Holiday', '09:00:00', '17:00:00', '2025-08-05'),
        (DEFAULT, 'Overtime', '17:00:00', '21:00:00', '2025-08-06'),
        (DEFAULT, 'Early Morning', '05:00:00', '13:00:00', '2025-07-31'),
        (DEFAULT, 'Late Evening', '21:00:00', '05:00:00', '2025-08-01'),
        (DEFAULT, 'Flexible', '06:00:00', '18:00:00', '2025-08-02');
        
        INSERT INTO HumanResources_Employee VALUES 
        (1, '295847284', 'adventure-works\\ken0', NULL, NULL, 'Chief Executive Officer', '1969-01-29', 'S', 'M', '2009-01-14', TRUE, 99, 69, TRUE, 'guid-emp-001', '2025-07-31'),
        (2, '245797967', 'adventure-works\\terri0', NULL, NULL, 'Vice President of Engineering', '1971-08-01', 'S', 'F', '2008-01-31', TRUE, 1, 20, TRUE, 'guid-emp-002', '2025-08-01'),
        (3, '509647174', 'adventure-works\\roberto0', NULL, NULL, 'Engineering Manager', '1974-11-12', 'M', 'M', '2007-11-11', TRUE, 2, 21, TRUE, 'guid-emp-003', '2025-08-02'),
        (4, '112457891', 'adventure-works\\rob0', NULL, NULL, 'Senior Tool Designer', '1965-01-23', 'S', 'M', '2006-01-05', TRUE, 48, 80, TRUE, 'guid-emp-004', '2025-08-03'),
        (5, '695256908', 'adventure-works\\thierry0', NULL, NULL, 'Tool Designer', '1949-08-29', 'M', 'M', '2006-01-05', TRUE, 5, 40, TRUE, 'guid-emp-005', '2025-08-04'),
        (6, '998320692', 'adventure-works\\david0', NULL, NULL, 'Marketing Manager', '1963-07-09', 'S', 'M', '2005-03-11', TRUE, 57, 41, TRUE, 'guid-emp-006', '2025-08-05'),
        (7, '134969118', 'adventure-works\\sara0', NULL, NULL, 'Marketing Specialist', '1952-09-05', 'M', 'F', '2005-03-11', TRUE, 72, 26, TRUE, 'guid-emp-007', '2025-08-06'),
        (8, '811994146', 'adventure-works\\james0', NULL, NULL, 'Vice President of Sales', '1959-09-01', 'M', 'M', '2005-07-01', TRUE, 22, 45, TRUE, 'guid-emp-008', '2025-07-31'),
        (9, '658797903', 'adventure-works\\peter0', NULL, NULL, 'Sales Representative', '1958-12-13', 'S', 'M', '2005-07-01', TRUE, 40, 41, TRUE, 'guid-emp-009', '2025-08-01'),
        (10, '879342154', 'adventure-works\\guy0', NULL, NULL, 'Sales Representative', '1956-01-09', 'M', 'M', '2005-07-01', TRUE, 3, 14, TRUE, 'guid-emp-010', '2025-08-02');
        
        INSERT INTO HumanResources_JobCandidate VALUES 
        (DEFAULT, 1, 'Resume XML data for candidate 1', '2025-07-31'),
        (DEFAULT, 2, 'Resume XML data for candidate 2', '2025-08-01'),
        (DEFAULT, 3, 'Resume XML data for candidate 3', '2025-08-02'),
        (DEFAULT, NULL, 'Resume XML data for candidate 4', '2025-08-03'),
        (DEFAULT, NULL, 'Resume XML data for candidate 5', '2025-08-04'),
        (DEFAULT, NULL, 'Resume XML data for candidate 6', '2025-08-05'),
        (DEFAULT, NULL, 'Resume XML data for candidate 7', '2025-08-06'),
        (DEFAULT, NULL, 'Resume XML data for candidate 8', '2025-07-31'),
        (DEFAULT, NULL, 'Resume XML data for candidate 9', '2025-08-01'),
        (DEFAULT, NULL, 'Resume XML data for candidate 10', '2025-08-02');
        
        -- Purchasing Schema
        INSERT INTO Purchasing_ShipMethod VALUES 
        (DEFAULT, 'XRQ - TRUCK GROUND', 3.95, 0.99, 'guid-ship-001', '2025-07-31'),
        (DEFAULT, 'ZY - EXPRESS', 9.95, 1.99, 'guid-ship-002', '2025-08-01'),
        (DEFAULT, 'OVERSEAS - DELUXE', 29.95, 2.99, 'guid-ship-003', '2025-08-02'),
        (DEFAULT, 'OVERNIGHT J-FAST', 21.95, 1.29, 'guid-ship-004', '2025-08-03'),
        (DEFAULT, 'CARGO TRANSPORT 5', 8.99, 1.49, 'guid-ship-005', '2025-08-04'),
        (DEFAULT, 'AIR FREIGHT', 15.99, 2.19, 'guid-ship-006', '2025-08-05'),
        (DEFAULT, 'GROUND STANDARD', 5.99, 0.79, 'guid-ship-007', '2025-08-06'),
        (DEFAULT, 'PRIORITY MAIL', 12.99, 1.89, 'guid-ship-008', '2025-07-31'),
        (DEFAULT, 'FEDEX GROUND', 7.99, 1.09, 'guid-ship-009', '2025-08-01'),
        (DEFAULT, 'UPS GROUND', 6.99, 0.89, 'guid-ship-010', '2025-08-02');
        
        INSERT INTO Purchasing_Vendor VALUES 
        (21, 'ADVANCED0001', 'Advanced Bike Components', 1, TRUE, TRUE, NULL, '2025-07-31'),
        (22, 'TRIKES0001', 'Trikes, Inc.', 2, TRUE, TRUE, NULL, '2025-08-01'),
        (23, 'MORGAN0001', 'Morgan Bike Accessories', 1, TRUE, TRUE, NULL, '2025-08-02'),
        (24, 'BICYCLE0001', 'Bicycle Specialists', 3, TRUE, TRUE, NULL, '2025-08-03'),
        (25, 'TRAINING0001', 'Training Systems', 2, TRUE, TRUE, NULL, '2025-08-04'),
        (26, 'COMPETE0001', 'Compete Enterprises, Inc', 1, TRUE, TRUE, NULL, '2025-08-05'),
        (27, 'LITWARE0001', 'Litware, Inc.', 4, TRUE, TRUE, NULL, '2025-08-06'),
        (28, 'BIKE0001', 'Bike World', 2, TRUE, TRUE, NULL, '2025-07-31'),
        (29, 'FITNESS0001', 'Fitness Association', 3, TRUE, TRUE, NULL, '2025-08-01'),
        (30, 'CONSUMER0001', 'Consumer Cycles', 1, TRUE, TRUE, NULL, '2025-08-02');
        
        -- Sales Data with proper relationships
        INSERT INTO Sales_Customer VALUES 
        (DEFAULT, 1, NULL, 1, 'AW00000001', 'guid-cust-001', '2025-07-31'),
        (DEFAULT, 2, NULL, 2, 'AW00000002', 'guid-cust-002', '2025-08-01'),
        (DEFAULT, 3, NULL, 3, 'AW00000003', 'guid-cust-003', '2025-08-02'),
        (DEFAULT, 4, NULL, 1, 'AW00000004', 'guid-cust-004', '2025-08-03'),
        (DEFAULT, 5, NULL, 2, 'AW00000005', 'guid-cust-005', '2025-08-04'),
        (DEFAULT, 6, NULL, 3, 'AW00000006', 'guid-cust-006', '2025-08-05'),
        (DEFAULT, 7, NULL, 1, 'AW00000007', 'guid-cust-007', '2025-08-06'),
        (DEFAULT, 8, NULL, 2, 'AW00000008', 'guid-cust-008', '2025-07-31'),
        (DEFAULT, 9, NULL, 3, 'AW00000009', 'guid-cust-009', '2025-08-01'),
        (DEFAULT, 10, NULL, 1, 'AW00000010', 'guid-cust-010', '2025-08-02');
        
        INSERT INTO Sales_SalesPerson VALUES 
        (8, 1, 300000.00, 0.00, 0.012, 559697.56, 3018725.49, 'guid-sp-001', '2025-07-31'),
        (9, 2, 250000.00, 0.00, 0.015, 367832.31, 1758385.93, 'guid-sp-002', '2025-08-01'),
        (10, 3, 275000.00, 0.00, 0.013, 445832.12, 2156789.45, 'guid-sp-003', '2025-08-02'),
        (1, 1, 280000.00, 0.00, 0.014, 512345.67, 2345678.90, 'guid-sp-004', '2025-08-03'),
        (2, 2, 290000.00, 0.00, 0.016, 623456.78, 2567890.12, 'guid-sp-005', '2025-08-04'),
        (3, 3, 260000.00, 0.00, 0.011, 398765.43, 1876543.21, 'guid-sp-006', '2025-08-05'),
        (4, 1, 270000.00, 0.00, 0.012, 456789.01, 2098765.43, 'guid-sp-007', '2025-08-06'),
        (5, 2, 285000.00, 0.00, 0.015, 567890.12, 2345678.90, 'guid-sp-008', '2025-07-31'),
        (6, 3, 295000.00, 0.00, 0.017, 678901.23, 2567890.12, 'guid-sp-009', '2025-08-01'),
        (7, 1, 265000.00, 0.00, 0.010, 389012.34, 1789012.34, 'guid-sp-010', '2025-08-02');
        
        INSERT INTO Sales_SpecialOffer VALUES 
        (DEFAULT, 'No Discount', 0.00, 'No Discount', 'No Discount', '2025-07-31', '2025-12-31', 0, NULL, 'guid-so-001', '2025-07-31'),
        (DEFAULT, 'Volume Discount 11 to 14', 0.02, 'Volume Discount', 'Customer', '2025-07-31', '2025-12-31', 11, 14, 'guid-so-002', '2025-08-01'),
        (DEFAULT, 'Volume Discount 15 to 24', 0.05, 'Volume Discount', 'Customer', '2025-07-31', '2025-12-31', 15, 24, 'guid-so-003', '2025-08-02'),
        (DEFAULT, 'Volume Discount 25 to 40', 0.10, 'Volume Discount', 'Customer', '2025-07-31', '2025-12-31', 25, 40, 'guid-so-004', '2025-08-03'),
        (DEFAULT, 'Volume Discount 41 to 60', 0.15, 'Volume Discount', 'Customer', '2025-07-31', '2025-12-31', 41, 60, 'guid-so-005', '2025-08-04'),
        (DEFAULT, 'Mountain-100 Clearance Sale', 0.35, 'Discontinued Product', 'Seasonal Discount', '2025-07-31', '2025-08-06', 0, NULL, 'guid-so-006', '2025-08-05'),
        (DEFAULT, 'Sport Helmet Discount-2002', 0.10, 'Excess Inventory', 'Seasonal Discount', '2025-07-31', '2025-08-06', 0, NULL, 'guid-so-007', '2025-08-06'),
        (DEFAULT, 'Road-650 Overstock', 0.30, 'Excess Inventory', 'Excess Inventory', '2025-07-31', '2025-08-06', 0, NULL, 'guid-so-008', '2025-07-31'),
        (DEFAULT, 'Mountain Tire Sale', 0.50, 'New Product', 'New Product', '2025-07-31', '2025-08-06', 0, NULL, 'guid-so-009', '2025-08-01'),
        (DEFAULT, 'LL Road Frame Sale', 0.20, 'Discontinued Product', 'Discontinued Product', '2025-07-31', '2025-08-06', 0, NULL, 'guid-so-010', '2025-08-02');
        
        INSERT INTO Sales_SpecialOfferProduct VALUES 
        (1, 1, 'guid-sop-001', '2025-07-31'),
        (1, 2, 'guid-sop-002', '2025-08-01'),
        (1, 3, 'guid-sop-003', '2025-08-02'),
        (1, 4, 'guid-sop-004', '2025-08-03'),
        (1, 5, 'guid-sop-005', '2025-08-04'),
        (2, 1, 'guid-sop-006', '2025-08-05'),
        (2, 2, 'guid-sop-007', '2025-08-06'),
        (3, 3, 'guid-sop-008', '2025-07-31'),
        (3, 4, 'guid-sop-009', '2025-08-01'),
        (4, 5, 'guid-sop-010', '2025-08-02');
        
        INSERT INTO Sales_Store VALUES 
        (21, 'A Bike Store', 8, 'Store demographics XML', 'guid-store-001', '2025-07-31'),
        (22, 'Progressive Sports', 9, 'Store demographics XML', 'guid-store-002', '2025-08-01'),
        (23, 'Advanced Bike Components', 10, 'Store demographics XML', 'guid-store-003', '2025-08-02'),
        (24, 'Modular Cycle Systems', 1, 'Store demographics XML', 'guid-store-004', '2025-08-03'),
        (25, 'Metropolitan Sports Supply', 2, 'Store demographics XML', 'guid-store-005', '2025-08-04'),
        (26, 'Aerobic Exercise Company', 3, 'Store demographics XML', 'guid-store-006', '2025-08-05'),
        (27, 'Associated Bikes', 4, 'Store demographics XML', 'guid-store-007', '2025-08-06'),
        (28, 'Bike World', 5, 'Store demographics XML', 'guid-store-008', '2025-07-31'),
        (29, 'Bicycle Specialists', 6, 'Store demographics XML', 'guid-store-009', '2025-08-01'),
        (30, 'Compete Enterprises, Inc', 7, 'Store demographics XML', 'guid-store-010', '2025-08-02');
        
        -- Generate Sales Orders with 2025 dates
        INSERT INTO Sales_SalesOrderHeader VALUES 
        (DEFAULT, 0, '2025-07-31', '2025-08-06', '2025-08-01', 5, TRUE, 'SO43659', NULL, NULL, 1, 8, 1, 1, 1, 1, 1, NULL, NULL, 20565.6206, 1971.5149, 616.0984, 23153.2339, NULL, 'guid-soh-001', '2025-07-31'),
        (DEFAULT, 0, '2025-08-01', '2025-08-06', '2025-08-02', 5, TRUE, 'SO43660', NULL, NULL, 2, 9, 2, 2, 2, 2, 2, NULL, NULL, 1294.2529, 124.2483, 38.8276, 1457.3288, NULL, 'guid-soh-002', '2025-08-01'),
        (DEFAULT, 0, '2025-08-02', '2025-08-06', '2025-08-03', 5, TRUE, 'SO43661', NULL, NULL, 3, 10, 3, 3, 3, 3, 3, NULL, NULL, 32726.4786, 3153.7696, 985.553, 36865.8012, NULL, 'guid-soh-003', '2025-08-02'),
        (DEFAULT, 0, '2025-08-03', '2025-08-06', '2025-08-04', 5, TRUE, 'SO43662', NULL, NULL, 4, 1, 1, 4, 4, 4, 4, NULL, NULL, 28832.5289, 2775.1646, 867.2389, 32474.9324, NULL, 'guid-soh-004', '2025-08-03'),
        (DEFAULT, 0, '2025-08-04', '2025-08-06', '2025-08-05', 5, TRUE, 'SO43663', NULL, NULL, 5, 2, 2, 5, 5, 5, 5, NULL, NULL, 419.4589, 40.2681, 12.5838, 472.3108, NULL, 'guid-soh-005', '2025-08-04'),
        (DEFAULT, 0, '2025-08-05', '2025-08-06', '2025-08-06', 5, TRUE, 'SO43664', NULL, NULL, 6, 3, 3, 6, 6, 6, 6, NULL, NULL, 24432.6088, 2344.9921, 732.81, 27510.4109, NULL, 'guid-soh-006', '2025-08-05'),
        (DEFAULT, 0, '2025-08-06', '2025-08-06', '2025-08-06', 5, TRUE, 'SO43665', NULL, NULL, 7, 4, 1, 7, 7, 7, 7, NULL, NULL, 14352.7713, 1380.1073, 431.2835, 16164.1621, NULL, 'guid-soh-007', '2025-08-06'),
        (DEFAULT, 0, '2025-07-31', '2025-08-06', '2025-08-01', 5, TRUE, 'SO43666', NULL, NULL, 8, 5, 2, 8, 8, 8, 8, NULL, NULL, 5056.4896, 485.5427, 151.7321, 5693.7644, NULL, 'guid-soh-008', '2025-07-31'),
        (DEFAULT, 0, '2025-08-01', '2025-08-06', '2025-08-02', 5, TRUE, 'SO43667', NULL, NULL, 9, 6, 3, 9, 9, 9, 9, NULL, NULL, 6107.0820, 586.1909, 183.1863, 6876.4592, NULL, 'guid-soh-009', '2025-08-01'),
        (DEFAULT, 0, '2025-08-02', '2025-08-06', '2025-08-03', 5, TRUE, 'SO43668', NULL, NULL, 10, 7, 1, 10, 10, 10, 10, NULL, NULL, 35944.1562, 3456.3150, 1079.4736, 40479.9448, NULL, 'guid-soh-010', '2025-08-02');
        
        -- Generate Order Details
        INSERT INTO Sales_SalesOrderDetail VALUES 
        (1, DEFAULT, NULL, 2, 1, 1, 2024.9940, 0.00, 4049.9880, 'guid-sod-001', '2025-07-31'),
        (1, DEFAULT, NULL, 1, 2, 1, 2024.9940, 0.00, 2024.9940, 'guid-sod-002', '2025-07-31'),
        (2, DEFAULT, NULL, 1, 3, 1, 2039.9940, 0.00, 2039.9940, 'guid-sod-003', '2025-08-01'),
        (2, DEFAULT, NULL, 3, 4, 1, 28.8404, 0.00, 86.5212, 'guid-sod-004', '2025-08-01'),
        (3, DEFAULT, NULL, 1, 5, 1, 2039.9940, 0.00, 2039.9940, 'guid-sod-005', '2025-08-02'),
        (3, DEFAULT, NULL, 2, 1, 1, 2024.9940, 0.00, 4049.9880, 'guid-sod-006', '2025-08-02'),
        (4, DEFAULT, NULL, 1, 2, 1, 2024.9940, 0.00, 2024.9940, 'guid-sod-007', '2025-08-03'),
        (4, DEFAULT, NULL, 4, 3, 1, 28.8404, 0.00, 115.3616, 'guid-sod-008', '2025-08-03'),
        (5, DEFAULT, NULL, 2, 4, 1, 28.8404, 0.00, 57.6808, 'guid-sod-009', '2025-08-04'),
        (5, DEFAULT, NULL, 1, 5, 1, 2039.9940, 0.00, 2039.9940, 'guid-sod-010', '2025-08-04');
        
        -- System tables
        INSERT INTO AWBuildVersion VALUES 
        (DEFAULT, '15.0.1400.25', '2025-07-31', '2025-08-01'),
        (DEFAULT, '15.0.1401.26', '2025-08-02', '2025-08-03'),
        (DEFAULT, '15.0.1402.27', '2025-08-04', '2025-08-05');
        
        INSERT INTO DatabaseLog VALUES 
        (DEFAULT, '2025-07-31', 'system_user', 'CREATE TABLE', 'dbo', 'TestTable1', 'CREATE TABLE TestTable1 (ID INT)', '<Event>CREATE TABLE</Event>'),
        (DEFAULT, '2025-08-01', 'system_user', 'CREATE TABLE', 'dbo', 'TestTable2', 'CREATE TABLE TestTable2 (ID INT)', '<Event>CREATE TABLE</Event>'),
        (DEFAULT, '2025-08-02', 'system_user', 'CREATE TABLE', 'dbo', 'TestTable3', 'CREATE TABLE TestTable3 (ID INT)', '<Event>CREATE TABLE</Event>'),
        (DEFAULT, '2025-08-03', 'system_user', 'CREATE TABLE', 'dbo', 'TestTable4', 'CREATE TABLE TestTable4 (ID INT)', '<Event>CREATE TABLE</Event>'),
        (DEFAULT, '2025-08-04', 'system_user', 'CREATE TABLE', 'dbo', 'TestTable5', 'CREATE TABLE TestTable5 (ID INT)', '<Event>CREATE TABLE</Event>');
        
        INSERT INTO ErrorLog VALUES 
        (DEFAULT, '2025-07-31', 'user1', 2, 16, 1, 'procedure1', 1, 'Error message 1'),
        (DEFAULT, '2025-08-01', 'user2', 3, 17, 2, 'procedure2', 2, 'Error message 2'),
        (DEFAULT, '2025-08-02', 'user3', 4, 18, 3, 'procedure3', 3, 'Error message 3'),
        (DEFAULT, '2025-08-03', 'user4', 5, 19, 4, 'procedure4', 4, 'Error message 4'),
        (DEFAULT, '2025-08-04', 'user5', 6, 20, 5, 'procedure5', 5, 'Error message 5');
        
        -- Create comprehensive summary
        CREATE OR REPLACE TABLE DATA_POPULATION_SUMMARY_FIXED AS
        SELECT 
            'ALL AdventureWorks tables populated with 2025 raw data (2025-07-31 to 2025-08-06)' as status,
            (SELECT COUNT(*) FROM Sales_SalesOrderHeader) as total_orders,
            (SELECT COUNT(*) FROM Production_Product) as total_products,
            (SELECT COUNT(*) FROM Person_Person) as total_people,
            (SELECT COUNT(*) FROM HumanResources_Employee) as total_employees,
            (SELECT COUNT(*) FROM Sales_Customer) as total_customers,
            (SELECT COUNT(*) FROM Purchasing_Vendor) as total_vendors,
            (SELECT MIN(OrderDate) FROM Sales_SalesOrderHeader) as earliest_order_date,
            (SELECT MAX(OrderDate) FROM Sales_SalesOrderHeader) as latest_order_date;
        """,
    )

populate_all_tables_2025_fixed()