# Daily Sales ETL Pipeline - Comprehensive Explanation

## Overview

This document provides a detailed explanation of the `daily_sales_etl.py` pipeline, which implements a comprehensive Extract, Transform, Load (ETL) process for sales data using Apache Airflow 3.0 with modern decorators and SQL operators.

## Architecture & Design Decisions

### 1. **Airflow 3.0 Features Used**
- **Decorators**: Uses `@dag` and `@task` decorators for cleaner, more Pythonic code
- **Modern Syntax**: Leverages Airflow 3.0's improved task definition patterns
- **Connection Management**: Uses `snowflake_conn` connection ID for Snowflake connectivity

### 2. **SQL Operators Instead of Snowflake Operators**
- **SQLExecuteQueryOperator**: Used instead of SnowflakeOperator for better portability
- **Database Agnostic**: Can be easily adapted to other SQL databases
- **Simplified Dependencies**: Reduces the need for Snowflake-specific packages

### 3. **Data Architecture**
```
Source: dbt_db.dbt_schema (AdventureWorks tables)
    ↓
Staging: db_wh.staging (Temporary processing)
    ↓
Warehouse: db_wh.sales_mart (Final analytics tables)
```

## Pipeline Workflow

### Phase 1: Infrastructure Setup

#### Task 1: `create_staging_tables`
**Purpose**: Creates temporary staging tables for data processing

**What it does**:
- Creates `db_wh.staging` schema if it doesn't exist
- Creates three staging tables:
  - `stg_sales_orders`: Temporary storage for sales order headers
  - `stg_sales_order_details`: Temporary storage for order line items
  - `stg_customers`: Temporary storage for customer data with person details

**Why staging tables**:
- **Data Isolation**: Keeps raw extracted data separate from final warehouse
- **Error Recovery**: Allows rollback if transformation fails
- **Performance**: Enables parallel processing of different data sets
- **Data Quality**: Provides checkpoint for validation before final load

### Phase 2: Data Extraction

#### Task 2: `extract_sales_orders`
**Purpose**: Extracts sales order header data from source system

**Source Table**: `dbt_db.dbt_schema.Sales_SalesOrderHeader`

**Key Features**:
- **Incremental Loading**: Only extracts orders from last 7 days or recently modified
- **Complete Order Info**: Captures all essential order attributes
- **Business Context**: Includes customer, salesperson, and territory relationships

**Fields Extracted**:
- Order identification (SalesOrderID, SalesOrderNumber)
- Dates (OrderDate, DueDate, ShipDate)
- Financial totals (SubTotal, TaxAmt, Freight, TotalDue)
- Business relationships (CustomerID, SalesPersonID, TerritoryID)

#### Task 3: `extract_sales_order_details`
**Purpose**: Extracts individual line items for each sales order

**Source Table**: `dbt_db.dbt_schema.Sales_SalesOrderDetail`

**Key Features**:
- **Referential Integrity**: Only extracts details for orders in staging
- **Product Information**: Links to product catalog
- **Pricing Details**: Captures unit prices, discounts, and line totals

**Business Logic**:
- Joins with staging orders to ensure data consistency
- Maintains order-to-detail relationships
- Preserves pricing and discount information

#### Task 4: `extract_customers`
**Purpose**: Extracts customer data with enriched person information

**Source Tables**:
- `dbt_db.dbt_schema.Sales_Customer` (main customer data)
- `dbt_db.dbt_schema.Person_Person` (person details)
- `dbt_db.dbt_schema.Person_EmailAddress` (contact information)

**Data Enrichment**:
- Combines customer records with person details
- Adds email addresses for communication
- Only extracts customers who have orders in the current batch

### Phase 3: Warehouse Structure Creation

#### Task 5: `create_warehouse_tables`
**Purpose**: Creates the final data warehouse tables for analytics

**Tables Created**:

1. **`fact_sales`** (Fact Table)
   - Central table for sales analytics
   - Contains measures (quantities, amounts, prices)
   - Links to dimension tables via foreign keys
   - Includes calculated fields (discount amounts, net sales)

2. **`dim_customer`** (Dimension Table)
   - Customer master data
   - Includes derived attributes (full name, customer type)
   - Supports customer analytics and segmentation

3. **`daily_sales_summary`** (Aggregate Table)
   - Pre-calculated daily metrics
   - Improves query performance for reporting
   - Includes KPIs like average order value, total sales

### Phase 4: Data Transformation & Loading

#### Task 6: `transform_load_sales_fact`
**Purpose**: Transforms raw data and loads into the sales fact table

**Business Transformations**:
- **Discount Calculation**: `unit_price * order_qty * unit_price_discount`
- **Net Sales**: Line total after applying discounts
- **Data Quality**: Filters out records with null order dates

**Analytical Value**:
- Enables sales performance analysis
- Supports product profitability analysis
- Facilitates territory and salesperson performance tracking

#### Task 7: `transform_load_customer_dim`
**Purpose**: Creates enriched customer dimension

**Data Transformations**:
- **Full Name Creation**: Combines first and last names intelligently
- **Customer Type Classification**:
  - "Business" for customers with store associations
  - "Individual" for person-based customers
  - "Unknown" for incomplete data

**Business Value**:
- Enables customer segmentation
- Supports personalized marketing
- Facilitates customer lifetime value analysis

#### Task 8: `generate_daily_summary`
**Purpose**: Creates daily aggregated metrics for fast reporting

**Metrics Calculated**:
- **Volume Metrics**: Total orders, customers, products
- **Financial Metrics**: Gross sales, discounts, net sales, tax, freight
- **Performance Metrics**: Average order value
- **Segmentation**: By territory and date

**Business Benefits**:
- Fast dashboard queries
- Trend analysis capabilities
- Executive reporting support

### Phase 5: Quality Assurance

#### Task 9: `perform_data_quality_checks`
**Purpose**: Validates data integrity and business rules

**Quality Checks Performed**:

1. **Record Count Validation**
   - Ensures no data loss during transformation
   - Compares staging vs. final table counts

2. **Null Value Validation**
   - Checks critical fields (sales_order_id, customer_id, product_id)
   - Prevents incomplete records in analytics

3. **Business Rule Validation**
   - Identifies negative sales amounts
   - Logs warnings for investigation

**Error Handling**:
- Raises exceptions for critical failures
- Logs warnings for business anomalies
- Provides detailed error messages for debugging

### Phase 6: Optimization & Maintenance

#### Task 10: `update_statistics`
**Purpose**: Optimizes query performance and maintains metadata

**Performance Optimization**:
- Updates table statistics for query optimizer
- Improves query execution plans
- Reduces query response times

**Metadata Management**:
- Tracks ETL execution history
- Records table update timestamps
- Maintains data lineage information

#### Task 11: `cleanup_staging`
**Purpose**: Cleans up temporary data to free resources

**Resource Management**:
- Truncates staging tables
- Frees up storage space
- Prepares for next ETL run

## Data Flow Diagram

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Source DB     │    │   Staging Area   │    │  Data Warehouse │
│  dbt_db.        │    │  db_wh.staging   │    │  db_wh.         │
│  dbt_schema     │    │                  │    │  sales_mart     │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Sales_          │───▶│ stg_sales_       │───▶│ fact_sales      │
│ SalesOrderHeader│    │ orders           │    │                 │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Sales_          │───▶│ stg_sales_order_ │───▶│ (joined with    │
│ SalesOrderDetail│    │ details          │    │  fact_sales)    │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Sales_Customer  │───▶│ stg_customers    │───▶│ dim_customer    │
│ Person_Person   │    │                  │    │                 │
│ Person_Email... │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    ├─────────────────┤
                                               │ daily_sales_    │
                                               │ summary         │
                                               └─────────────────┘
```

## Configuration & Deployment

### Connection Setup
1. **Snowflake Connection**: Configure `snowflake_conn` in Airflow UI
2. **Database Access**: Ensure access to `dbt_db.dbt_schema` (source) and `db_wh` (target)
3. **Permissions**: Grant CREATE, INSERT, UPDATE, DELETE permissions

### Scheduling
- **Frequency**: Daily at 2:00 AM
- **Catchup**: Disabled to prevent historical backfill
- **Max Active Runs**: 1 to prevent overlapping executions

### Monitoring & Alerting
- **Retries**: 2 attempts with 5-minute delays
- **Logging**: Comprehensive logging at each step
- **Data Quality**: Automated validation with exception handling

## Business Value

### 1. **Operational Efficiency**
- Automated daily processing reduces manual effort
- Consistent data availability for business users
- Reliable data pipeline with error handling

### 2. **Analytics Enablement**
- Clean, structured data for BI tools
- Pre-calculated metrics for fast reporting
- Historical data preservation for trend analysis

### 3. **Data Quality**
- Built-in validation ensures data integrity
- Staging approach allows for data verification
- Comprehensive error handling and logging

### 4. **Scalability**
- Modular design allows for easy extension
- Incremental loading reduces processing time
- Optimized for large data volumes

## Maintenance & Troubleshooting

### Common Issues
1. **Connection Failures**: Check Snowflake connection configuration
2. **Data Quality Failures**: Review source data for anomalies
3. **Performance Issues**: Monitor table statistics and query plans

### Monitoring Points
- Task execution times
- Record counts at each stage
- Data quality check results
- Resource utilization

This ETL pipeline provides a robust, scalable solution for processing sales data while maintaining high standards for data quality, performance, and reliability.