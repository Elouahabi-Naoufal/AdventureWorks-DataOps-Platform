# Commit Messages for Airflow DAGs

## Daily Sales ETL DAG
```
feat: Add daily sales ETL pipeline with Airflow 3 decorators

- Implement daily_sales_etl DAG with @dag decorator syntax
- Extract sales data from SALES_SALESORDERHEADER and SALES_SALESORDERDETAIL
- Aggregate by product, territory, and salesperson for previous day
- Load into dbt_warehouse.daily_sales_summary table
- Include data validation, quality checks, and comprehensive logging
- Use ACCOUNTADMIN role with proper database/schema context
- Handle edge cases with no data gracefully
- Add retry logic and error handling for production readiness
```

## Sales Territory Analysis DAG
```
feat: Add weekly sales territory analysis with conditional processing

- Implement sales_territory_analysis DAG for weekly territory metrics
- Extract from SALES_SALESORDERHEADER, SALES_SALESORDERDETAIL, SALES_SALESTERRITORY
- Aggregate last 7 days data by territory with revenue and order metrics
- Load into dbt_warehouse.weekly_sales_territory_metrics table
- Add conditional processing to handle periods with no data
- Generate territory performance insights and rankings
- Include comprehensive validation and logging
- Use upsert pattern with week-based data cleanup
- Implement production-ready error handling and monitoring
```

## Combined Commit Message
```
feat: Add production-ready sales analytics DAGs with Airflow 3

- Add daily_sales_etl: Daily aggregation by product/territory/salesperson
- Add sales_territory_analysis: Weekly territory performance metrics
- Both DAGs use Airflow 3 @dag decorator syntax
- Implement conditional processing for no-data scenarios
- Include comprehensive data validation and quality checks
- Add proper error handling, retries, and logging
- Use ACCOUNTADMIN role with structured database access
- Support both SQL operators and Python tasks for flexibility
- Create supporting tables for ETL logging and monitoring
```