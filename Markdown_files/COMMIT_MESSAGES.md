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

## Sales Person Performance DAG
```
feat: Add weekly sales person performance analysis DAG

- Implement sales_person_performance DAG for individual salesperson metrics
- Extract from SALES_SALESORDERHEADER and SALES_SALESORDERDETAIL tables
- Aggregate last 7 days data by salesperson with revenue and order counts
- Load into dbt_warehouse.weekly_salesperson_metrics table
- Include conditional processing for no-data scenarios
- Add comprehensive validation and performance logging
- Use week-based data cleanup and upsert patterns
- Implement production-ready error handling with retries
```

## Currency Exchange Rates DAG
```
feat: Add daily currency exchange rates processing DAG

- Implement currency_exchange_rates DAG for international sales support
- Extract from SALES_CURRENCYRATE table with daily processing
- Link currency rates with actual sales volume data
- Load into dbt_warehouse.daily_currency_rates table
- Include data availability checks and conditional processing
- Add comprehensive validation and volume tracking
- Use daily data cleanup and processing patterns
- Implement production-ready error handling and logging
```

## Sales Forecasting DAG
```
feat: Add weekly sales forecasting with trend analysis

- Implement sales_forecasting DAG for predictive analytics
- Analyze 30-day historical data from SALES_SALESORDERHEADER
- Generate forecasts by territory and product category
- Calculate trend factors comparing recent vs historical periods
- Load into dbt_warehouse.weekly_sales_forecast table
- Include confidence levels (HIGH/MEDIUM/LOW) based on data quality
- Add comprehensive validation and forecast accuracy tracking
- Use weekly processing with historical trend analysis
- Implement production-ready error handling and data quality checks
```

## Combined Sales Module Commit Message
```
feat: Complete sales analytics module with 5 production-ready DAGs

- Add daily_sales_etl: Daily aggregation by product/territory/salesperson
- Add sales_territory_analysis: Weekly territory performance metrics
- Add sales_person_performance: Weekly individual salesperson analysis
- Add currency_exchange_rates: Daily international currency processing
- Add sales_forecasting: Weekly predictive analytics with trend analysis
- All DAGs use Airflow 3 @dag decorator syntax with proper scheduling
- Implement conditional processing for no-data scenarios across all DAGs
- Include comprehensive data validation, quality checks, and logging
- Add proper error handling, retries, and production monitoring
- Use ACCOUNTADMIN role with structured database access patterns
- Support both SQL operators and Python tasks for maximum flexibility
- Create supporting tables for ETL logging and performance monitoring
- Complete sales module: 5/5 DAGs implemented and tested
```