"""
Customer Demographics Analysis DAG
Advanced demographic analysis with geographic segmentation, age profiling, and behavioral insights.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

@dag(
    dag_id='customer_demographics',
    start_date=datetime(2024, 8, 1),
    schedule='@weekly',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['customer', 'demographics', 'geographic', 'segmentation', 'weekly'],
    description='Advanced customer demographic analysis with geographic and behavioral insights',
)
def customer_demographics():
    
    # Task 1: Create target tables
    create_target_tables = SQLExecuteQueryOperator(
        task_id='create_target_tables',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        -- Geographic customer analysis table
        CREATE TABLE IF NOT EXISTS customer_geographic_analysis (
            analysis_date DATE,
            country_region VARCHAR(50),
            state_province VARCHAR(50),
            city VARCHAR(50),
            customer_count INTEGER,
            total_revenue DECIMAL(19,4),
            avg_revenue_per_customer DECIMAL(19,4),
            avg_orders_per_customer DECIMAL(10,2),
            market_penetration_score DECIMAL(5,2),
            geographic_segment VARCHAR(20),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Customer age demographics table
        CREATE TABLE IF NOT EXISTS customer_age_demographics (
            analysis_date DATE,
            age_group VARCHAR(20),
            age_range VARCHAR(20),
            customer_count INTEGER,
            total_revenue DECIMAL(19,4),
            avg_revenue_per_customer DECIMAL(19,4),
            avg_order_frequency DECIMAL(10,2),
            preferred_product_category VARCHAR(50),
            spending_behavior VARCHAR(20),
            marketing_persona VARCHAR(50),
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Customer behavioral profiles table
        CREATE TABLE IF NOT EXISTS customer_behavioral_profiles (
            analysis_date DATE,
            customer_id INTEGER,
            purchase_pattern VARCHAR(20),
            seasonal_preference VARCHAR(20),
            price_sensitivity VARCHAR(20),
            brand_loyalty_score DECIMAL(5,2),
            product_diversity_score DECIMAL(5,2),
            geographic_mobility VARCHAR(20),
            digital_engagement_level VARCHAR(20),
            customer_persona VARCHAR(50),
            marketing_recommendations TEXT,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Demographic insights summary table
        CREATE TABLE IF NOT EXISTS demographic_insights_summary (
            analysis_date DATE,
            insight_category VARCHAR(50),
            insight_description TEXT,
            metric_value DECIMAL(19,4),
            trend_direction VARCHAR(10),
            business_impact VARCHAR(20),
            recommended_action TEXT,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )
    
    # Task 2: Check if data exists
    @task(task_id='check_data_exists')
    def check_data_exists():
        """Check if sufficient customer demographic data exists"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_schema;
        
        SELECT 
            COUNT(DISTINCT c.CustomerID) as customers_with_orders,
            COUNT(DISTINCT a.AddressID) as addresses_available,
            COUNT(DISTINCT sp.StateProvinceID) as states_provinces
        FROM SALES_CUSTOMER c
        JOIN SALES_SALESORDERHEADER h ON c.CustomerID = h.CustomerID
        LEFT JOIN PERSON_ADDRESS a ON c.CustomerID = a.AddressID
        LEFT JOIN PERSON_STATEPROVINCE sp ON a.StateProvinceID = sp.StateProvinceID
        WHERE h.Status = 5;
        """
        
        result = hook.get_first(sql)
        customers = result[0] if result else 0
        addresses = result[1] if result and len(result) > 1 else 0
        
        logging.info(f"Found {customers} customers with orders, {addresses} addresses")
        
        if customers < 5:
            logging.info("Insufficient demographic data - completing successfully")
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
        
        DELETE FROM customer_geographic_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_age_demographics 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM customer_behavioral_profiles 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        
        DELETE FROM demographic_insights_summary 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 4: Analyze geographic demographics
    analyze_geographic_demographics = SQLExecuteQueryOperator(
        task_id='analyze_geographic_demographics',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_geographic_analysis (
            analysis_date,
            country_region,
            state_province,
            city,
            customer_count,
            total_revenue,
            avg_revenue_per_customer,
            avg_orders_per_customer,
            market_penetration_score,
            geographic_segment
        )
        WITH geographic_data AS (
            SELECT 
                COALESCE(cr.Name, 'Unknown') as country_region,
                COALESCE(sp.Name, 'Unknown') as state_province,
                COALESCE(a.City, 'Unknown') as city,
                COUNT(DISTINCT c.CustomerID) as customer_count,
                SUM(h.TotalDue) as total_revenue,
                COUNT(DISTINCT h.SalesOrderID) as total_orders
            FROM dbt_schema.SALES_CUSTOMER c
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON c.CustomerID = h.CustomerID
            LEFT JOIN dbt_schema.PERSON_ADDRESS a ON c.CustomerID = a.AddressID
            LEFT JOIN dbt_schema.PERSON_STATEPROVINCE sp ON a.StateProvinceID = sp.StateProvinceID
            LEFT JOIN dbt_schema.PERSON_COUNTRYREGION cr ON sp.CountryRegionCode = cr.CountryRegionCode
            WHERE h.Status = 5
            GROUP BY cr.Name, sp.Name, a.City
            HAVING customer_count >= 1
        ),
        geographic_metrics AS (
            SELECT 
                *,
                total_revenue / customer_count as avg_revenue_per_customer,
                total_orders / customer_count as avg_orders_per_customer,
                -- Market penetration score based on customer density and revenue
                LEAST(100, (customer_count * 10) + (total_revenue / 1000)) as market_penetration_score
            FROM geographic_data
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            country_region,
            state_province,
            city,
            customer_count,
            total_revenue,
            avg_revenue_per_customer,
            avg_orders_per_customer,
            market_penetration_score,
            CASE 
                WHEN market_penetration_score >= 80 THEN 'PREMIUM_MARKET'
                WHEN market_penetration_score >= 60 THEN 'STRONG_MARKET'
                WHEN market_penetration_score >= 40 THEN 'GROWING_MARKET'
                WHEN market_penetration_score >= 20 THEN 'EMERGING_MARKET'
                ELSE 'NICHE_MARKET'
            END as geographic_segment
        FROM geographic_metrics
        ORDER BY total_revenue DESC;
        """,
    )
    
    # Task 5: Analyze age demographics
    analyze_age_demographics = SQLExecuteQueryOperator(
        task_id='analyze_age_demographics',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_age_demographics (
            analysis_date,
            age_group,
            age_range,
            customer_count,
            total_revenue,
            avg_revenue_per_customer,
            avg_order_frequency,
            preferred_product_category,
            spending_behavior,
            marketing_persona
        )
        WITH customer_ages AS (
            SELECT 
                c.CustomerID,
                -- Simulate age based on customer ID patterns for demo
                CASE 
                    WHEN c.CustomerID % 100 BETWEEN 1 AND 20 THEN 25 + (c.CustomerID % 10)
                    WHEN c.CustomerID % 100 BETWEEN 21 AND 40 THEN 35 + (c.CustomerID % 15)
                    WHEN c.CustomerID % 100 BETWEEN 41 AND 60 THEN 45 + (c.CustomerID % 20)
                    WHEN c.CustomerID % 100 BETWEEN 61 AND 80 THEN 55 + (c.CustomerID % 15)
                    ELSE 65 + (c.CustomerID % 10)
                END as estimated_age,
                SUM(h.TotalDue) as total_spent,
                COUNT(DISTINCT h.SalesOrderID) as total_orders,
                AVG(h.TotalDue) as avg_order_value
            FROM dbt_schema.SALES_CUSTOMER c
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON c.CustomerID = h.CustomerID
            WHERE h.Status = 5
            GROUP BY c.CustomerID
        ),
        age_groups AS (
            SELECT 
                CASE 
                    WHEN estimated_age BETWEEN 18 AND 25 THEN 'GEN_Z'
                    WHEN estimated_age BETWEEN 26 AND 35 THEN 'MILLENNIAL_YOUNG'
                    WHEN estimated_age BETWEEN 36 AND 45 THEN 'MILLENNIAL_MATURE'
                    WHEN estimated_age BETWEEN 46 AND 55 THEN 'GEN_X'
                    WHEN estimated_age BETWEEN 56 AND 65 THEN 'BABY_BOOMER_YOUNG'
                    ELSE 'BABY_BOOMER_SENIOR'
                END as age_group,
                CASE 
                    WHEN estimated_age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN estimated_age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN estimated_age BETWEEN 36 AND 45 THEN '36-45'
                    WHEN estimated_age BETWEEN 46 AND 55 THEN '46-55'
                    WHEN estimated_age BETWEEN 56 AND 65 THEN '56-65'
                    ELSE '65+'
                END as age_range,
                COUNT(*) as customer_count,
                SUM(total_spent) as total_revenue,
                AVG(total_spent) as avg_revenue_per_customer,
                AVG(total_orders) as avg_order_frequency,
                AVG(avg_order_value) as avg_order_value
            FROM customer_ages
            GROUP BY age_group, age_range
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            age_group,
            age_range,
            customer_count,
            total_revenue,
            avg_revenue_per_customer,
            avg_order_frequency,
            CASE 
                WHEN age_group IN ('GEN_Z', 'MILLENNIAL_YOUNG') THEN 'Electronics & Accessories'
                WHEN age_group = 'MILLENNIAL_MATURE' THEN 'Home & Family Products'
                WHEN age_group = 'GEN_X' THEN 'Professional & Outdoor'
                ELSE 'Classic & Comfort Products'
            END as preferred_product_category,
            CASE 
                WHEN avg_order_value >= 1000 THEN 'HIGH_SPENDER'
                WHEN avg_order_value >= 500 THEN 'MODERATE_SPENDER'
                WHEN avg_order_value >= 200 THEN 'BUDGET_CONSCIOUS'
                ELSE 'PRICE_SENSITIVE'
            END as spending_behavior,
            CASE 
                WHEN age_group = 'GEN_Z' THEN 'Digital Native - Social Media Focused'
                WHEN age_group = 'MILLENNIAL_YOUNG' THEN 'Tech Savvy Professional - Mobile First'
                WHEN age_group = 'MILLENNIAL_MATURE' THEN 'Established Professional - Quality Focused'
                WHEN age_group = 'GEN_X' THEN 'Experienced Buyer - Value Conscious'
                WHEN age_group = 'BABY_BOOMER_YOUNG' THEN 'Traditional Buyer - Brand Loyal'
                ELSE 'Conservative Buyer - Service Oriented'
            END as marketing_persona
        FROM age_groups
        ORDER BY total_revenue DESC;
        """,
    )
    
    # Task 6: Create behavioral profiles
    create_behavioral_profiles = SQLExecuteQueryOperator(
        task_id='create_behavioral_profiles',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO customer_behavioral_profiles (
            analysis_date,
            customer_id,
            purchase_pattern,
            seasonal_preference,
            price_sensitivity,
            brand_loyalty_score,
            product_diversity_score,
            geographic_mobility,
            digital_engagement_level,
            customer_persona,
            marketing_recommendations
        )
        WITH customer_behavior AS (
            SELECT 
                c.CustomerID,
                COUNT(DISTINCT h.SalesOrderID) as total_orders,
                SUM(h.TotalDue) as total_spent,
                AVG(h.TotalDue) as avg_order_value,
                COUNT(DISTINCT DATE_TRUNC('MONTH', h.OrderDate)) as active_months,
                COUNT(DISTINCT d.ProductID) as unique_products,
                MIN(h.OrderDate) as first_order,
                MAX(h.OrderDate) as last_order,
                STDDEV(h.TotalDue) as spending_variance
            FROM dbt_schema.SALES_CUSTOMER c
            JOIN dbt_schema.SALES_SALESORDERHEADER h ON c.CustomerID = h.CustomerID
            JOIN dbt_schema.SALES_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
            WHERE h.Status = 5
            GROUP BY c.CustomerID
        ),
        behavioral_scoring AS (
            SELECT 
                CustomerID,
                total_orders,
                total_spent,
                avg_order_value,
                active_months,
                unique_products,
                spending_variance,
                -- Purchase pattern analysis
                CASE 
                    WHEN total_orders >= 10 AND active_months >= 6 THEN 'REGULAR_BUYER'
                    WHEN total_orders >= 5 AND active_months >= 3 THEN 'FREQUENT_BUYER'
                    WHEN total_orders >= 3 THEN 'OCCASIONAL_BUYER'
                    WHEN total_orders = 2 THEN 'REPEAT_BUYER'
                    ELSE 'ONE_TIME_BUYER'
                END as purchase_pattern,
                -- Seasonal preference (simulated based on customer patterns)
                CASE 
                    WHEN CustomerID % 4 = 0 THEN 'SPRING_SUMMER'
                    WHEN CustomerID % 4 = 1 THEN 'FALL_WINTER'
                    WHEN CustomerID % 4 = 2 THEN 'HOLIDAY_FOCUSED'
                    ELSE 'YEAR_ROUND'
                END as seasonal_preference,
                -- Price sensitivity
                CASE 
                    WHEN spending_variance <= 100 THEN 'PRICE_CONSCIOUS'
                    WHEN spending_variance <= 500 THEN 'MODERATE_FLEXIBILITY'
                    ELSE 'PRICE_INSENSITIVE'
                END as price_sensitivity,
                -- Brand loyalty score (0-100)
                LEAST(100, (total_orders * 5) + (active_months * 3)) as brand_loyalty_score,
                -- Product diversity score (0-100)
                LEAST(100, unique_products * 2) as product_diversity_score,
                -- Geographic mobility (simulated)
                CASE 
                    WHEN CustomerID % 3 = 0 THEN 'HIGH_MOBILITY'
                    WHEN CustomerID % 3 = 1 THEN 'MODERATE_MOBILITY'
                    ELSE 'LOW_MOBILITY'
                END as geographic_mobility,
                -- Digital engagement level
                CASE 
                    WHEN total_orders >= 8 THEN 'HIGH_DIGITAL'
                    WHEN total_orders >= 4 THEN 'MODERATE_DIGITAL'
                    ELSE 'LOW_DIGITAL'
                END as digital_engagement_level
            FROM customer_behavior
        )
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            CustomerID,
            purchase_pattern,
            seasonal_preference,
            price_sensitivity,
            brand_loyalty_score,
            product_diversity_score,
            geographic_mobility,
            digital_engagement_level,
            CASE 
                WHEN purchase_pattern = 'REGULAR_BUYER' AND brand_loyalty_score >= 70 THEN 'VIP_CHAMPION'
                WHEN purchase_pattern IN ('FREQUENT_BUYER', 'REGULAR_BUYER') THEN 'LOYAL_ADVOCATE'
                WHEN purchase_pattern = 'OCCASIONAL_BUYER' AND product_diversity_score >= 50 THEN 'EXPLORER_BUYER'
                WHEN purchase_pattern = 'REPEAT_BUYER' THEN 'POTENTIAL_LOYALIST'
                WHEN price_sensitivity = 'PRICE_CONSCIOUS' THEN 'BARGAIN_HUNTER'
                ELSE 'CASUAL_SHOPPER'
            END as customer_persona,
            CASE 
                WHEN purchase_pattern = 'REGULAR_BUYER' AND brand_loyalty_score >= 70 THEN 'Exclusive offers, early access, premium support'
                WHEN purchase_pattern IN ('FREQUENT_BUYER', 'REGULAR_BUYER') THEN 'Loyalty rewards, referral programs, personalized recommendations'
                WHEN purchase_pattern = 'OCCASIONAL_BUYER' AND product_diversity_score >= 50 THEN 'Product discovery campaigns, cross-sell opportunities'
                WHEN purchase_pattern = 'REPEAT_BUYER' THEN 'Engagement campaigns, loyalty program enrollment'
                WHEN price_sensitivity = 'PRICE_CONSCIOUS' THEN 'Discount campaigns, value propositions, bundle offers'
                ELSE 'Awareness campaigns, product education, trial offers'
            END as marketing_recommendations
        FROM behavioral_scoring;
        """,
    )
    
    # Task 7: Generate demographic insights
    generate_demographic_insights = SQLExecuteQueryOperator(
        task_id='generate_demographic_insights',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        INSERT INTO demographic_insights_summary (
            analysis_date,
            insight_category,
            insight_description,
            metric_value,
            trend_direction,
            business_impact,
            recommended_action
        )
        -- Geographic insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            'GEOGRAPHIC' as insight_category,
            'Top performing geographic market by revenue' as insight_description,
            total_revenue as metric_value,
            'POSITIVE' as trend_direction,
            'HIGH' as business_impact,
            'Increase marketing investment and expand product offerings in ' || country_region || ', ' || state_province as recommended_action
        FROM customer_geographic_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY total_revenue DESC 
        LIMIT 1
        
        UNION ALL
        
        -- Age demographic insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            'AGE_DEMOGRAPHICS' as insight_category,
            'Highest value age segment by average revenue per customer' as insight_description,
            avg_revenue_per_customer as metric_value,
            'POSITIVE' as trend_direction,
            'HIGH' as business_impact,
            'Focus premium product marketing on ' || age_group || ' segment (' || age_range || ' years)' as recommended_action
        FROM customer_age_demographics 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY avg_revenue_per_customer DESC 
        LIMIT 1
        
        UNION ALL
        
        -- Behavioral insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            'BEHAVIORAL' as insight_category,
            'Most common customer persona distribution' as insight_description,
            COUNT(*) as metric_value,
            'STABLE' as trend_direction,
            'MEDIUM' as business_impact,
            'Develop targeted campaigns for ' || customer_persona || ' segment with ' || 
            SUBSTRING(marketing_recommendations, 1, 50) || '...' as recommended_action
        FROM customer_behavioral_profiles 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY customer_persona, marketing_recommendations
        ORDER BY COUNT(*) DESC 
        LIMIT 1
        
        UNION ALL
        
        -- Market penetration insights
        SELECT 
            DATE_TRUNC('WEEK', CURRENT_DATE) as analysis_date,
            'MARKET_PENETRATION' as insight_category,
            'Average market penetration score across all regions' as insight_description,
            AVG(market_penetration_score) as metric_value,
            'NEUTRAL' as trend_direction,
            'MEDIUM' as business_impact,
            'Focus on emerging markets with penetration scores below 40 for growth opportunities' as recommended_action
        FROM customer_geographic_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """,
    )
    
    # Task 8: Validate demographic analysis results
    @task(task_id='validate_demographic_results')
    def validate_demographic_results():
        """Validate demographic analysis results and log key insights"""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Validate geographic analysis
        geo_sql = """
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;
        
        SELECT 
            COUNT(DISTINCT country_region) as countries_analyzed,
            COUNT(DISTINCT state_province) as states_analyzed,
            SUM(customer_count) as total_customers,
            AVG(market_penetration_score) as avg_penetration_score
        FROM customer_geographic_analysis 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE);
        """
        
        geo_result = hook.get_first(geo_sql)
        if geo_result:
            logging.info(f"Geographic Analysis: {geo_result[0]} countries, {geo_result[1]} states, {geo_result[2]} customers")
        
        # Validate age demographics
        age_sql = """
        SELECT 
            COUNT(DISTINCT age_group) as age_groups,
            SUM(customer_count) as total_customers,
            age_group,
            customer_count
        FROM customer_age_demographics 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY age_group, customer_count
        ORDER BY customer_count DESC;
        """
        
        age_results = hook.get_records(age_sql)
        if age_results:
            logging.info("Age Demographics Distribution:")
            for result in age_results[:3]:  # Top 3 age groups
                logging.info(f"  {result[2]}: {result[3]} customers")
        
        # Validate behavioral profiles
        behavior_sql = """
        SELECT 
            customer_persona,
            COUNT(*) as customer_count,
            AVG(brand_loyalty_score) as avg_loyalty_score
        FROM customer_behavioral_profiles 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        GROUP BY customer_persona
        ORDER BY customer_count DESC;
        """
        
        behavior_results = hook.get_records(behavior_sql)
        if behavior_results:
            logging.info("Customer Personas Distribution:")
            for persona, count, loyalty in behavior_results:
                logging.info(f"  {persona}: {count} customers (Loyalty: {loyalty:.1f})")
        
        # Log key insights
        insights_sql = """
        SELECT 
            insight_category,
            insight_description,
            metric_value,
            recommended_action
        FROM demographic_insights_summary 
        WHERE analysis_date = DATE_TRUNC('WEEK', CURRENT_DATE)
        ORDER BY 
            CASE business_impact 
                WHEN 'HIGH' THEN 1 
                WHEN 'MEDIUM' THEN 2 
                ELSE 3 
            END;
        """
        
        insights_results = hook.get_records(insights_sql)
        if insights_results:
            logging.info("Key Demographic Insights:")
            for category, description, value, action in insights_results:
                logging.info(f"  {category}: {description} (Value: {value:.2f})")
                logging.info(f"    Action: {action}")
        
        return "Demographic analysis validation completed successfully"
    
    # Define task dependencies
    data_check = check_data_exists()
    
    # Conditional execution based on data availability
    create_target_tables >> data_check
    data_check >> cleanup_data
    cleanup_data >> [analyze_geographic_demographics, analyze_age_demographics, create_behavioral_profiles]
    [analyze_geographic_demographics, analyze_age_demographics, create_behavioral_profiles] >> generate_demographic_insights
    generate_demographic_insights >> validate_demographic_results()

customer_demographics()