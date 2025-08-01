from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id='hr_employee_analytics',
    start_date=datetime(2024, 1, 1),
    schedule='0 8 * * 1',  # Weekly on Monday
    catchup=False,
    tags=['hr', 'employees', 'payroll']
)
def hr_employee_analytics():
    
    @task
    def get_employee_demographics():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            COUNT(*) as total_employees,
            COUNT(CASE WHEN Gender = 'M' THEN 1 END) as male_count,
            COUNT(CASE WHEN Gender = 'F' THEN 1 END) as female_count,
            AVG(DATEDIFF('year', BirthDate, CURRENT_DATE)) as avg_age,
            COUNT(CASE WHEN MaritalStatus = 'M' THEN 1 END) as married_count,
            COUNT(CASE WHEN MaritalStatus = 'S' THEN 1 END) as single_count
        FROM dbt_db.dbt_schema.HumanResources_Employee
        WHERE CurrentFlag = TRUE
        """
        result = hook.get_first(sql)
        return {
            'total_employees': result[0],
            'male_count': result[1],
            'female_count': result[2],
            'avg_age': round(float(result[3]), 1),
            'married_count': result[4],
            'single_count': result[5]
        }
    
    @task
    def analyze_department_distribution():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            d.Name as department,
            d.GroupName,
            COUNT(edh.BusinessEntityID) as employee_count,
            AVG(eph.Rate) as avg_hourly_rate
        FROM dbt_db.dbt_schema.HumanResources_Department d
        JOIN dbt_db.dbt_schema.HumanResources_EmployeeDepartmentHistory edh ON d.DepartmentID = edh.DepartmentID
        JOIN dbt_db.dbt_schema.HumanResources_Employee e ON edh.BusinessEntityID = e.BusinessEntityID
        LEFT JOIN dbt_db.dbt_schema.HumanResources_EmployeePayHistory eph ON e.BusinessEntityID = eph.BusinessEntityID
        WHERE edh.EndDate IS NULL AND e.CurrentFlag = TRUE
        GROUP BY d.Name, d.GroupName
        ORDER BY employee_count DESC
        """
        results = hook.get_records(sql)
        return [{
            'department': r[0],
            'group_name': r[1],
            'employee_count': r[2],
            'avg_hourly_rate': float(r[3]) if r[3] else 0
        } for r in results]
    
    @task
    def calculate_tenure_analysis():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            e.BusinessEntityID,
            p.FirstName || ' ' || p.LastName as full_name,
            e.JobTitle,
            e.HireDate,
            DATEDIFF('year', e.HireDate, CURRENT_DATE) as years_of_service,
            CASE 
                WHEN DATEDIFF('year', e.HireDate, CURRENT_DATE) < 1 THEN 'New Hire'
                WHEN DATEDIFF('year', e.HireDate, CURRENT_DATE) < 5 THEN 'Junior'
                WHEN DATEDIFF('year', e.HireDate, CURRENT_DATE) < 10 THEN 'Mid-Level'
                ELSE 'Senior'
            END as tenure_category
        FROM dbt_db.dbt_schema.HumanResources_Employee e
        JOIN dbt_db.dbt_schema.Person_Person p ON e.BusinessEntityID = p.BusinessEntityID
        WHERE e.CurrentFlag = TRUE
        ORDER BY years_of_service DESC
        """
        results = hook.get_records(sql)
        return [{
            'employee_id': r[0],
            'full_name': r[1],
            'job_title': r[2],
            'hire_date': r[3].isoformat() if r[3] else None,
            'years_of_service': r[4],
            'tenure_category': r[5]
        } for r in results]
    
    @task
    def analyze_pay_equity(demographics, departments):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            e.Gender,
            d.GroupName,
            COUNT(*) as employee_count,
            AVG(eph.Rate) as avg_rate,
            MIN(eph.Rate) as min_rate,
            MAX(eph.Rate) as max_rate
        FROM dbt_db.dbt_schema.HumanResources_Employee e
        JOIN dbt_db.dbt_schema.HumanResources_EmployeeDepartmentHistory edh ON e.BusinessEntityID = edh.BusinessEntityID
        JOIN dbt_db.dbt_schema.HumanResources_Department d ON edh.DepartmentID = d.DepartmentID
        JOIN dbt_db.dbt_schema.HumanResources_EmployeePayHistory eph ON e.BusinessEntityID = eph.BusinessEntityID
        WHERE edh.EndDate IS NULL AND e.CurrentFlag = TRUE
        GROUP BY e.Gender, d.GroupName
        ORDER BY d.GroupName, e.Gender
        """
        results = hook.get_records(sql)
        
        pay_equity = []
        for r in results:
            pay_equity.append({
                'gender': r[0],
                'group_name': r[1],
                'employee_count': r[2],
                'avg_rate': float(r[3]),
                'min_rate': float(r[4]),
                'max_rate': float(r[5])
            })
        
        return {
            'demographics_summary': demographics,
            'department_breakdown': departments,
            'pay_equity_analysis': pay_equity
        }
    
    @task
    def identify_promotion_candidates(tenure_data):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT 
            e.BusinessEntityID,
            p.FirstName || ' ' || p.LastName as full_name,
            e.JobTitle,
            d.Name as department,
            eph.Rate as current_rate,
            e.VacationHours,
            e.SickLeaveHours,
            DATEDIFF('year', e.HireDate, CURRENT_DATE) as tenure_years
        FROM dbt_db.dbt_schema.HumanResources_Employee e
        JOIN dbt_db.dbt_schema.Person_Person p ON e.BusinessEntityID = p.BusinessEntityID
        JOIN dbt_db.dbt_schema.HumanResources_EmployeeDepartmentHistory edh ON e.BusinessEntityID = edh.BusinessEntityID
        JOIN dbt_db.dbt_schema.HumanResources_Department d ON edh.DepartmentID = d.DepartmentID
        JOIN dbt_db.dbt_schema.HumanResources_EmployeePayHistory eph ON e.BusinessEntityID = eph.BusinessEntityID
        WHERE edh.EndDate IS NULL 
        AND e.CurrentFlag = TRUE
        AND DATEDIFF('year', e.HireDate, CURRENT_DATE) >= 2
        AND e.VacationHours < 40  -- Low vacation usage indicates dedication
        ORDER BY tenure_years DESC, eph.Rate ASC
        """
        results = hook.get_records(sql)
        
        candidates = []
        for r in results[:15]:  # Top 15 candidates
            candidates.append({
                'employee_id': r[0],
                'full_name': r[1],
                'current_title': r[2],
                'department': r[3],
                'current_rate': float(r[4]),
                'vacation_hours': r[5],
                'sick_leave_hours': r[6],
                'tenure_years': r[7],
                'promotion_score': (r[7] * 10) + (40 - r[5])  # Simple scoring
            })
        
        return sorted(candidates, key=lambda x: x['promotion_score'], reverse=True)
    
    create_hr_reports = SQLExecuteQueryOperator(
        task_id='create_hr_reports',
        conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE dbt_db.dbt_schema.hr_weekly_report (
            report_id INTEGER AUTOINCREMENT PRIMARY KEY,
            report_date DATE DEFAULT CURRENT_DATE,
            total_employees INTEGER,
            avg_age DECIMAL(5,2),
            departments_count INTEGER,
            promotion_candidates_count INTEGER,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    
    @task
    def generate_executive_summary(pay_analysis, promotion_candidates):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Insert weekly report summary
        sql = """
        INSERT INTO dbt_db.dbt_schema.hr_weekly_report 
        (total_employees, avg_age, departments_count, promotion_candidates_count)
        VALUES (%s, %s, %s, %s)
        """
        hook.run(sql, parameters=[
            pay_analysis['demographics_summary']['total_employees'],
            pay_analysis['demographics_summary']['avg_age'],
            len(pay_analysis['department_breakdown']),
            len(promotion_candidates)
        ])
        
        return {
            'report_generated': True,
            'key_metrics': {
                'total_employees': pay_analysis['demographics_summary']['total_employees'],
                'departments': len(pay_analysis['department_breakdown']),
                'promotion_ready': len(promotion_candidates),
                'gender_ratio': f"{pay_analysis['demographics_summary']['male_count']}M:{pay_analysis['demographics_summary']['female_count']}F"
            }
        }
    
    # Task dependencies with multiple divergent paths
    demographics = get_employee_demographics()
    departments = analyze_department_distribution()
    tenure = calculate_tenure_analysis()
    
    # Parallel analysis branches
    pay_analysis = analyze_pay_equity(demographics, departments)
    promotion_candidates = identify_promotion_candidates(tenure)
    
    # Final convergence
    create_hr_reports >> generate_executive_summary(pay_analysis, promotion_candidates)

hr_employee_analytics()