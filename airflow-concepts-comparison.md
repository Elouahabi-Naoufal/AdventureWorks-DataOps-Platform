# Airflow: Sensors vs Operators vs Decorators

## Overview Comparison

| Aspect | **Sensors** | **Operators** | **Decorators** |
|--------|-------------|---------------|----------------|
| **Purpose** | Wait for conditions | Execute tasks | Simplify task creation |
| **Execution** | Polling/waiting | Action-based | Function-based |
| **Return** | Success/failure | Task result | Function result |
| **Use Case** | Conditional triggers | Data processing | Python functions |

## 🔍 Sensors

**Definition**: Wait for external conditions before proceeding

### Key Characteristics:
- **Polling mechanism** - continuously check conditions
- **Blocking behavior** - hold up workflow until condition met
- **Timeout handling** - fail after specified time
- **Reschedule mode** - free up worker slots while waiting

### Common Types:
```python
# File Sensor
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/path/to/file.csv',
    poke_interval=30
)

# SQL Sensor  
sql_sensor = SqlSensor(
    task_id='wait_for_data',
    conn_id='postgres_default',
    sql="SELECT COUNT(*) FROM table WHERE date = '{{ ds }}'"
)
```

## ⚙️ Operators

**Definition**: Execute specific tasks or operations

### Key Characteristics:
- **Action-oriented** - perform actual work
- **Immediate execution** - run when triggered
- **Task completion** - finish and return result
- **Resource consumption** - use system resources during execution

### Common Types:
```python
# Bash Operator
bash_task = BashOperator(
    task_id='run_script',
    bash_command='python /path/to/script.py'
)

# Python Operator
python_task = PythonOperator(
    task_id='process_data',
    python_callable=my_function
)
```

## 🎯 Decorators

**Definition**: Transform Python functions into Airflow tasks

### Key Characteristics:
- **Function-based** - wrap existing Python functions
- **Simplified syntax** - reduce boilerplate code
- **Type hints support** - better IDE integration
- **Automatic serialization** - handle parameters seamlessly

### Common Types:
```python
from airflow.decorators import task

@task
def extract_data():
    return {"data": "extracted"}

@task
def transform_data(data):
    return {"data": "transformed"}

# Usage in DAG
extracted = extract_data()
transformed = transform_data(extracted)
```

## 🔄 When to Use Each

### Use **Sensors** when:
- Waiting for external files/data
- Checking database conditions
- Monitoring API endpoints
- Coordinating with external systems

### Use **Operators** when:
- Running shell commands
- Executing SQL queries
- Calling external APIs
- Moving/copying data

### Use **Decorators** when:
- Writing custom Python logic
- Creating reusable functions
- Simplifying task definitions
- Working with complex data flows

## 💡 Best Practices

### Sensors:
- Set appropriate `poke_interval`
- Use `mode='reschedule'` for long waits
- Implement proper timeout handling

### Operators:
- Choose specific operators over generic ones
- Handle failures gracefully
- Use templating for dynamic values

### Decorators:
- Keep functions focused and small
- Use type hints for better debugging
- Return serializable data types

## 🔗 Task Dependencies

All three can be chained together:

```python
@dag(schedule_interval='@daily')
def example_dag():
    
    # Sensor waits for condition
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/data/input.csv'
    )
    
    # Decorator processes data
    @task
    def process_file():
        return "processed"
    
    # Operator executes command
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm /tmp/temp_files/*'
    )
    
    # Chain dependencies
    wait_for_file >> process_file() >> cleanup
```

## Summary

- **Sensors**: Wait and watch 👀
- **Operators**: Do the work 🔨  
- **Decorators**: Simplify Python tasks ✨

Choose based on your specific workflow needs!