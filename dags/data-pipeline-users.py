def extract_data():
    print("Extracting data from users.csv...")
    csv_path = os.path.join(os.path.dirname(__file__), 'users.csv')
    df = pd.read_csv(csv_path)
    return df.to_dict('records')

# Define the transform function
def transform_data(**context):
    print("Transforming data...")
    # Get data from previous task
    data = context['task_instance'].xcom_pull(task_ids='extract')
    
    # Add any transformations here if needed
    # For now, we'll just pass through the data
    return data

# Define the load function
def load_data(**context):
    print("Loading data to SQLite database...")
    # Get transformed data from previous task
    data = context['task_instance'].xcom_pull(task_ids='transform')
    
    # Connect to database
    db_path = os.path.join(os.path.dirname(__file__), 'etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Insert data
    for record in data:
        cursor.execute('''
            INSERT INTO users (id, first_name, last_name, email, gender, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            record['id'],
            record['first_name'],
            record['last_name'],
            record['email'],
            record['gender'],
            record['ip_address']
        ))
    
    conn.commit()
    conn.close()
    print(f"Successfully loaded {len(data)} user records into the database")

# Define the tasks
start_task = EmptyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Add database setup task
setup_db_task = PythonOperator(
    task_id='setup_database',
    python_callable=setup_database,
    dag=dag,
)

# Define the task dependencies
start_task >> setup_db_task >> extract_task >> transform_task >> load_task >> end_task