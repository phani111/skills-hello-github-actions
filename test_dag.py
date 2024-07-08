# Define dag
with models.DAG(
    dag_id='ap-verona-mi-deposits_history',
    start_date=pendulum.datetime(2022, 11, 14, tz="Europe/London"),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['ephemeral', 'veronaopsmi', 'bq_load'],
) as dag:

    table_task_groups = []

    for table in TABLES:
        with TaskGroup(group_id=f'process_{table}') as tg:
            gcs_bq_cleaning = dataproc.DataprocSubmitJobOperator(
                task_id=f"bq_full_refresh_{table}",
                job=create_pyspark_job(table),
                project_id=PROJECT_ID,
                region=REGION,
                on_failure_callback=task_failure_callback,  # Add failure callback
            )

            pig_task = dataproc.DataprocSubmitJobOperator(
                task_id=f"pig_task_{table}",
                job=create_pig_job_script(table),
                project_id=PROJECT_ID,
                region=REGION,
                asynchronous=False,
                on_failure_callback=task_failure_callback,  # Add failure callback
            )

            pig_ok = bash_operator.BashOperator(
                task_id=f'pig_ok_{table}',
                bash_command=f'echo verona {table} history successful',
                on_failure_callback=task_failure_callback,  # Add failure callback
            )

            gcs_bq_cleaning >> pig_task >> pig_ok

        table_task_groups.append(tg)

    final_task = bash_operator.BashOperator(
        task_id='final_task',
        bash_command='echo "All tables processed successfully"',
        on_failure_callback=task_failure_callback,  # Add failure callback
    )

    table_task_groups >> final_task
