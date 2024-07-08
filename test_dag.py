
# --- Failure Callback ---
def task_failure_callback(context):
    task_instance = context['task_instance']
    table_name = task_instance.task_id.split('_')[-1]
    log_text = f"Task {task_instance.task_id} for table {table_name} failed."
    logging.error(log_text)
    task_instance.xcom_push(key='failed_table', value=table_name)

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

    # Final Task to summarize failed tables
    def summarize_failures(**kwargs):
        ti = kwargs['ti']
        failed_tables = ti.xcom_pull(key='failed_table', task_ids=[f'process_{table}.pig_task_{table}' for table in TABLES])
        failed_tables = [table for table in failed_tables if table]  # Filter out None values
        if failed_tables:
            logging.info(f"The following tables failed to load: {', '.join(failed_tables)}")
        else:
            logging.info("All tables loaded successfully.")

    final_task = PythonOperator(
        task_id='final_task',
        python_callable=summarize_failures,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensure this task runs regardless of upstream task outcomes
    )

    # Ensure all task groups are completed before running the final task
    table_task_groups >> final_task
