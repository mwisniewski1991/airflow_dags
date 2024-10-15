from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

@dag(
    dag_id='postgresql_backup',
    start_date=datetime(2024, 10, 15),
    schedule='0 0 * * *',
    catchup=False
)
def postgresql_backup_dag():
    
    @task.sensor(poke_interval=60*60, timeout=60*60*24, mode='poke', soft_fail=True)
    def check_last_update():
        from pytz import timezone

        query = '''
            SELECT DATE(MAX(updated_at_cet)) 
            FROM schema.table_name
        ''' 
        connection = PostgresHook(postgres_conn_id='database_name').get_conn()
        cursor = connection.cursor()

        cursor.execute(query)
        records = cursor.fetchone()
        max_updated_at_cet = records[0]

        cet = timezone('CET')
        current_date = datetime.now(cet).date()
    
        if current_date == max_updated_at_cet:
            condition_met = True
            operator_return_value = max_updated_at_cet
        else:
            condition_met = False
            operator_return_value = max_updated_at_cet

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)


    @task()
    def backup_postgresql(checking_result):
        import subprocess
        from pytz import timezone

        cet = timezone('CET')
        current_date_string = datetime.now(cet).strftime("%Y-%m-%d_%H-%M-%S")

        hook = PostgresHook(postgres_conn_id='database_name')
        postgres_uri = hook.get_uri()

        backup_directory = 'temp'
        backup_file_name = f'backup_{current_date_string}.sql'

        backup_command = f'pg_dump "{postgres_uri}" > {backup_directory}/{backup_file_name}'
        subprocess.Popen(backup_command, shell=True)

    checking_result = check_last_update()
    backup_postgresql(checking_result)


postgresql_backup_dag()