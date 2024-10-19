from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
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

        return {'backup_directory': backup_directory, 'backup_file_name': backup_file_name}

    @task()
    def send_file_to_nas(backup_info):
        
        backup_directory = backup_info['backup_directory']
        backup_file_name = backup_info['backup_file_name']
        remote_directory = '/path/to/remote/directory'
    
        local_path = f'{backup_directory}/{backup_file_name}'
        remote_path = f'{remote_directory}{backup_file_name}'

        sftp_hook = SFTPHook(ssh_conn_id='sftp_connection_id')

        try:
            sftp_hook.store_file(remote_path, local_path)

        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        finally:
            sftp_hook.close_conn()


    checking_result = check_last_update()
    backup_info = backup_postgresql(checking_result)
    send_file_to_nas(backup_info)


postgresql_backup_dag()