from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.http.hooks.http import HttpHook

class Dagrun_Failed_Listener(AirflowPlugin):
    class Listener:
        @hookimpl
        def on_task_instance_failed(previous_state, task_instance, session):
            message = f'''### DAG failed
            DAG ID: {task_instance.dag_id}
            Task ID: {task_instance.task_id}
            End Date UTC: {task_instance.end_date}
            '''
            
            http_hook = HttpHook(method='POST', http_conn_id='connection_id')
            http_conn = http_hook.get_connection('connection_id').extra_dejson

            topic_name = http_conn.get('topic_one')
            token = http_conn.get('token')

            response = http_hook.run(endpoint=f"/{topic_name}", data=message.encode('utf-8'), headers={'Authorization': f'Bearer {token}'})
        
            if response.status_code == 200:
                print("Message sent successfully")
            else:
                raise Exception(f"Failed to send message. Status code: {response.status_code}")



    name = "dagrun_failed_listener"
    listeners = [Listener]