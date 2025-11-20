import os
import json
import pyodbc
from cozepy import COZE_COM_BASE_URL
from nltk.sem.chat80 import sql_query

DB_CONFIG = {
    'Data_Source': '144.214.55.157',
    'database': 'is5413093',
    'UserID': 'is5413093',
    'Password': 'is54130932007',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

coze_api_token = 'cztei_08u7E8VCnlFEwfw41cholSGu1LgZfG46jYmJ5WXYacBbwJLqXZBDJpbwWdpQ9tW0X'
coze_api_base = COZE_COM_BASE_URL
from cozepy import Coze, TokenAuth, Stream, WorkflowEvent, WorkflowEventType  # noqa
coze = Coze(auth=TokenAuth(token=coze_api_token), base_url=coze_api_base)
workflow_id = '7572078566180913205'


def create_db_connection():
    try:
        conn_str = (
            f"SERVER={DB_CONFIG['Data_Source']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['UserID']};"
            f"PWD={DB_CONFIG['Password']};"
            f"Driver={DB_CONFIG['driver']};"
        )
        connection = pyodbc.connect(conn_str)
        return connection
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None


def execute_sql_query(sql_query):
    connection = create_db_connection()
    if not connection:
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            print("\n=== RESULT ===")
            print(" | ".join(columns))
            print("-" * 50)

            for row in results:
                print(" | ".join(str(cell) for cell in row))

            return results
        else:
            connection.commit()
            print(f"Success, rows: {cursor.rowcount}")
            return cursor.rowcount

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            connection.close()

def handle_workflow_iterator(stream: Stream[WorkflowEvent]):
    query_results = None
    for event in stream:
        if event.event == WorkflowEventType.MESSAGE:
            print("got message", event.message)
            try:
                content_json = json.loads(event.message.content)
                sql_query1 = content_json.get("output")

                if sql_query1:
                    print("\n=== SQL INQUIRE OUTPUT===")
                    print(sql_query1)
                    query_results = execute_sql_query(sql_query1)
                    print(query_results)

            except Exception as e:
                print(f"ERROR: {e}")
        elif event.event == WorkflowEventType.ERROR:
            print("ERROR FOUND WORKFLOW", event.error)
        elif event.event == WorkflowEventType.INTERRUPT:
            handle_workflow_iterator(
                coze.workflows.runs.resume(
                    workflow_id=workflow_id,
                    event_id=event.interrupt.interrupt_data.event_id,
                    resume_data="hey",
                    interrupt_type=event.interrupt.interrupt_data.type,
                )
            )
    return query_results

user_inquire = input("Please enter your inquire:")
user_query = (
handle_workflow_iterator(
    coze.workflows.runs.stream(
        workflow_id=workflow_id,
        parameters={
            "inquire": user_inquire,
        }
    )
))

