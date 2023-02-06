from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# from operators.tweet_operator import run_tweet_elt

default_arguments = {'owner': 'Ibrahima',
                     'email': 'fibrahimabirane@ept.sn',
                     'start_date': datetime(2020, 1, 20)
                     }
etl_dag = DAG('tweet_analysis_dag', default_args=default_arguments )

scrap_tweet_toPg = SimpleHttpOperator(
    task_id='scrap_tweet',
    method='POST',
    http_conn_id='http_default', # Connection ID defined in Airflow's UI
    endpoint='http://localhost:5000/scrapToPostgres',
    data={"track": "value1"},
    headers={'Content-Type': 'application/json'},
    dag=etl_dag
)

load_topic_toDB = SimpleHttpOperator(
    task_id='load_topic',
    method='GET',
    http_conn_id='http_default', # Connection ID defined in Airflow's UI
    endpoint='http://localhost:5000/loadTopicToDB',
    data={},
    headers={'Content-Type': 'application/json'},
    dag=etl_dag
)

load_sentiment_toDB = SimpleHttpOperator(
    task_id='load_topic_t',
    method='POST',
    http_conn_id='http_default', # Connection ID defined in Airflow's UI
    endpoint='http://localhost:5000/loadTopicToDB',
    data={},
    headers={'Content-Type': 'application/json'},
    dag=etl_dag
)

scrap_tweet_toPg >> load_sentiment_toDB >> load_topic_toDB




#
# run_tweet_elt = PythonOperator(
#     task_id='complete_tweet_etl',
#     python_callable=run_tweet_elt,
#     dag=etl_dag
# )
#
# run_tweet_elt
