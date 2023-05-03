from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

default_args = {
    'owner': 'k-shirochenkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 10)
}

schedule_interval = '0 12 * * *'

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20230320',
    'user': 'student',
    'password': 'dpo_python_2020'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def exercise():
    @task()
    def get_data_feed():
        query = '''
        SELECT user_id, 
                 toDate(time) as event_date,
                 gender,
                 age,
                 os,
                 countIf(action, action='like') as likes,
                 countIf(action, action='view') as views
          from simulator_20230320.feed_actions
          where toDate(time) = yesterday()
          group by user_id, event_date, gender, age, os
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def get_data_messages():
        query = '''
        SELECT event_date, user_id, gender, age, os, messages_sent, messages_received, users_sent, users_received
        FROM
          (SELECT user_id, 
                 toDate(time) as event_date,
                 gender,
                 age,
                 os,
                 count(user_id) as messages_sent,
                 count(distinct reciever_id) as users_sent
          from simulator_20230320.message_actions
          where toDate(time) = yesterday()
          group by user_id, event_date, gender, age, os) as t1
        FULL JOIN
          (SELECT reciever_id, 
                 toDate(time) as event_date,
                 count(reciever_id) as messages_received,
                 count(distinct user_id) as users_received
          from simulator_20230320.message_actions 
          where toDate(time) = yesterday()
          group by reciever_id, event_date) as t2
        ON t1.user_id = t2.reciever_id
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def df_merge(df1, df2):
        df = df1.merge(df2, how='outer', on=['event_date', 'user_id', 'gender', 'age', 'os']).fillna(0)
        return df

    @task()
    def create_metrics(df):
        dimension_gender = df.groupby(['event_date', 'gender']).sum().reset_index()
        dimension_gender['dimension'] = 'gender'
        dimension_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)

        dimension_os = df.groupby(['event_date', 'os']).sum().reset_index()
        dimension_os['dimension'] = 'os'
        dimension_os.rename(columns={'os': 'dimension_value'}, inplace=True)

        dimension_age = df.groupby(['event_date', 'age']).sum().reset_index()
        dimension_age['dimension'] = 'age'
        dimension_age.rename(columns={'age': 'dimension_value'}, inplace=True)

        df_all = pd.concat([dimension_gender, dimension_os, dimension_age])
        df_all = df_all[
            ['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent',
             'users_received', 'users_sent']]
        df_all[['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']] = df_all[
            ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].astype('int64')
        return df_all

    @task()
    def create_table():
        query_create_table = '''
        CREATE TABLE IF NOT EXISTS test.etl_k_shirochenkov (
            event_date      date,
            dimension       varchar(40),
            dimension_value varchar(40),
            views           integer,
            likes           integer,
            messages_received integer,
            messages_sent   integer,
            users_received  integer,
            users_sent      integer
        )
        ENGINE = Log()
        '''
        report = ph.execute(query_create_table, connection=connection_test)
        print(report)

    @task()
    def write_data(df):
        table_name = 'etl_k_shirochenkov'
        ph.to_clickhouse(df, table_name, connection=connection_test, index=False)

    data_feed = get_data_feed()
    data_messages = get_data_messages()
    data_merged = df_merge(data_feed, data_messages)
    data_processed = create_metrics(data_merged)
    create_table()
    write_data(data_processed)


exercise = exercise()
