import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'k-shirochenkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 13)
}
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20230320',
    'user': 'student',
    'password': 'dpo_python_2020'
}
token = os.environ['BOT_TOKEN']
bot = telegram.Bot(token=token)
group_chat_id = -802518328

schedule_interval = '59 10 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_app():
    @task()
    def get_data_messenger():
        query = '''
        SELECT toString(toDate(time)) as date,
                count(user_id) as message_sent,
                count(distinct user_id) as message_senders,
                count(DISTINCT reciever_id ) as message_receivers
        FROM  simulator_20230320.message_actions 
        where toDate(time) < today() and toDate(time) > yesterday() - 7
        group by date
        order by date
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def get_data_retention():
        query = '''
        SELECT start_day start_day,
               day day,
                             count(user_id) AS users
        FROM
             (SELECT user_id,
                     min(toDate(time)) AS start_day
              FROM simulator_20230320.feed_actions
              GROUP BY user_id
              having start_day >= today() - 15 and start_day < today()
              ) t1
           JOIN
             (SELECT DISTINCT user_id,
                              toDate(time) AS day
              FROM simulator_20230320.feed_actions) t2 USING user_id
        GROUP BY day, start_day
        ORDER BY start_day, day
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def get_data_dau():
        query = '''
        SELECT toString(day) as date, round(users1 / users2, 2) as users_ctr
        FROM 
          (SELECT toDate(time) as day,
                 count(distinct user_id) AS users1
          FROM simulator_20230320.feed_actions 
          WHERE toDate(time) < today() 
            and toDate(time) >= today() - 7
          GROUP BY day) as t1
        JOIN
          (SELECT toDate(time) as day,
                  count(distinct user_id) AS users2
          FROM simulator_20230320.message_actions 
          WHERE toDate(time) < today() 
            and toDate(time) >= today() - 7
          GROUP BY day) as t2 
          ON t1.day=t2.day
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def process_messenger_data(df):
        df = df.rename(columns={'message_sent': 'Отправлено сообщений', 'message_senders': 'Отправители',
                                'message_receivers': 'Получатели'})
        melted = df.melt(id_vars=['date'], var_name="type", value_name="value")
        plt.rcParams['figure.figsize'] = [20, 11]
        plt.rcParams["figure.autolayout"] = True
        sns.set()
        plot_object = io.BytesIO()
        ax = sns.barplot(data=melted, x='date', y='value', hue='type')
        ax.set(title='Динамика мессендежра за последние 7 дней')
        for p in ax.patches:
            height = p.get_height()  # get the height of each bar
            ax.text(x=p.get_x() + (p.get_width() / 2), y=height + 170, s=height, ha='center')  # adding text to each bar
        ax.set(xlabel=None)
        plt.legend(title='Категория', bbox_to_anchor=(1.005, 0.97), loc='upper left', borderaxespad=0)
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    @task()
    def process_retention_data(df):
        df['day_n'] = (df['day'] - df['start_day']).dt.days
        df.start_day = df.start_day.astype(str)
        pivot = df.pivot_table(index='start_day', columns='day_n', values='users')
        pivot = pivot.divide(pivot[0], axis=0)
        plt.rcParams['figure.figsize'] = [20, 11]
        plt.rcParams["figure.autolayout"] = True
        sns.heatmap(pivot, linewidth=.01, annot=True, fmt='.0%', vmin=0, vmax=0.40)
        plt.title('Ретеншн за последние 14 дней')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    @task()
    def process_users_ctr(df):
        plt.rcParams['figure.figsize'] = [20, 12]
        plt.rcParams["figure.autolayout"] = True
        sns.set()
        plot_object = io.BytesIO()
        ax = sns.lineplot(data=df, x='date', y='users_ctr', marker='o')
        ax.set(title='Отношение пользователей ленты к пользователям мессенджера')
        for x, y in zip(df['date'], df['users_ctr']):
            ax.text(x=x, y=0.985 * y, s=y, color='blue')
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    data_messenger = get_data_messenger()
    data_retention = get_data_retention()
    data_dau = get_data_dau()
    process_messenger_data(data_messenger)
    process_retention_data(data_retention)
    process_users_ctr(data_dau)


report_app = report_app()
