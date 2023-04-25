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
    'start_date': datetime(2023, 4, 12)
}

schedule_interval = '58 10 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_feed():
    @task()
    def get_data():
        query = '''
        SELECT toString(toDate(time)) as date,
                count(distinct user_id) as dau,
                countIf(action, action='view') as views,
                countIf(action, action='like') as likes,
                likes / views as ctr
        FROM  simulator_20230320.feed_actions
        where toDate(time) < today() and toDate(time) > yesterday() - 7
        group by date
        '''
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'database': 'simulator_20230320',
            'user': 'student',
            'password': 'dpo_python_2020'
        }
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def message_with_metrics(df):
        token = os.environ['BOT_TOKEN']
        bot = telegram.Bot(token=token)
        group_chat_id = -802518328
        bot.sendMessage(chat_id=group_chat_id,
                        text=f'Метрики за {df.iloc[6, 0]}:\nDAU: {df.iloc[6, 1]}\nПросмотры: {df.iloc[6, 2]}\nЛайки: {df.iloc[6, 3]}\nCTR: {df.iloc[6, 4]:.2f}')

        plt.rcParams['figure.figsize'] = [20, 11]
        sns.set()

        plot_object = io.BytesIO()

        fig, axes = plt.subplots(2, 2)
        fig.suptitle(f'Графики с показателями за неделю {df.iloc[0, 0]}-{df.iloc[6, 0]}')
        sns.lineplot(data=df, x='date', y='dau', ax=axes[0, 0], marker='o').set(xlabel=None)
        axes[0, 0].set_title('Активные пользователи в день')
        sns.lineplot(data=df, x='date', y='views', ax=axes[0, 1], marker='o').set(xlabel=None)
        axes[0, 1].set_title('Просмотры')
        sns.lineplot(data=df, x='date', y='likes', ax=axes[1, 0], marker='o').set(xlabel=None)
        axes[1, 0].set_title('Лайки')
        sns.lineplot(data=df, x='date', y='ctr', ax=axes[1, 1], marker='o').set(xlabel=None)
        axes[1, 1].set_title('CTR')
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    data = get_data()
    message_with_metrics(data)


report_feed = report_feed()
