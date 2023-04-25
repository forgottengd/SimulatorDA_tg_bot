import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
import os
from datetime import datetime, timedelta
import scipy.stats as stats

from airflow.decorators import dag, task

default_args = {
    'owner': 'k-shirochenkov',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 4, 18)
}
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20230320',
    'user': 'student',
    'password': 'dpo_python_2020'
}
token = os.environ['BOT_TOKEN']
bot = telegram.Bot(token=token)
chat_id = 96083455
group_chat_id = -958942131

# every 15 minutes
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_demon():
    @task()
    def get_data_feed():
        query = '''
        SELECT toStartOfFifteenMinutes(time) AS timestamp,
              count(distinct user_id) as dau,
              countIf(user_id, action='like') as likes,
              countIf(user_id, action='view') as views,
              round(countIf(user_id, action='like') / countIf(user_id, action='view'), 3) AS ctr
        FROM simulator_20230320.feed_actions
        WHERE time >= toStartOfFifteenMinutes(now()) - INTERVAL '1 DAY'
          AND time < toStartOfFifteenMinutes(now())
        GROUP BY toStartOfFifteenMinutes(time)
        ORDER BY timestamp
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def proceed_metrics_feed(df):
        plt.rcParams['figure.figsize'] = [20, 12]
        sns.set()
        if (df.iloc[-1, 4]) > df.ctr.mean() + 3 * df.ctr.std() or (df.iloc[-1, 4]) < df.ctr.mean() - 3 * df.ctr.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика CTR в ленте новостей. 
            Текущее значение {df.iloc[-1, 4]}. Отклонение более {round((df.iloc[-1, 4] - df.ctr.mean()) / df.ctr.mean() * 100, 2)}%.
            Дашборд: https://superset.lab.karpov.courses/superset/dashboard/3184/''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='ctr', marker='o')
            ax.set(title='CTR за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        if (df.iloc[-1, 1]) > df.dau.mean() + 3 * df.dau.std() or (df.iloc[-1, 1]) < df.dau.mean() - 3 * df.dau.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика DAU в ленте новостей. 
                Текущее значение {df.iloc[-1, 1]}. Отклонение более {round((df.iloc[-1, 1] - df.dau.mean()) / df.dau.mean() * 100, 2)}%.
                Дашборд: https://superset.lab.karpov.courses/superset/dashboard/3184/''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='dau', marker='o')
            ax.set(title='DAU за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        if (df.iloc[-1, 2]) > df.likes.mean() + 3 * df.likes.std() or (df.iloc[-1, 2]) < df.likes.mean() - 3 * df.likes.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика LIKES в ленте новостей. 
                Текущее значение {df.iloc[-1, 2]}. Отклонение более {round((df.iloc[-1, 2] - df.likes.mean()) / df.likes.mean() * 100, 2)}%.
                Дашборд: https://superset.lab.karpov.courses/superset/dashboard/3184/''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='likes', marker='o')
            ax.set(title='Лайки за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        if (df.iloc[-1, 3]) > df.views.mean() + 3 * df.views.std() or (df.iloc[-1, 3]) < df.views.mean() - 3 * df.views.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика VIEWS в ленте новостей. 
                Текущее значение {df.iloc[-1, 3]}. Отклонение более {round((df.iloc[-1, 3] - df.views.mean()) / df.views.mean() * 100, 2)}%.
                Дашборд: https://superset.lab.karpov.courses/superset/dashboard/3184/''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='views', marker='o')
            ax.set(title='Просмотры за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    @task()
    def get_data_messenger():
        query = '''
        SELECT toStartOfFifteenMinutes(time) AS timestamp,
              count(distinct user_id) as dau,
              count(user_id) as messages_sent
        FROM simulator_20230320.message_actions 
        WHERE time >= toStartOfFifteenMinutes(now()) - INTERVAL '1 DAY'
          AND time < toStartOfFifteenMinutes(now())
        GROUP BY toStartOfFifteenMinutes(time)
        ORDER BY timestamp
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def proceed_metrics_messenger(df):
        plt.rcParams['figure.figsize'] = [20, 12]
        sns.set()
        if (df.iloc[-1, 1]) > df.dau.mean() + 3 * df.dau.std() or (df.iloc[-1, 1]) < df.dau.mean() - 3 * df.dau.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика DAU в ленте новостей. 
                Текущее значение {df.iloc[-1, 1]}. Отклонение более {round((df.iloc[-1, 1] - df.dau.mean()) / df.dau.mean() * 100, 2)}%.
                ''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='dau', marker='o')
            ax.set(title='DAU за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        if (df.iloc[-1, 2]) > df.messages_sent.mean() + 3 * df.messages_sent.std() or (df.iloc[-1, 2]) < df.messages_sent.mean() - 3 * df.messages_sent.std():
            bot.sendMessage(chat_id=group_chat_id, text=f'''Метрика Отправленные сообщения в ленте новостей. 
                Текущее значение {df.iloc[-1, 2]}. Отклонение более {round((df.iloc[-1, 2] - df.messages_sent.mean()) / df.messages_sent.mean() * 100, 2)}%.
                ''')
            plot_object = io.BytesIO()
            ax = sns.lineplot(data=df, x='timestamp', y='dau', marker='o')
            ax.set(title='DAU за последние 24 часа')
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

    data_feed = get_data_feed()
    proceed_metrics_feed(data_feed)
    data_messenger = get_data_messenger()
    proceed_metrics_messenger(data_messenger)


alert_demon = alert_demon()