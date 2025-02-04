# Описание выполненого задания

Сервер Clickhouse - Docker
Клиент Clickhouse - Python (Jupyter) и DBeaver
Визуализация - Power BI

# Подготовка тестовой среды

Если потребуется запустить контейнер, то сам образ нужно скачать с [Яндекс.Диск](https://disk.yandex.ru/d/kLxYgmjGn-vxwA) и добавить архив в папку docker и затем запустить файл run_container.bat из папки docker.
В контейнере будет готовая БД с данными в таблице events и Materialized View events_by_day_mat_view с таблиец events_by_day.

В ахиве data можно найти данные из таблицы events в формате csv.

Данный скрипт был использован для создания таблицы и заполнения ее данными:

```
#generate_clickhouse_data.ipynb
import numpy as np
import pandas as pd
import clickhouse_connect
import uuid
from faker import Faker
from datetime import datetime

client = clickhouse_connect.get_client(host='localhost', port='8123', user='default')

create_table = """
CREATE TABLE events (
    event_id UUID,
    user_id UUID,
    event_type String,
    event_timestamp DateTime,
    product_id UUID,
    revenue Float
)
ENGINE = MergeTree
ORDER BY (event_timestamp)
;
"""
drop_table_if_exist = 'DROP TABLE IF EXISTS events'

client.command(drop_table_if_exist)
client.command(create_table)

users = [str(uuid.uuid4()) for _ in range(1000)]
products = [str(uuid.uuid4()) for _ in range(10)]
events = ['click', 'view', 'purchase']
count_rows = 100000
fake = Faker()

for i in range(1, 11):
    data = {
        'event_id' : [str(uuid.uuid4()) for _ in range(count_rows)],
        'user_id' : [np.random.choice(users) for _ in range(count_rows)],
        'event_type' : [np.random.choice(events) for _ in range(count_rows)],
        'event_timestamp' : [fake.date_between(start_date='-1y') for _ in range(count_rows)], 
        'product_id' : [np.random.choice(products) for _ in range(count_rows)]
    }
    
    revenues = []
    for value in data['event_type']:
        revenue = 0.0 if value != 'purchase' else np.round(np.random.uniform(low=0.1, high=400.0), decimals=2)
        revenues.append(revenue)    
    
    data['revenue'] = revenues
    
    df = pd.DataFrame(data)
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    client.insert_df('events', df)
    now =datetime.now()

    print(now.strftime("%Y-%m-%d %H:%M:%S") + ' вставка ' + str(count_rows) + ' строк')
```


Скрипт generate_clickhouse_data.ipynb запускался несколько раз с разными интервалами в массивах users, products и в ключах словаря revenue и event_timestamp, поэтому количество пользователей и продуктов, а также интервал выборки в БД будет иным. 

# Анализ пользовательского поведения и анализ дохода

**Все запросы из задания выгружают данные за текущий месяц с 01 по текущее число.**

*Запрос, который извлекает общее количество уникальных пользователей и общее количество событий* 
```
SELECT uniq(user_id) AS unique_users
	,count(event_id) AS events_count
FROM events 
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
```

*Запрос, который показывает распределение типов событий* 
```
SELECT event_type AS event
	,count(event_id) AS events_count
FROM events 
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
GROUP BY event_type
```

*Запрос, который подсчитывает общий доход от покупок и выделяет средний доход на пользователя* 
```
SELECT sum(t1.revenue) AS revenue
	,avg(t1.revenue) AS average_revenue_per_user
FROM (
	SELECT user_id 
		,sum(revenue) AS revenue
	FROM events  
	WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
	AND event_type = 'purchase'
	GROUP BY user_id
) t1
```

*Запрос для определения 5 лучших продуктов по доходу* 
```
SELECT product_id 
	,sum(revenue) AS revenue
FROM events  
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
AND event_type = 'purchase'
GROUP BY product_id 
ORDER BY revenue DESC
LIMIT 5
```

# Создание нового представления

*Создать Materialized View, который будет хранить агрегированные данные по дням: общее количество событий и общий доход от покупок*
Представление создается при помощи данных запросов:
```
CREATE TABLE IF NOT EXISTS events_by_day (
    day Date,
    events_count UInt16,
    revenue Float
) engine=SummingMergeTree()
ORDER BY (day)

CREATE MATERIALIZED VIEW events_by_day_mat_view TO events_by_day AS
SELECT 
    toDate(event_timestamp) as day, 
    count(event_id) as events_count, 
    sum(revenue) as revenue 
FROM events 
GROUP BY day
```

# Оптимизация запросов

Все мои запросы используют фильтрацию по дате с использованием оператора BETWEEN, а сама дата отсортирована на уровне таблицы, что позволяет быстро выделить нужные данные и работать только с ними. Также для каждого запроса данные можно брать из таблицы, в которой сохранены агрегированные данные от представления, а не из исходных данные. 

Для запроса подсчета общего доход от покупок и выделения среднего дохода на пользователя можно создать представления, которое бы суммировала доход по каждому пользователю в разрезе месяца и уже из него рассчитывалось среднее значение. Если за месяц будет большое количество пользователей, то можно создать ещё одно представление на основе существующего, которое бы находило среднее значение за месяц.

Из моих запросов можно выделить 2 последних, где есть такое условие:
```
event_type = 'purchase'
```

Так как event_type не является ключом и по архитектуре моей таблицы events все покупки имеют revenue <> 0, то можно заменить условие event_type = 'purchase' на сравнение revenue с 0.
Это ускорит работу запросов из-за отсутствия необходимости находить точную строку в значениях.


# Визуализация запросов

Все запросы из анализа были визуализированы в Power BI. 
![Визуализация](report/report.png)
