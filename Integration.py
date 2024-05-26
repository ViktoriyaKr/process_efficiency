from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import PendingRollbackError

import pandas as pd
from dotenv import load_dotenv

from os import environ, path
from datetime import datetime

script_path = path.dirname(path.abspath(__file__))

load_dotenv()

GP_LOGIN = environ.get('GP_LOGIN')
GP_PASSWORD = environ.get('GP_PASSWORD')
GP_HOST = environ.get('GP_HOST')
GP_DATABASE = environ.get('GP_DATABASE')
GP_DRIVER = environ.get('GP_DRIVER')

CH_LOGIN = environ.get('CH_LOGIN')
CH_PASSWORD = environ.get('CH_PASSWORD')
CH_HOST = environ.get('CH_HOST')
CH_DATABASE = environ.get('CH_DATABASE')
CH_DRIVER = environ.get('CH_DRIVER')

ORP_LOGIN = environ.get('ORP_LOGIN')
ORP_PASSWORD = environ.get('ORP_PASSWORD')
ORP_HOST = environ.get('ORP_HOST')
ORP_DATABASE = environ.get('ORP_DATABASE')
ORP_DRIVER = environ.get('ORP_DRIVER')

def create_connection(driver:str, login:str, password:str, host:str, db:str) -> Engine:
    conn = create_engine(f'{driver}://{login}:{password}@{host}/{db}')
    return conn

GP_CONN = create_connection(driver=GP_DRIVER, login=GP_LOGIN, password=GP_PASSWORD, host=GP_HOST, db=GP_DATABASE)
CH_CONN = create_connection(driver=CH_DRIVER, login=CH_LOGIN, password=CH_PASSWORD, host=CH_HOST, db=CH_DATABASE)
ORP_CONN = create_connection(driver=ORP_DRIVER, login=ORP_LOGIN, password=ORP_PASSWORD, host=ORP_HOST, db=ORP_DATABASE)

#Чтение sql файла и запись полученных новых данных в DataFrame
def get_data(name_df: str, conn: Engine, name_date_df: str = None) -> pd.DataFrame:
    
    start_date = ""
    if name_date_df:
        last_date = f"""
                    select 
                        max("{name_date_df}")
                    from analitycs."{name_df}"
                    """
        last_date = pd.read_sql(last_date, con = ORP_CONN)
        last_date['max'] = pd.to_datetime(last_date['max'])
        start_date = last_date['max'].values[0]
        start_date = pd.Timestamp(start_date).date()

    stop_date = datetime.now().date()

    with open(path.join(script_path, f'sql_int\{name_df}.sql'), 'r', encoding='utf-8') as f:
        query = f.read()
        query = query.format(StartDate=start_date, StopDate=stop_date)
    
    try:
        df = pd.read_sql(query, conn)
    except PendingRollbackError:
        conn.connect().rollback()
        df = pd.read_sql(query, conn)
    return df

#Новые данные по выполненным поручениям на филиале 
Tasks_df = get_data(name_df='Int.Poruchenij', name_date_df='ДатаСоздания', conn=GP_CONN)

#Актуальная информация по формату филиала
Modeli_Filialov_df = get_data(name_df='Int.Modeli_Filialov', conn=GP_CONN)

#Актуальная информация по действующим сотрудникам и должностям
Roly_Dolgnosti_df = get_data(name_df='Int.Roly_Dolgnosti', conn=GP_CONN)

#Новые данные по продажам на филиалах 
Sales_Time_df = get_data(name_df='Int.Sales_Time', name_date_df='Month', conn=CH_CONN)

#Добавить столбец с временим чистой продажи
Sales_Time_df['Время чистой продажи'] = None
Sales_Time_df.loc[
    (Sales_Time_df['тип продажи'] == 'менеджер') &
    (Sales_Time_df['возврат'] == "") &
    (Sales_Time_df['допка'] == "") &
    (Sales_Time_df['доставка'] == "") &
    (Sales_Time_df['обмен'] == "") &
    (Sales_Time_df['симка'] == "") &
    (Sales_Time_df['страховка'] == "") &
    (Sales_Time_df['услуга'] == ""),
    'Время чистой продажи'
] = Sales_Time_df['от создания РН до проведения РН']

#Добавить столбец с временем набора интернет-заказа
Sales_Time_df['Время набора интернет-заказа'] = None
Sales_Time_df.loc[
    Sales_Time_df['место создания рн'] == 'Сайт ДНС',
    'Время набора интернет-заказа'
] = Sales_Time_df['от создания РН до проведения оплаты']

#Среднее время по филиалам и месяцам
Sales_Time_df = Sales_Time_df.groupby(['Month', 'BranchGuid'], as_index=False)[['Время чистой продажи', 'Время набора интернет-заказа']].mean()

#Актуальная информация по включенным поручениям на филиале
Active_tasks = get_data(name_df='Active_tasks', conn = ORP_CONN)

#Актуальная информация по рекомендуемым схемам процессов на филиале
Scheme_work = get_data(name_df='Scheme_work', conn = ORP_CONN)

Info_branch = pd.merge(Active_tasks, Scheme_work, how='left', left_on='branchguid', right_on='branch_guid')

#Создание DataFrame с рекомендованными поручениями на филиал в соответствии со схемой процессов
schemas = {
    'УСМ1': 'УСМ1',
    'УСМ2': 'УСМ2',
    'Конвеер1': 'Конвеер1',
    'Конвеер2': 'Конвеер2'
}

tasks = {
    'ysm': ['Выдача интернет-заказа', 'Выдача товара', 'Групповой разнос товара', 'Набор для клиента с другого филиала в ячейку', 
            'Набор интернет заказа в ячейку', 'Набор товара', 'Набор товара на транзит', 'Набор товара с приемки New', 'Снятие антикражки'],
    'konveer': ['Выдача интернет-заказа', 'Выдача товара', 'Групповой разнос из постамата', 'Групповой разнос товара', 'Набор для клиента с другого филиала в ячейку',
                'Набор интернет заказа в ячейку', 'Набор товара', 'Набор товара на транзит', 'Набор товара с приемки New', 'Оказание услуг', 'Снятие антикражки']
}

def create_df(schema_name, tasks_key):
    return pd.DataFrame({'Schema': [schema_name] * len(tasks[tasks_key]), 'Tasks': tasks[tasks_key]})

data_frames = [
    create_df(schemas['УСМ1'], 'ysm'),
    create_df(schemas['УСМ2'], 'ysm'),
    create_df(schemas['Конвеер1'], 'konveer'),
    create_df(schemas['Конвеер2'], 'konveer')
]

recommend_tasks = pd.concat(data_frames, ignore_index=True)

Status_vkl_recom_tasks = pd.merge(Info_branch, recommend_tasks, how='left', left_on=['recommended_scheme', 'TaskName'], right_on=['Schema', 'Tasks'])

#Выявление ошибок в настройке процессов на филиале путем сравнивания фактических настроек с рекомендуемыми
Status_vkl_recom_tasks['true'] = 1
Status_vkl_recom_tasks.loc[
    (Status_vkl_recom_tasks['Tasks'].isna() & Status_vkl_recom_tasks['IsUsed'] == 1 ) |
    (Status_vkl_recom_tasks['Tasks'].notna() & Status_vkl_recom_tasks['IsUsed'] == 0),
    'true'
] = 0

Status_vkl_recom_tasks = Status_vkl_recom_tasks.drop(['branch_name', 'branch_guid', 'inner_ts', 'Tasks'], axis=1)

def Postgre_insert_drop(df: pd.DataFrame, table: str, editing: str) -> None:
    """Залив в БД."""
    df['inner_ts'] = datetime.now()
    df.to_sql(table, con=ORP_CONN, schema="analitycs", if_exists=editing, index=False)

Postgre_insert_drop(Tasks_df, 'Int.Poruchenij', 'append')

Postgre_insert_drop(Modeli_Filialov_df, 'Int.Modeli_Filialov', 'replace')

Postgre_insert_drop(Roly_Dolgnosti_df, 'Int.Roly_Dolgnosti', 'replace')

Postgre_insert_drop(Sales_Time_df, 'Int.Sales_Time', 'append')

Postgre_insert_drop(Status_vkl_recom_tasks, 'Int.Status_Vkluch_Poruch', 'replace')