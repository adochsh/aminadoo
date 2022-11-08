import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder,GridUpdateMode,DataReturnMode
from st_aggrid.shared import JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import cx_Oracle
import os
import platform
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
import recommend_sql

##### Для запуска на локальном компьютере (с установленным клиентом оракла)
# if platform.system() == "Darwin":
#     cx_Oracle.init_oracle_client(lib_dir=os.environ.get("HOME")+"/Downloads/instantclient_19_8")

# GET OS ENV KEYS
host = os.environ['DB_HOST'] OR nxlrd1-scan1.int.adeo.com
userdb = os.environ['DB_USER']
passwordb = os.environ['DB_PASS']
service_namedb = os.environ['DB_SERV']

st.set_page_config(page_title="REPL", page_icon=None, layout="wide")
sql = recommend_sql.script

# Функция для запуска SQL запроса в БД
@st.experimental_memo(ttl=6000,suppress_st_warning=True)
def select(sql):
    try:
        dsn = cx_Oracle.makedsn(host, 1521, service_name=service_namedb)
        con = cx_Oracle.connect(user=userdb, password=passwordb, dsn=dsn,encoding="UTF-8")
        cursor = con.cursor()
    except cx_Oracle.DatabaseError as e:
        st.write("There is a problem with Connection to Oracle ", e)

    df = pd.read_sql(sql, con)

    t = datetime.now().strftime("%H:%M:%S")[0:5]
    st.write('Время обновления: {}'.format(t))
    return df

###Сложная визуализация для таблицы
def aggrid_interactive_table(df: pd.DataFrame, reload_data):
    """Creates an st-aggrid interactive table based on a dataframe.
    Args:
    df (pd.DataFrame]): Source dataframe
    Returns:
    dict: The selected row
    """
    cellsytle_jscode = JsCode(
    """function(params) {
            if (params.value > 0) {
                return {
                    'color': 'white',
                    'backgroundColor': 'red'
                }
            } else if (params.value < 0) {
                return {
                    'color': 'white',
                    'backgroundColor': 'crimson'
                }
            } else {
                return {
                    'color': 'white',
                    'backgroundColor': 'light' } } }; """
    )
    cellsytle_jscode2 = JsCode(
    """function(params) {
            if (params.value === 0) {
                return {
                    'color': 'white',
                    'backgroundColor': 'green'
                }
            } else if (params.value < 14) {
                return {
                    'color': 'red',
                    'backgroundColor': 'light'
                }
            } else {
                return {
                    'color': 'black',
                    'backgroundColor': 'light' } } }; """
    )

    options = GridOptionsBuilder.from_dataframe( df, enableRowGroup=True, enableValue=True, enablePivot=True )
    options.configure_column('Округл. Предл. для пуша, шт', cellStyle=cellsytle_jscode )
    options.configure_column('Сток на 922, дни', cellStyle=cellsytle_jscode2 )
    # options.configure_column('Резервы', editable = True)
    # options.configure_column('Сток на складе 921', pinned = True)

    options.configure_side_bar()
    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        #theme ="light", #fresh , 'streamlit', 'light', 'dark', 'blue', 'fresh', 'material'
        # update_mode =GridUpdateMode.MANUAL,#GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
        reload_data=reload_data,
        )
    # if reload_data:
    #     st.write('aggrid reloaded!')
    return selection

# функция для выгрузки в эксель
@st.cache(suppress_st_warning=True)
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'})
    worksheet.set_column('A:A', None, format1)
    writer.save()
    processed_data = output.getvalue()
    return processed_data

i =[12346148, 82025672, 15369311, 82135561, 82135581, 12657210, 15607438]
l =[3,5,32,35,51,56,86,6,20,26, 117]

reload_data = False
items = ",".join(str(i) for i in i)
locs = ",".join(str(i) for i in l)

with st.sidebar.form("Update"):
    submit_button = st.form_submit_button(label='Обновить данные')

# Запуск запроса по фомированию рекомендации пуша
to_df = sql.format(items,locs,items,locs,locs)
df = select(to_df)

# Обнуление рекомендаций, где сток на складе уже разгружен
df.loc[(df.STOCKDAYS_WH922==0), 'PUSH_QTY_ROUND'] = 0
df['ALERT'] = df['ALERT'].astype(str)
# Переименование столбцов
df = df.fillna(0).rename(columns={'PUSH_QTY_RAW': "Предл. для пуша, шт"
                               , 'PUSH_QTY_ROUND':'Округл. Предл. для пуша, шт'
                               , 'STORE':'Магазин', 'ITEM':'Артикул', 'SUPPLIER':'Код Поставщика'
                               , 'STOCK_WH922':'Сток на 922, шт', 'STOCKDAYS_WH922':'Сток на 922, дни'
                               , 'STOCK_DAYS':'Сток на момент доставки, дни'
                               , 'STOCK_DAYS_CURR':'Сток текущий, дни'
                               , 'TRANSIT_STOCK_DAYS':'Cток в пути, дни'
                               , 'FOULIBL':'Поставщик' , 'CNT_ORDERS':'Заказы текущ.', 'QTY_IN_TRANSIT':'Товар в пути'
                               , 'D1_DAYS_FROM_NOW':'Срок до ближ. даты доставки'
                               , 'D2D1_DIFF_DAYS':'Срок между доставками'
                               , 'AVG_DAILY_FRCST':'СДПП', 'AVG_FOR_ALL_STORES':'СДПП по сети'
                               , 'STOCK_STORE':'Сток в текущий, шт'
                               , 'QTY_TRANSFERS':'Трансфера в магазин', 'RESERVS':'Резервы'
                               , 'FRCST_FROM_NOW_TO_D1':'Сумма прогноза продаж (до ближ. даты доставки)'
                               , 'ROUND_LVL':'Уровень PCB', 'SUPP_PACK_SIZE':'Коробка, шт'
                               , 'PAL_VAL':'Паллета, шт'
                               })
df = df[['Предл. для пуша, шт', 'Округл. Предл. для пуша, шт', 'ALERT','Сток на 922, дни','Сток на 922, шт', 'FACT_STOCK_WH922', 'RESERV_WH', 'NON_SELLABLE_QTY'
        , 'Артикул','Магазин','Код Поставщика', 'Сток на момент доставки, дни', 'Сток текущий, дни'
        , 'Срок до ближ. даты доставки', 'Срок между доставками',  'Cток в пути, дни', 'SAFETY_DAYS'
        , 'СДПП', 'СДПП по сети', 'Сумма прогноза продаж (до ближ. даты доставки)'
        , 'Сток в текущий, шт', 'Трансфера в магазин', 'Товар в пути', 'Резервы', 'RESERV_QTY', 'RESERV_DAYS'
        , 'Поставщик', 'Заказы текущ.', 'MIN_ORDER_DATE', 'MIN_DELIVERY_DATE', 'MAX_DELIVERY_DATE', 'PLAN_ORD_DATE', 'PLAN_DELIV_DATE'
        , 'Уровень PCB', 'Коробка, шт', 'Паллета, шт' ]].sort_values(by=['Округл. Предл. для пуша, шт'], ascending=False)

reload_data=True
selection = aggrid_interactive_table(df, reload_data)
#### приведение к формату для загрузчика трансфера в рмс
tf= df[df['Округл. Предл. для пуша, шт']>0][['Артикул','Магазин','Округл. Предл. для пуша, шт']].copy().rename(columns={'Артикул':'Код ЛМ', 'Магазин':'Получатель' })
tf['Отправитель'] = 922
tf['Дата доставки'] =(pd.to_datetime("today").normalize() + timedelta(days=2)).strftime("%d-%m-%Y")
tf['Тип логист. потока'] = ''
tf['Утвердить трансфер'] = ''

#### transform to pivot
tf = tf[['Отправитель','Получатель','Дата доставки','Тип логист. потока','Утвердить трансфер','Код ЛМ','Округл. Предл. для пуша, шт']]
tf = tf.pivot(index ='Код ЛМ' , columns =[ 'Отправитель', 'Получатель', 'Дата доставки', 'Тип логист. потока','Утвердить трансфер']
                              , values ='Округл. Предл. для пуша, шт').fillna(0)
#### transform to EXCEL to download
df_xlsx = to_excel(tf)
st.download_button(label='📥 Скачать в виде шаблона для трансферов',
                                data=df_xlsx ,
                                file_name= 'df_test.xlsx')
