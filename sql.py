
from sqlalchemy import Table, text,create_engine,MetaData, insert,Column,String,Integer,Float,Boolean,select,DateTime,and_,or_
import datetime
import pandas as pd
import numpy as np
import os
import sqlalchemy

DIFF_JST_FROM_UTC = 9
COLS=50
# table定義

metadata = MetaData()
dummyTable = Table('dummy0', metadata,
            Column("date",DateTime()),
            Column("data1", Float()),
            )

l1=[Column(f"data{num}",Float()) for num in range(COLS)]

dummyTable3 = Table('dummy2', metadata,
            Column("date",DateTime()),
            *l1
            )
metadata1 = MetaData()
#storage = Table('storage', metadata1)

#db_host = os.environ["INSTANCE_HOST"]  # e.g. '127.0.0.1' ('172.17.0.1' if deployed to GAE Flex)
#db_user = os.environ["DB_USER"]  # e.g. 'my-db-user'
#db_pass = os.environ["DB_PASS"]  # e.g. 'my-db-password'
#db_name = os.environ["DB_NAME"]  # e.g. 'my-database'
#db_port = os.environ["DB_PORT"]  # e.g. 3306


dialect="mysql"
driver=""
username="user"
password="pass"
host="host"
port=80
database="db"
charset_type="utf-8"
#engine = create_engine(f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}?charset={charset_type}")
enginet = create_engine('sqlite:///db0.sqlite', echo=False)
engine1 = create_engine('sqlite:///db1.sqlite', echo=False)
def del_table(table,eng):
    table.drop(eng)
#del_table(storage,engine1)

metadata.create_all(enginet)
metadata1.create_all(engine1)

# 書き込み
def write_data():
    now=datetime.datetime.utcnow() + datetime.timedelta(hours=DIFF_JST_FROM_UTC)
    dic1={f"data{num}":val for num,val in enumerate(np.random.normal(0,2,COLS))}
    with enginet.connect() as con:
        stmt = insert(dummyTable3).values(date=now,**dic1)
        result_proxy = con.execute(stmt)
# queryで読み込み
def read_data(now,start):
    with enginet.connect() as con:
        stmt = select(dummyTable3).where(and_(dummyTable3.c.date>=start,dummyTable3.c.date<now))
        rs = con.execute(stmt)
        df=pd.DataFrame(rs.fetchall())
        if len(df)>0:
            df.columns=rs.keys()
            df=df.set_index(df.columns[0])
        return(df)
    return(0)

def width(x):
    return(max(x)-min(x))

def process_all():
    write_data()
    now=datetime.datetime.utcnow() + datetime.timedelta(hours=DIFF_JST_FROM_UTC)
    start=now-datetime.timedelta(minutes=5)
    start=pd.to_datetime("2022/11/23/21:25")
    df=read_data(now,start)

    #従来のデータに新規のデータをマージする
    df.to_sql("storage",con=engine1,if_exists="append")
    df2=pd.read_sql("storage",con=engine1)
    df2=df2.drop_duplicates(subset=["date"])
    df2=df2.dropna(subset=["date"])
    df2.to_sql("storage",con=engine1,if_exists="replace",index=False)

    #すべてのデータを取り出して、それの毎時の平均を計算し、それを新しいテーブルに出力
    df3=pd.read_sql("storage",con=engine1)
    days=df3["date"].dt.strftime('%Y-%m-%d %H:00')
    df4=df3.filter(like="data").groupby(days).agg([np.mean,width])
    df4.index=pd.to_datetime(df4.index)
    df4.columns=[f"{col[0]}_{col[1]}" for col in df4.columns]
    df4=df4.reset_index()    
    df4.to_sql("agged",engine1,if_exists="replace",index=False)
    df5=pd.read_sql("agged",con=engine1)

process_all()
