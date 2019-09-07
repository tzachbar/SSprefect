import numpy as np
import pandas

from sqlalchemy import create_engine

from prefect import task, Flow
from prefect.engine.executors import LocalExecutor


iris_url = "https://gist.githubusercontent.com/orcaman/6244b23a9a8e5e7be8849bada02fdca2/raw/e7db7071633d63f1661a4798b4b91b2f2dd0c413/iris.csv"


@task
def extract():

    df = pandas.read_csv(iris_url)
    return df

@task
def transform(df):

    insert_loc = 0
    for col_name in df.columns[0:4]:
        df.insert(insert_loc + 1, col_name + '_mean', df.groupby('species')[col_name].transform(np.mean))
        df.insert(insert_loc + 2, col_name + '_stdev', df.groupby('species')[col_name].transform(np.std))
        insert_loc += 3

    return df

@task
def load(df):

    conn = create_engine('postgresql://awsuser:Aa123456@redshift-cluster-1.cbcap9uylzfk.us-east-2.redshift.amazonaws.com:5439/dev')
    df.to_sql('iris_data', conn, index=False)



with Flow("ETL") as flow:
    e = extract()
    t = transform(e)
    l = load(t)

state = flow.run(executor=LocalExecutor())
