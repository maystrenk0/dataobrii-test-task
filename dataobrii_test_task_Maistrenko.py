from datetime import timedelta
import pandas as pd
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions
from random import randint

# Task A

endpoint = 'cb.dzpssc72kkqfyax.cloud.couchbase.com'
username = 'int3grvl@gmail.com'
password = 'Pwdb1234!'
bucket_name = 'travel-sample'
table_name = 'airline'

auth = PasswordAuthenticator(username, password)
timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))
cluster = Cluster('couchbases://'+endpoint,
                  ClusterOptions(auth, timeout_options=timeout_opts))
cluster.wait_until_ready(timedelta(seconds=5))

sql_query = f'SELECT * FROM `travel-sample`.inventory.{table_name}'
result = cluster.query(sql_query)

df_raw = pd.DataFrame(result)
df = pd.DataFrame(list(df_raw[table_name]))
df.to_csv(f'{table_name}.csv', index = False)

# Task B

test_data = [randint(0,10) for i in range(df.shape[0])]
df['test_data'] = test_data

# Task C

df_csv = pd.read_csv(f'{table_name}.csv')  
df_upd = df_csv.merge(df, how='outer', on=list(df_csv.columns))
df_upd.to_csv(f'{table_name}.csv', index = False)
