from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import time
#set credentials
credentials = service_account.Credentials.from_service_account_file('key.json')
project_id = 'fair-gist-367408'
client = bigquery.Client(credentials= credentials,project=project_id)

query_job = client.query("""SELECT sale.customer_id as customer_id,
min(cus.first_name) as first_name,
min(cus.last_name) as last_name,
sum(sale.total) as total_sale_thb,
sum(sale.shipping) as shipping_thb,
sum(sale.tax) as tax_thb,
CURRENT_DATETIME('+07:00') as created_date,
CURRENT_DATETIME('+07:00') as updated_date,
FROM `ws5_de2022.customer` as cus 
inner join `ws5_de2022.netsale` as sale on cus.customer_id = sale.customer_id
group by sale.customer_id order by first_name""")

#tranfrom data 
results = pd.DataFrame(query_job.to_dataframe())
results.to_csv('out.csv') # cannot insert dataframe to client.insert_rows_from_dataframe
time.sleep(5)
file = pd.read_csv('out.csv')
file = file.drop(columns='Unnamed: 0')

#create table
table_id = "fair-gist-367408.ws5_de2022.total_netsale"
schema = [
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("total_sale_thb", "FLOAT"),
    bigquery.SchemaField("shipping_thb", "FLOAT"),
    bigquery.SchemaField("tax_thb", "FLOAT"),
    bigquery.SchemaField("created_date", "DATETIME"),
    bigquery.SchemaField("updated_date", "DATETIME"),
]
table = bigquery.Table(table_id, schema=schema)
print("Creating...")

table = client.create_table(table,exists_ok=True)  # Make an API request.
#data = client.insert_rows_from_dataframe(table,results) #Error : https://github.com/googleapis/python-bigquery/issues/1348
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
time.sleep(7)

print("inserting...")
error = client.insert_rows_from_dataframe(table,file)
print("Insert Data to table {} success".format(table.table_id))

Finish_job2 = client.query("""Select * from `ws5_de2022.total_netsale`""")
print("Result Ex2")
print(Finish_job2.to_dataframe())