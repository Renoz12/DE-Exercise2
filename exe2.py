import pandas as pd
import pandas_gbq as pg
from google.oauth2 import service_account

customer_path = "data/Customer"
netsale_path = "data/Netsale"
product_path = "data/Product"
project_id = 'fair-gist-367408'
credentials = service_account.Credentials.from_service_account_file(
    'key.json',
)

def transfromCSVtoRDS(path,name):
    data = pd.read_csv(path)
    result = pg.to_gbq(data,name, project_id=project_id,if_exists = "replace",credentials=credentials)
    print("creating..."+name)


#Part 2: DE Exercise 1.Load and transform data from CSV file to RDS
transfromCSVtoRDS(customer_path,"netsale_db.customer")
transfromCSVtoRDS(netsale_path,"netsale_db.netsale")

sql = """SELECT * from `netsale_db.customer` limit 5"""
data1 = pd.read_gbq(sql, project_id=project_id, dialect='standard',credentials=credentials)
print("table customer:")
print(data1)

sql = """SELECT * from `netsale_db.netsale` limit 5"""
data2 = pd.read_gbq(sql, project_id=project_id, dialect='standard',credentials=credentials)
print("table netsale:")
print(data2)

#Part 2: DE Exercise 2,3,4(1)
tablename = "netsale_db.total_netsale"
sql = """SELECT sale.customer_id as customer_id,
min(cus.first_name) as first_name,
min(cus.last_name) as last_name,
sum(sale.total) as total_sale_thb,
sum(sale.shipping) as shipping_thb,
sum(sale.tax) as tax_thb,
CURRENT_DATETIME('+07:00') as created_date,
CURRENT_DATETIME('+07:00') as updated_date,
FROM `netsale_db.customer` as cus 
inner join `netsale_db.netsale` as sale on cus.customer_id = sale.customer_id
group by sale.customer_id"""
data3 = pd.read_gbq(sql, project_id=project_id, dialect='standard',credentials=credentials)
result = pg.to_gbq(data3,tablename, project_id=project_id,if_exists = "replace",credentials=credentials,
table_schema=[{'name': 'customer_id','type': 'INTEGER'},
                {'name': 'first_name','type': 'STRING'},
                {'name': 'last_name','type': 'STRING'},
                {'name': 'total_sale_thb','type': 'FLOAT'},
                {'name': 'shipping_thb','type': 'FLOAT'},
                {'name': 'tax_thb','type': 'FLOAT'},
                {'name': 'created_date','type': 'DATETIME'},
                {'name': 'updated_date','type': 'DATETIME'},
                ],)

sql = """SELECT * from `netsale_db.total_netsale` limit 5"""
data4 = pd.read_gbq(sql, project_id=project_id, dialect='standard',credentials=credentials)
print("table netsale:")
print(data4)


#Part 2: DE Exercise 5
product = pd.read_csv(product_path)
print("Product before tranfrom :")
print(product)
#reqex = '\^[a-z]+\^'
product_edit = pd.DataFrame(pd.Series(product.name).str.replace('^un^','',regex=False))


print("Tranfrom name :")
print(product_edit)
product.name = product_edit.name

print("Result :")
print(product)
tablename = "netsale_db.product"
result = pg.to_gbq(product,tablename, project_id=project_id,if_exists = "replace",credentials=credentials)
sql = """SELECT * from `netsale_db.product`"""
data5 = pd.read_gbq(sql, project_id=project_id, dialect='standard')

print("table netsale:")
print(data5)