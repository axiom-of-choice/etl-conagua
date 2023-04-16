from extract import extract, extract_json
from transform import generate_table_1, generate_table_2
from load import write_local
from utils import remove_staging_files


remove_staging_files()
extract()
df1 = generate_table_1()
df2 = generate_table_2(df1)
write_local(df= df1,path='./data/processed/process_1/table_1')
write_local(df=df2, path='./data/processed/process_2/table_2')
write_local(df=df2, path='./data/processed/process_2/table_2', partition='current')
remove_staging_files()