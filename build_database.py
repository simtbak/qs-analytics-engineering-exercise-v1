# Use this script to write the python needed to complete this task
import timeit
from sqlalchemy import create_engine
from urllib.request import urlopen
from urllib.parse import quote_plus
import pandas as pd
import gzip
import json

api = "https://www.thecocktaildb.com/api/json/v1/1/"
engine = create_engine('sqlite:///cocktails.db')
conn = engine.connect()

start_time = timeit.default_timer()
last_time = timeit.default_timer()

def runtime():
    global last_time
    step_duration = timeit.default_timer() - last_time
    total_duration = timeit.default_timer() - start_time
    if total_duration < 60:
        out = str(step_duration)[:5] + "s / " + str(total_duration)[:6]+"s"
    else:
        total_minutes = total_duration/60
        out = str(step_duration)[:5] + "s / " + str(total_minutes)[:6]+"m"
        
    last_time = timeit.default_timer()
    return "(" + out + ")"

def get_cocktail_details(search):
    #Search the API for a drink and return the name and associated glass
    api_target = urlopen(api + 'search.php?s=' + quote_plus(search))
    raw = json.loads(api_target.read())["drinks"][0]
    return pd.DataFrame([raw])[['strDrink','strGlass']]

def import_glass_stock():
    print(runtime(), "> [create] Import Glass Stock")
    #Read bar_data to DB
    df = pd.read_csv(open("data\\bar_data.csv"), sep=',')
    df["stock"] = df["stock"].replace(r'\D',r'',regex=True)
    df.to_sql('glass_stock', engine, index=False, if_exists='replace')

def import_seeds():
    print(runtime(), "> [create] Import Seed Data")
    conn.execute("drop table if exists combined_seeds;")
    #unzip and write each of the provided filenames to the DB
    path = "data\\"
    filenames = ['budapest.csv.gz','london_transactions.csv.gz','ny.csv.gz']
    for filename in filenames:
        with gzip.open(path+filename, 'rt', encoding='utf-8') as data:
            #Read london as tab separated
            sep = '\t' if filename == 'london_transactions.csv.gz' else ','
            df = pd.read_csv(data, sep=sep) 
            
            #Fix headers regardless of language
            df.columns = ['id','ts','drink','amount']
            
            #Fix time stamp column for newyork
            if filename == 'ny.csv.gz': 
                df['ts'] = df['ts'].replace(r'(.*?)-(\d{4})(.*)',r'\2-\1\3:00',regex=True)
            
            tablename = "seed_" + filename[:-7]
            df.to_sql(tablename, engine, index=False, if_exists='replace')
    
    #create the combined_seeds table
    conn.execute(open('combined_seeds.sql', 'rt').read())

def import_drink_glass_types():
    print(runtime(), "> [create] Import Drink Glass Types")
    conn.execute("drop table if exists drink_glass_types;")
    #Create unique drinks table from API data
        # select unique list of all drinks in DB
        # pass each value to api
        # return matching json object and write to DB
    unique_drinks = pd.read_sql("select drink from combined_seeds group by drink", engine).values
    drink_glass_types = pd.concat([get_cocktail_details(drink[0]) for drink in unique_drinks])
    drink_glass_types['strGlass'] = drink_glass_types['strGlass'].str.lower()
    drink_glass_types.to_sql('drink_glass_types', engine, index=False, if_exists='replace')


refresh = True
if refresh:
    import_glass_stock()
    import_seeds()
    import_drink_glass_types()
    print(runtime(), "> dropping all_transaction_glasses")
    conn.execute("drop table if exists all_transaction_glasses;")
    print(runtime(), "> dropping glass_usage")
    conn.execute("drop table if exists glass_usage;")

print(runtime(), "> [create] All Transaction Glasses")
conn.execute(open('all_transaction_glasses.sql', 'rt').read())

print(runtime(), ">> [index] All Transaction Glasses")
conn.execute("create index if not exists comparison on all_transaction_glasses (bar, glass, ts_start, ts_end);")

print(runtime(), "> [create] Glass Usage (may take up to 3mins)")
conn.execute(open('glass_usage.sql', 'rt').read())

print(runtime(), ">> Selecting glass_stock_check")
poc = pd.read_sql(open('glass_stock_check.sql', 'rt').read(), engine)
print(poc.to_string())
poc.to_sql('POC_glass_stock_check', engine)
