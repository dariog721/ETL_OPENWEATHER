import requests
import pandas as pd 
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import time
import schedule
import pytz

db_uri ='postgresql://postgres:admin@localhost:5432/postgres'
local_timezone = pytz.timezone('America/Mexico_City')
api = '###'
engine = create_engine(db_uri)

def req(city,tyrq):
    url = f"https://api.openweathermap.org/data/2.5/{tyrq}?q={city}&appid={api}&units=metric"   
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def sesion_db(engine,table):
    metadata = MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()
    users_table = Table(f'{table}', metadata, autoload_with=engine)
    return session, users_table 

def transform_data(data,table,transform_func):
    session, users_table  = sesion_db(engine,table)
    transform_func(data, session, users_table)
    session.close()

def transform_current_data(data,session,users_table):
    dt_timestamp = datetime.utcfromtimestamp(data['dt'])
    local_dt_timestamp = dt_timestamp.replace(tzinfo=pytz.utc).astimezone(local_timezone)
    dt_sunrice = datetime.utcfromtimestamp(data['sys']['sunrise'])
    local_dt_sunrice = dt_sunrice.replace(tzinfo=pytz.utc).astimezone(local_timezone)
    dt_sunset =  datetime.utcfromtimestamp(data['sys']['sunset'])
    local_dt_sunset = dt_sunset.replace(tzinfo=pytz.utc).astimezone(local_timezone)
    values = {'weather_id': data['weather'][0]['id'],
                    'weather_main': data['weather'][0]['main'],
                    'weather_description': data['weather'][0]['description'],
                    'temp': data['main']['temp'],
                    'feels_like': data['main']['feels_like'],
                    'temp_min': data['main']['temp_min'],
                    'temp_max': data['main']['temp_max'],
                    'pressure': data['main']['pressure'],
                    'humidity': data['main']['humidity'],
                    'visibility': data['visibility'],
                    'wind_speed': data['wind']['speed'],
                    'wind_deg': data['wind']['deg'],
                    'clouds': data['clouds']['all'],
                    'data_time' : local_dt_timestamp,
                    'sunrise' :  local_dt_sunrice,
                    'sunset' : local_dt_sunset,
                    'city_id': data['id'],
                    'time_insert': pd.to_datetime('today')                      
              }
    
    insert_stmt = insert(users_table).values(**values)
    upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['data_time'],
                set_={'time_insert': insert_stmt.excluded.time_insert})
    session.execute(upsert_stmt)
    session.commit()
    current_time = time.ctime()
    print(f"Data inserted or updated in weatherdata on {current_time}")
    return None

def transform_forecast_data(data,session,users_table):
    for l in range(0,len(data['list'])):
        try:
            dt_timestamp = datetime.utcfromtimestamp(data['list'][l]['dt'])
            values = {'data_time': dt_timestamp,
                                    'temp' : data['list'][l]['main']['temp'],
                                     'feels_like': data['list'][l]['main']['temp'],
                                     'temp_min': data['list'][l]['main']['temp_min'],
                                     'temp_max': data['list'][l]['main']['temp_max'],
                                     'pressure': data['list'][l]['main']['pressure'],
                                     'sea_level': data['list'][l]['main']['sea_level'],
                                     'grnd_level': data['list'][l]['main']['grnd_level'],
                                     'humidity': data['list'][l]['main']['humidity'],
                                     'weather_main': data['list'][l]['weather'][0]['main'],
                                     'weather_description': data['list'][l]['weather'][0]['description'],
                                     'clouds': data['list'][l]['clouds']['all'],
                                     'wind_speed': data['list'][l]['wind']['speed'],
                                     'wind_deg': data['list'][l]['wind']['deg'],
                                     'visibility': data['list'][l]['visibility'],
                                     'date_text': data['list'][l]['dt_txt'],
                                     'city_id': data['city']['id'],
                                     'time_insert': pd.to_datetime('today')}

            insert_stmt = insert(users_table).values(**values)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['data_time'],
                set_={'time_insert': insert_stmt.excluded.time_insert}
            )
            session.execute(upsert_stmt)
            session.commit()
            current_time = time.ctime()
        except Exception as e:
            print(f"Error en la fila {l + 1}: {e}") 
    print(f"Data inserted or updated in forecastdata on {current_time}")   
    return None
def get_daily_etl():
    weather_data = req('Mexico City, MX','weather')
    transform_data(weather_data, 'weatherdata', transform_current_data)
    return None

def get_forecasting_etl():
    forecast = req('Mexico City, MX','forecast')
    transform_data(forecast, 'forecastdata', transform_forecast_data)
    return None

def task_schudle():
    schedule.every(2).minutes.do(get_daily_etl)
    schedule.every(5).hours.do(get_forecasting_etl)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    task_schudle()
