import json
import requests
import pandas as pd 
from datetime import datetime
import os 
from dotenv import load_dotenv
import psycopg2 #For connecting to PostgreSQL databases and executing queries
from sqlalchemy import create_engine # To efficiently manage and reuse database connections.
from sqlalchemy import text


# Get the weather data from the API - i.e. EXTRACT
def download_weather_api(city):
    load_dotenv()
    api_key = os.getenv("api_key")
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"

    response = requests.get(url)
    data = response.json() #️This takes the data from the API (in JSON format) and turns it into something Python can understand — like a dictionary.
    
    return data 

def extract(cities):
    all_data = []

    for city in cities:
        city_data = download_weather_api(city)
        all_data.append(city_data)

    return all_data


# TRANSFORM
def transform(data):
    forecast_list = data["list"] 
    df = pd.json_normalize(forecast_list) # This puts the dictionary into a table format so it is easier to read 


    # print(df.columns.tolist()) #this allows you to view all the columns

    # Drop unwanted columns
    df = df.drop(["weather", "dt_txt", "clouds.all", "wind.deg", "wind.gust", "visibility", "sys.pod", "main.pressure", "main.sea_level", "main.grnd_level", "main.temp_kf"], axis=1) #️axis=1 means we are dropping a column, axis=0 would mean row

    # Rename columns
    df = df.rename(columns={
        "dt": "date_time",
        "pop": "probability_of_precipitation (%)",
        "main.temp": "temperature (°C)",
        "main.feels_like": "feels_like (°C)",
        "main.temp_min": "min_temperature (°C)",
        "main.temp_max": "max_temperature (°C)",
        "main.humidity": "humidity (%)",
        "wind.speed": "wind_speed (metres/s)",
        "rain.3h": "rain_vol (mm)",
        
    })

    print(df.columns.tolist())

    # Reordering columns
    df = df[["date_time", "temperature (°C)", "feels_like (°C)", "min_temperature (°C)", "max_temperature (°C)", "rain_vol (mm)", "probability_of_precipitation (%)", "humidity (%)", "wind_speed (metres/s)"]]

    # Changing format of columns / dealing with NaN values
    df["date_time"] = pd.to_datetime(df["date_time"], unit='s')
    df["probability_of_precipitation (%)"] = df["probability_of_precipitation (%)"] * 100
    df["temperature (°C)"] = (df["temperature (°C)"] - 273.15).round(2)
    df["feels_like (°C)"] = (df["feels_like (°C)"] - 273.15).round(2)
    df["min_temperature (°C)"] = (df["min_temperature (°C)"] - 273.15).round(2)
    df["max_temperature (°C)"] = (df["max_temperature (°C)"] - 273.15).round(2)
    df["rain_vol (mm)"] = df["rain_vol (mm)"].fillna(0)

    pd.set_option('display.max_columns', None)
    print(df.head())

    return df


# LOAD
def load(df):
    db_username = os.getenv("db_username")
    db_password = os.getenv("db_password")
    db_host = "localhost"
    db_port = 5432
    db_name = "postgres"

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")


    # Test the connection (this part isn't needed - just for verification purposes)
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            for row in result:
                print("Connected to PostgreSQL!")
                print("PostgreSQL version:", row[0])
    except Exception as e:
        print("Failed to connect to the database.")
        print("Error:", e)

    # Replace 'weather_forecast' with your desired table name
    df.to_sql(
        name='weather_forecast',        # Table name in the database
        con=engine,                     # The SQLAlchemy engine you created
        if_exists='replace',           # Replace table if it already exists (options: 'fail', 'replace', 'append')
        index=False                     # Don't write DataFrame index as a column
    )
    print("✅ Data loaded to PostgreSQL successfully!")

if __name__ == "__main__":
    raw_data = download_weather_api("Paris")
    transformed_df = transform(raw_data)
    load(transformed_df)


