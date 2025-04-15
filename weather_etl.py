import json
import requests
import pandas as pd 
from datetime import datetime

# Get the weather data from the API - i.e. EXTRACT
api_key = "8ba78d45122a26e397f45ea63c827de9"
city = "London,uk"
url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"

response = requests.get(url)

# The following 2 lines are just a way to take raw data from an API, make it easy to work with in Python, and then format it nicely for displaying.
data = response.json() #️This takes the data from the API (in JSON format) and turns it into something Python can understand — like a dictionary.
json_str = json.dumps(data, indent=4) #️This takes that Python dictionary and turns it back into a nicely formatted JSON string, mainly for displaying or saving it neatly.

# print(json_str)

# TRANSFORMATION
forecast_list = data["list"] 
df = pd.json_normalize(forecast_list) # This puts the dictionary into a table format so it is easier to read 
# print(df.head(10))

# print(df.columns.tolist()) #this allows you to view all the columns

# Drop unwanted columns
df = df.drop(["weather", "dt_txt", "clouds.all", "wind.deg", "wind.gust", "visibility", "sys.pod", "main.pressure", "main.sea_level", "main.grnd_level", "main.temp_kf"], axis=1) #️axis=1 means we are dropping a column, axis=0 would mean row

# Rename columns
df = df.rename(columns={
    "dt": "Date Time",
    "pop": "Probability of Precipitation",
    "main.temp": "Temperature",
    "main.feels_like": "Feels Like",
    "main.temp_min": "Min Temperature",
    "main.temp_max": "Max Temperature",
    "main.humidity": "Humidity",
    "wind.speed": "Wind Speed",
    "rain.3h": "Rain Volume in mm (Last 3 hours)",
    
})

# Reordering columns
df = df[["Date Time", "Temperature", "Feels Like", "Min Temperature", "Max Temperature", "Rain Volume in mm (Last 3 hours)", "Probability of Precipitation", "Humidity", "Wind Speed"]]

# Changing format of columns
df["Date Time"]






# print(df.head())





# if response.status_code == 200:
#     data = response.json()
#     weather_data = []
#     for x in data["list"]:
#         date_time = 