# ETL_OPENWEATHER
For this ETL process, I worked with the openweather API, extracting daily information and forecast, transforming the data in python/pandas and storing it in postgresql using sqlalchemy. Data is automatically requested every 5 min for daily data and every 5 hours for forecast data using the schedule library.

### Architecture 
![alt text](https://github.com/dariog721/ETL_OPENWEATHER/blob/main/ETL_PROCESS_OPENWEATHER.png)

### Data Used
Openweather API https://openweathermap.org/api

### Libraries used

- Request
- Pandas
- Sqlalchemy
- Schedule

  ### Dashboard
  ![alt text](https://github.com/dariog721/ETL_OPENWEATHER/blob/main/DASH_DAYLI.png)
   ![alt text](https://github.com/dariog721/ETL_OPENWEATHER/blob/main/DASH_FORECAST.png)
