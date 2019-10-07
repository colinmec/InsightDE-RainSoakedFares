# Rain Soaked Fares
A multi-data source pipeline for analysis of transportation business under weather events.

## Introduction
Weather is a major source of risk and uncertainty for some business sectors, for example, agriculture, tourism and transportation. For ground logistics, some sources projected that $2.2B to $3.5B is lost to day-to-day weather events [[Link]](https://www.fleetowner.com/blog/mitigating-weather-s-impact-trucking), and similar magnitude is expected for other transportation services. My objective here is to create a data pipeline to fascilitate weather-informed business analytics. This might help businesses to better forecast demands, minimize adverse impacts due to weather, improve user experience, identify new business opportunities and prepare for long-term effects such as climate change.

## Combining separate data sources into a unifying data-frame
Transportation business data is provided by New York City Taxi and Limousine Commission. [[Link]](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) A copy of the data, in .csv format, can be found in AWS S3 public repository with bucket name 'nyc-tlc'. The data contains individual taxi trips and associated information such as pick-up and drop-off times, locations, distance, fare, tip and various charges. A sample can be found in 'test/sampleData' directory.

Historical weather reports are sourced from NOAA Integrated Surface Data FTP server. [[Link]](https://www.ncdc.noaa.gov/isd/data-access) In addition to raw weather station observations, NOAA maintains a much reduced version of the data, which is interpolated with respect to top-of-the-hour time. Albeit less accurate, using this interpolated dataset can reduce computation time by a large margin. This dataset contains 8 weather observations: air temperature, dew point, pressure, wind direction, wind speed, cloud coverage, precipitation with 1 hour and 6 hours accumulation time.

The weather data is in fixed width format and needs to be reformatted into .csv to be compatible with the main ETL process. Four weather stations around New York City are selected. To improve computation speed and organization, it is partitioned by month, in the same manner as the taxi data. Samples can be found in 'test/sampleData'.

## Architecture/Pipeline
![Tech Stack](https://github.com/colinmec/InsightDE-RainSoakedFares/blob/master/Tech%20Stack.png)

The main work load of Spark is to clean up taxi data and incorporate weather information for each taxi trip, using data from appropriate weather station. Results are stored in PostgreSQL and the following fields are retained:  
    `VendorID`      : Vendor ID  
    `pUTimeStamp`   : Pick-up timestamp  
    `dOTimeStamp`   : Drop-off timestamp  
    `pULong`,`pULat`: Pick-up longitude and lattitude (prior to 2017)  
    `dOLong`,`dOLat`: Drop-off longitude and lattitude (prior to 2017)  
    `pULocId`       : Pick-up location ID  
    `dOLocId`       : Drop-off location ID  
    `distance`      : Distance travelled  
    `nPax`          : Number of passengers  
    `fare`          : Fare  
    `tip`           : Tip  
    `totalPaid`     : Total amount = fare + tip + MTA tax + other surcharges  
    `pUAirTemp`     : Air temperature at pick-up  
    `pUCloudCov`    : Cloud coverage at pick-up  
    `pUPrecip1Hr`   : 1-hour accumulated precipitation at pick-up  
    `station`       : Weather station (0: Manhattan, 1: LGA, 2: JFK, 3: Newark)  

## Schema, data cleanliness and other considerations
1. The schema for taxi data changes frequently. This incurs programming and computation overhead.  
2. Bad schema is found in taxi data from June to December 2016.  
3. Misplaced taxi data entries such as anachronous timestamps are commonplace  
4. Central-Park weather station has missing data between April and August 2012

## Front-end features
1. Selection of charts by month  
2. Daily aggregated metric along with precipitation  
3. Metric selection: fare  

## Codes and instructions
Source code directory: src  
Main:  
`noaaFtpToS3Bucket`: Reformats and partitions weather data; stores results in S3  
`ETLMain`: Main PySpark routine  
`resultViewer`: Dash front-end applications  
Others:  
`globalVar`: Contains misc. variables and weather schema definitions  
`parseISD`: Called by `noaaFtpToS3Bucket` to parse weather data  
`datetimeTools`: Contains datetime modules found in Python 3 but not Python 2  
`S3Tools`: Functions for read, write and transfer files in S3  
`dataProcessing`: Called by `ETLMain`, contains main ETL functions  
`getTaxiFields`: Called by `dataProcessing` to deal with inconsistent taxi schema  
`appendWeatherData`: Called by `dataProcessing` to incorporate weather data into taxi data  
`postgres`: Called by `dataProcessing` to write PostgreSQL database  

Spark command line:  
`spark-submit --master spark://<master DNS>:7077 --packages org.postgresql:postgresql:42.1.1 ETLMain.py`

## Potential add-ins
1. Machine learning results for investigating behavioral pattern in relation to weather, such as those from clustering.
