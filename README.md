# Rain Soaked Fares
A multi-data source pipeline for analysis of transportation business under weather events.

## Introduction
Weather is a major source of risk and uncertainty for some business sectors, for example, agriculture, tourism and transportation. For ground logistics, some sources projected that $2.2B to $3.5B is lost to day-to-day weather events [[Link]](https://www.fleetowner.com/blog/mitigating-weather-s-impact-trucking), and similar magnitude is expected for other transportation services. My objective here is to create a data pipeline to fascilitate weather-informed business analytics. This might help businesses to better forecast demands, minimize adverse impacts due to weather, improve user experience, identify new business opportunities and prepare for long-term effects such as climate change.

## Combining separate data sources into a unifying data-frame
Transportation business data is provided by New York City Taxi and Limousine Commission. [[Link]](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) A copy of the data can be found in AWS S3 public repository named 'nyc-tlc'. The data contains individual taxi trips and associated information such as pick-up and drop-off time, location, distance, fare, tip and various charges. A sample can be found in 'test/sample data' directory

Historical weather reports are sourced from NOAA

## Architecture/Pipeline
![Tech Stack](https://github.com/colinmec/InsightDE-RainSoakedFares/blob/master/Tech%20Stack.png)

## Front-end features
- Metric definitions: e.g., fare, duration, fare/mile, duration/mile
- Bar chart: metric for dry vs wet
- Historical curve
- Projections

## Potential add-ins
