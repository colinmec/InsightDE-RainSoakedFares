# -*- coding: utf-8 -*-
# Contains front-end application methods:
# 1. Show total daily fare with precipitation info.
# 2. Show accumulated daily precipitation
# Author: Colin Chow
# Date created: 10/3/2019
# Version:

import os
import psycopg2 as pg
import pandas   as pd
import numpy    as np
import pandas.io.sql as psql

import dash
import dash_core_components as dcc
import dash_html_components as html

from datetime import datetime as dt
from dash.dependencies import Input, Output, State

def dayLabel(intKey):
    # Gives week day label
    return {0:'M',1:'Tu',2:'W',3:'Th',4:'F',5:'Sa',6:'Su'}.get(intKey)

def nameOfMonth(digits):
    # Gives name of month
    return {'01':'January'  , '02':'February', '03':'March'   , '04':'April'  ,
            '05':'May'      , '06':'June'    , '07':'July'    , '08':'August' ,
            '09':'September', '10':'October' , '11':'November', '12':'December'
           }.get(digits)

def stationName(station):
    # Gives station name
    return {0:'Manhattan/Bronx', 1:'La-Guardia/Flushing', 
            2:'Queens/Brooklyn', 3:'Staten Island'}.get(station)

def fixPrecip(val):
    # NOAA use -0.1 to represent trace precipitation (< instrumental precision)
    # Convert -0.1 to 0.05 (arbitrarily defined as 0.5*precision)
    if val < 0:
        return -val/2
    else:
        return val
    
def isRaining(val):
    # Returns a 3-element bit array indicating:
    # 0: pouring rain, 1: drizzle, 2: dry
    # Thresholds are arbitrarily defined
    pouring   = 20
    littleWet = 0.5
    if val > pouring:
        return [1, 0, 0]
    elif val > littleWet:
        return [0, 1, 0]
    else:
        return [0, 0, 1]

def getBarChartData(metric, year, month, station):
    # Generate report for plots
    # Load data from postgreSQL
    conn = pg.connect(dbname   = "taxi_and_weather_5pc"  , \
                      user     = os.environ['PGUSER']    , \
                      password = os.environ['PGPASSWORD'], \
                      host     = os.environ['PGHOST'])
    sqlStr = "SELECT * FROM yellow_" + year + "_" + month
    df     = psql.read_sql(sqlStr, conn)

    # Get selected station, append dates and fix precipitation values
    df           = df[(df['station'] == station)]
    df['year']   = [str(dt.fromtimestamp(ts).year) for ts in df['pUTimeStamp']]
    df['month']  = [str(dt.fromtimestamp(ts).month).zfill(2) for ts in df['pUTimeStamp']]
    df['day']    = [dt.fromtimestamp(ts).day  for ts in df['pUTimeStamp']]
    df['hour']   = [dt.fromtimestamp(ts).hour for ts in df['pUTimeStamp']]
    df['wkday']  = [dayLabel(dt.fromtimestamp(ts).weekday()) for ts in df['pUTimeStamp']]
    df['precip'] = [fixPrecip(val) for val in df['pUPrecip1Hr']]

    # Drop column 'pUPrecip1Hr' and discard out of place rows
    df = df.drop(columns = ['pUPrecip1Hr'])
    df = df[(df['year']    == year)]
    df = df[(df['month']   == month)]
    
    if metric == 0:
        yAxis  = getDailyFare(df)
        title  = 'Daily fare revenue for {} {} within {}' \
                 .format(nameOfMonth(month), year, stationName(station))
        yLabel = 'Amount ($)'
    elif metric == 1:
        yAxis  = getDailyTip(df)
        title  = 'Daily total tip for {} {} within {}' \
                 .format(nameOfMonth(month), year, stationName(station))
        yLabel = 'Amount ($)'
    elif metric == 2:
        yAxis  = getDailyPaid(df)
        title  = 'Daily total amount for {} {} within {}' \
                 .format(nameOfMonth(month), year, stationName(station))
        yLabel = 'Amount ($)'
    elif metric == 3:
        yAxis  = getAvgSpeed(df)
        title  = 'Average vehicle speed for {} {} within {}' \
                 .format(nameOfMonth(month), year, stationName(station))
        yLabel = 'MPH'
    elif metric == 4:
        yAxis  = getAvgFPM(df)
        title  = 'Average fare per mile for {} {} within {}' \
                 .format(nameOfMonth(month), year, stationName(station))
        yLabel = 'Dollar per mile'
        
    # Aggregate total precip amount by hour and by day
    sum_hr = df[['day','wkday','hour','precip']].drop_duplicates()
    sum_dy = sum_hr.groupby(['day','wkday'],as_index=False).agg({'precip':'sum'})
    
    # Setup x- and y-axes
    # Break-up y-axes according to precipitation conditions
    xAxis = [str(val[0]) + '<br>' + val[1] for val in zip(sum_dy['day'], sum_dy['wkday'])]
    yRain = [yAxis[ind]*isRaining(val)[0] for ind, val in enumerate(sum_dy['precip'])]
    yDriz = [yAxis[ind]*isRaining(val)[1] for ind, val in enumerate(sum_dy['precip'])]
    yDry  = [yAxis[ind]*isRaining(val)[2] for ind, val in enumerate(sum_dy['precip'])]
    
    # barData contains arrays to be used in plots
    barData = [{'year':year, 'month':month, 'station':station, 'title':title, 'yLabel':yLabel}]
    barData.append({'x':xAxis,'y':sum_dy['precip'],'type':'bar','name':'Prcp.','marker':{'color':'#0000CD'}})
    barData.append({'x':xAxis,'y':yRain,'type':'bar','name':'RAIN','marker':{'color':'#87CEFA'}})
    barData.append({'x':xAxis,'y':yDriz,'type':'bar','name':'DRIZ','marker':{'color':'#7FFFD4'}})
    barData.append({'x':xAxis,'y':yDry, 'type':'bar','name':'DRY','marker':{'color':'#F0E68C'}})
        
    return barData

def getDailyFare(df):
    sum_dy = df.groupby(['day','wkday'],as_index=False).agg({'fare':'sum'})
    return 20*sum_dy['fare']

def getDailyTip(df):
    sum_dy = df.groupby(['day','wkday'],as_index=False).agg({'tip':'sum'})
    return 20*sum_dy['tip']

def getDailyPaid(df):
    sum_dy = df.groupby(['day','wkday'],as_index=False).agg({'totalPaid':'sum'})
    return 20*sum_dy['totalPaid']

def getAvgSpeed(df):
    dfDist = df[(df['distance'] > 0.0)]
    dfDist['duration'] = dfDist['dOTimeStamp'] - dfDist['pUTimeStamp']
    dfDist = dfDist[(dfDist['duration'] > 0)]
    dfDist['speed'] = 3600*dfDist['distance']/dfDist['duration']
    dfMean = dfDist.groupby(['day','wkday'],as_index=False).agg({'speed':np.mean})
    return dfMean['speed']

def getAvgFPM(df):
    dfDist = df[(df['distance'] > 0.0)]
    dfDist['FPM'] = dfDist['fare']/dfDist['distance']
    dfMean = dfDist.groupby(['day','wkday'],as_index=False).agg({'FPM':np.mean})
    return dfMean['FPM']

#setting up dash
external_stylesheets = ['https://codepen.io/anon/pen/mardKv.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(style={'backgroundColor': '#FFFFFF'}, children=[
    html.H1(
        children='Aggregated results for Yellow Taxi',
        style={
            'textAlign': 'center',
            'color'    : '#000000'
        }
    ),

    html.Div(children='Select from pull-down menu', style={
        'textAlign': 'center',
        'color'    : '#000000'
    }),
    
    html.Div([
        html.Div(children='',
            style=dict(width='20%', display='table-cell'),
            ),
        
        html.Div([
            dcc.Dropdown(
                id='metric',
                options=[
                    {'label': 'Fare',              'value': 0},
                    {'label': 'Tip',               'value': 1},
                    {'label': 'Total paid',        'value': 2},
                    {'label': 'Avg mile per hour', 'value': 3},
                    {'label': 'Avg fare per mile', 'value': 4}
                    ],
                value=0,
                ),
            ],
            style=dict(width='15%', display='table-cell'),
            ),
        
        html.Div(children='',
            style=dict(width='3%', display='table-cell'),
            ),
        
        html.Div([
            dcc.Dropdown(
                id='month',
                options=[
                    {'label': nameOfMonth('01'), 'value': '01'},
                    {'label': nameOfMonth('02'), 'value': '02'},
                    {'label': nameOfMonth('03'), 'value': '03'},
                    {'label': nameOfMonth('04'), 'value': '04'},
                    {'label': nameOfMonth('05'), 'value': '05'},
                    {'label': nameOfMonth('06'), 'value': '06'},
                    {'label': nameOfMonth('07'), 'value': '07'},
                    {'label': nameOfMonth('08'), 'value': '08'},
                    {'label': nameOfMonth('09'), 'value': '09'},
                    {'label': nameOfMonth('10'), 'value': '10'},
                    {'label': nameOfMonth('11'), 'value': '11'},
                    {'label': nameOfMonth('12'), 'value': '12'}
                    ],
                value='03',
                ),
            ],
            style=dict(width='12%', display='table-cell'),
            ),
            
        html.Div([
            dcc.Dropdown(
                id='year',
                options=[
                    {'label': '2019', 'value': '2019'},
                    {'label': '2018', 'value': '2018'},
                    {'label': '2017', 'value': '2017'},
                    {'label': '2016', 'value': '2016'},
                    {'label': '2015', 'value': '2015'},
                    {'label': '2014', 'value': '2014'},
                    {'label': '2013', 'value': '2013'},
                    {'label': '2012', 'value': '2012'},
                    {'label': '2011', 'value': '2011'},
                    {'label': '2010', 'value': '2010'},
                    {'label': '2009', 'value': '2009'}
                ],
                value='2018',
                ),
            ],
            style=dict(width='12%', display='table-cell'),
            ),
        
        html.Div(children='',
            style=dict(width='3%', display='table-cell'),
            ),
        
        html.Div([
            dcc.Dropdown(
                id='station',
                options=[
                    {'label': stationName(0), 'value': 0},
                    {'label': stationName(1), 'value': 1},
                    {'label': stationName(2), 'value': 2},
                    {'label': stationName(3), 'value': 3}
                ],
                value=0,
                ),
            ],
            style=dict(width='15%', display='table-cell'),
            ),
        
        html.Div(children='',
            style=dict(width='20%', display='table-cell'),
            ),
        ],
        
        style = dict(
            width = '100%',
            display = 'table',
            ),
        ),
    
    # Plot daily fare revenue
    dcc.Graph(id='fare',),
    
    # Plot accumulated precipitation
    dcc.Graph(id='precipitation',),

    # Hidden div inside the app that stores the aggregated result
    html.Div(id='result', style={'display': 'none'})
])

@app.callback(
    Output('result' , 'children'),
    [Input('metric' , 'value'),
     Input('month'  , 'value'),
     Input('year'   , 'value'),
     Input('station', 'value')])
def processData(metric, month, year, station):
    barData = getBarChartData(metric, year, month, station)
    return barData

@app.callback(
    Output('fare'  , 'figure'),
    [Input('result', 'children')])
def update_figure(barData):
    return {
        'data': [barData[2], barData[3], barData[4]],
        'layout': {
            'title':barData[0]['title'],
            'xaxis':{'title':'Day of month'},
            'yaxis':{'title':barData[0]['yLabel']},
            'barmode'      :'stack',
            'showlegend'   :True,
            'plot_bgcolor' :'#FFFFFF',
            'paper_bgcolor':'#FFFFFF',
            'font':{'color':'#000000'}
            }
        }

@app.callback(
    Output('precipitation'  , 'figure'),
    [Input('result', 'children')])
def update_figure(barData):
    return {
        'data': [barData[1]],
        'layout': {
            'title':'Precipitation in mm for {} {}'  \
                    .format(nameOfMonth(barData[0]['month']), \
                            barData[0]['year']),
            'xaxis':{'title':'Day of month'},
            'yaxis':{'title':'Total precip. (mm)'  },
            'showlegend'   :True,
            'plot_bgcolor' :'#FFFFFF',
            'paper_bgcolor':'#FFFFFF',
            'font':{'color':'#000000'}
            }
        }
    
if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=False, port=8050, host ='0.0.0.0')
