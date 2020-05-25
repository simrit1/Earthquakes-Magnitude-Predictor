"""
    DATA VISUALISATION
"""

# LIBRAIRIES

import math ; from math import pi 
import pandas as pd

from bokeh.io               import output_notebook, output_file, curdoc
from bokeh.layouts          import gridplot, layout
from bokeh.plotting         import figure, show, save, ColumnDataSource
from bokeh.models.tools     import HoverTool
from bokeh.palettes         import Category20c
from bokeh.transform        import cumsum
from bokeh.tile_providers   import get_provider, Vendors
from bokeh.themes           import built_in_themes

from pymongo import MongoClient

# READ FROM MONGO

def read_mongo(
    host     = '127.0.0.1',
    port     = 27017,
    username = None, 
    password = None,
    db       = None,
    coll     = None 
):
    mongo_uri  = 'mongodb://{}:{}/{}.{}'.format(
        host, port, 
        db,   coll
    )
    connection = MongoClient(mongo_uri)
    database   = connection[db]
    cursor     = database[coll].find()
    data       = pd.DataFrame(list(cursor)) ; del data['_id']

    return data

df_earthquakes = read_mongo(db = 'Earthquakes', coll = 'earthquakes')
df_predictions = read_mongo(db = 'Earthquakes', coll = 'predictions')
df_summary     = read_mongo(db = 'Earthquakes', coll = 'summary')

df_earthquakes_2016 = df_earthquakes[df_earthquakes['Year'] == 2016]

# MAP

def bo_style(p):
    # title
    p.title.align           = 'center'
    p.title.text_font_size  = '20pt'
    p.title.text_font       = 'serif'
    # axis title
    p.xaxis.axis_label_text_font_size  = '14pt'
    p.xaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size  = '14pt'
    p.yaxis.axis_label_text_font_style = 'bold'
    # ticks
    p.xaxis.major_label_text_font_size  = '12pt'
    p.yaxis.major_label_text_font_size  = '12pt'
    # legend
    p.legend.location = 'top_left'

    return p

def map_equakes():
    """
        Maps the earthquakes 
    """
    lat = df_earthquakes_2016['Latitude' ].values.tolist()
    lon = df_earthquakes_2016['Longitude'].values.tolist()
    
    lat_pred = df_predictions['Latitude' ].values.tolist()
    lon_pred = df_predictions['Longitude'].values.tolist()
    
    lst_lat      = []
    lst_lon      = []
    lst_lat_pred = []
    lst_lon_pred = []

    i, j = 0, 0

    for i in range(len(lon)):
        """
            Converts Lon/Lat to Merc proj
        """
        r_major = 6378137.000
        x       = r_major * math.radians(lon[i])
        scale   = x / lon[i]
        y = 180.0/math.pi * math.log(math.tan(math.pi/4.0 + lat[i] * (math.pi/180.0)/2.0)) * scale
        lst_lon.append(x)
        lst_lat.append(y)
        
        i +=1
    
    for j in range(len(lon_pred)):
        """
            Converts predicted Lon/Lat to Merc proj
        """
        r_major = 6378137.000
        x       = r_major * math.radians(lon_pred[j])
        scale   = x / lon_pred[j]
        y       = 180.0/math.pi * math.log(math.tan(math.pi/4.0 + lat_pred[j] * (math.pi/180.0)/2.0)) * scale
        
        lst_lon_pred.append(x)
        lst_lat_pred.append(y)
        
        j +=1
    
    df_earthquakes_2016['coo_x'] = lst_lat
    df_earthquakes_2016['coo_y'] = lst_lon
    df_predictions['coo_x'] = lst_lat_pred
    df_predictions['coo_y'] = lst_lon_pred

    df_earthquakes_2016['Magnitude_size'] = df_earthquakes_2016['Magnitude'] *4
    df_predictions['Magnitude_size']      = df_predictions['Magnitude_pred'] *4

    latitudes       = df_earthquakes_2016['coo_x'].tolist()
    longitudes      = df_earthquakes_2016['coo_y'].tolist()
    magnitudes      = df_earthquakes_2016['Magnitude'].tolist()
    years           = df_earthquakes_2016['Year'].tolist()
    magnitude_sizes = df_earthquakes_2016['Magnitude_size'].tolist()

    latitudes_pred       = df_predictions['coo_x'].tolist()
    longitudes_pred      = df_predictions['coo_y'].tolist()
    magnitudes_pred      = df_predictions['Magnitude_pred'].tolist()
    years_pred           = df_predictions['Year'].tolist()
    magnitude_sizes_pred = df_predictions['Magnitude_size'].tolist()

    cds = ColumnDataSource(
        data = dict(
            latitude        = latitudes,
            longitude       = longitudes,
            magnitude       = magnitudes,
            year            = years,
            magnitude_size  = magnitude_sizes
        )
    )

    cds_pred = ColumnDataSource(
        data = dict(
            latitude_pred       = latitudes_pred,
            longitude_pred      = longitudes_pred,
            magnitude_pred      = magnitudes_pred,
            year_pred           = years_pred,
            magnitude_size_pred = magnitude_sizes_pred
        )
    )

    TOOLTIPS = [
        ("Year", " @year"),
        ("Magnitude", " @magnitude"),
        ("Predicted Magnitude", " @magnitude_pred")
    ]

    tile_provider = get_provider(Vendors.CARTODBPOSITRON_RETINA)

    map = figure(
        title       = "Earthquake map",
        plot_width  = 1600,
        plot_height = 650, 
        x_range     = (-2000000, 6000000),
        y_range     = (-1000000, 7000000),
        tooltips    = TOOLTIPS,
        toolbar_location = "left"
    )

    map.add_tile(tile_provider)

    map.circle(
        x          = 'longitude', 
        y          = 'latitude',
        size       = 'magnitude_size', 
        fill_color = '#cc0000',
        fill_alpha = .7, 
        source     = cds,
        legend     = "Earthquakes [2016]"
    )

    map.circle(
        x          = 'longitude_pred', 
        y          = 'latitude_pred',
        size       = 'magnitude_size_pred', 
        fill_color = '#6897BB',
        fill_alpha = .7, 
        source     = cds_pred,
        legend     = "Predicted magnitude [2017]"
    )

    map.title.align          = 'center'
    map.title.text_font_size = '20pt'
    map.title.text_font      = 'serif'

    map.legend.location              = 'bottom_right'
    map.legend.background_fill_color = 'black'
    map.legend.background_fill_alpha = .8
    map.legend.click_policy          = 'hide'
    map.legend.label_text_color      = 'white'
    
    map.xaxis.visible = False
    map.yaxis.visible = False

    map.axis.axis_label = None
    map.axis.visible    = False

    map.grid.grid_line_color = None

    return map

# BARPLOT

def plot_bar():
    cds = ColumnDataSource(
        data = dict(
            years    = df_summary['Year' ].values.tolist(),
            n_quakes = df_summary['Count'].values.tolist()
        )
    )
    
    TOOLTIPS = [
        ('Year', ' @years'),
        ('Number of earthquakes', ' @n_quakes')
    ]
    
    barChart = figure(
        title            = 'Earthquakes Frequency by Year',
        plot_height      = 350,
        plot_width       = 800,
        x_axis_label     = 'Years',
        y_axis_label     = 'Number of Occurances',
        x_minor_ticks    = 2,
        y_range          = (0, df_summary['Count'].max() +100),
        toolbar_location = 'left',
        tooltips         = TOOLTIPS
    )
    
    barChart.vbar(
        x      = 'years', 
        bottom = 0, 
        top    = 'n_quakes', 
        color  = '#008744', 
        width  = .75,    
        legend = 'n earthquakes', 
        source = cds
    )
    
    barChart = bo_style(barChart)

    return barChart
    
# MAGNITUDE PLOT

def plot_magnitude():
    cds = ColumnDataSource(
        data = dict(
            years         = df_summary['Year'].values.tolist(),
            magnitude_avg = df_summary['Magnitude_avg'].round(1).values.tolist(),
            magnitude_max = df_summary['Magnitude_max'].values.tolist()
        )
    )
    
    TOOLTIPS = [
        ('Year', ' @years'),
        ('Average Magnitude', ' @magnitude_avg'),
        ('Maximum Magnitude', ' @magnitude_max')
    ]
    
    mag = figure(
        title            = 'Maximum and Average Magnitudes by Year',
        plot_height      = 350,
        plot_width       = 800, 
        x_axis_label     = 'Years',
        y_axis_label     = 'Magnitude',
        x_minor_ticks    = 2,
        y_range          = (5, df_summary['Magnitude_max'].max() +1),
        toolbar_location = 'left',
        tooltips         = TOOLTIPS
    )
    
    mag.line(
        x          = 'years', 
        y          = 'magnitude_max', 
        color      = '#cc0000', 
        line_width = 2, 
        legend     = 'Magnitude (max)', 
        source     = cds
    )
    mag.circle(
        x          = 'years', 
        y          = 'magnitude_max', 
        color      = '#cc0000', 
        size       = 8, 
        fill_color = '#cc0000', 
        source     = cds
    )
    
    mag.line(
        x          = 'years', 
        y          = 'magnitude_avg', 
        color      = 'yellow', 
        line_width = 2, 
        legend     = 'Magnitude (avg)', 
        source     = cds
    )
    mag.circle(
        x          = 'years', 
        y          = 'magnitude_avg', 
        color      = 'yellow', 
        size       = 8, 
        fill_color = 'yellow', 
        source     = cds
    )
    
    mag = bo_style(mag)
    
    return mag
    
# CREATE DASHBOARD

output_file('templates/dashboard.html')
curdoc().theme = 'dark_minimal'
board = layout(
    [
        [map_equakes()], 
        [plot_bar(), plot_magnitude()]
    ]
)
save(board)