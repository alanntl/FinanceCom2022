from urllib import robotparser
import numpy as np
import pandas as pd
import os

os.environ['R_HOME'] = os.environ["CONDA_PREFIX"]+'/Lib/R'
from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter


def return_func(data:pd.DataFrame=None, lag:int=24) ->pd.DataFrame:
    df= data
    df = df.select_dtypes(include=[np.float, np.integer])
    return df.pct_change(lag)

def std_func(data:pd.DataFrame=None,window:int = 20)->pd.DataFrame:
    df= data
    df = df.select_dtypes(include=[np.float, np.integer])
    return df.rolling(window).std()

def mean_func(data:pd.DataFrame=None,window:int = 20)->pd.DataFrame:
    df= data
    df = df.select_dtypes(include=[np.float, np.integer])
    return df.rolling(window).mean()

def agg_trade_measures(data: pd.DataFrame=None, time_step:int=1, time_unit:str='minutes', time_zone='Asia/Hong_Kong' ) ->pd.DataFrame:
    
    data = data
    data.to_csv('temporary/temp.csv')
    ro.r.source('./functions/DC_CSV_to_AD_XTS_A.R')
    output = ro.r['dc_tick_csv_to_ad_trades_xts']('TRUE', 'temporary/', 'temp')
    #print(output)
    ro.r.source('./functions/AD_XTS_to_AD_XTS_A.r')
    output = ro.r['ad_xts_rds_to_ad_aggtrademeasures_xts']('TRUE','temporary/', 'temp', time_step, time_unit) 
    #print(output)
    ro.r.source('./functions/AD_XTS_to_AD_CSV.R') 
    rds_file_name = 'temp_'+str(time_step)+time_unit + '.rds'
    output = ro.r['ad_xts_to_ad_csv']('temporary/',rds_file_name)

    filelist = [ f for f in os.listdir('./temporary') ]
    for f in filelist:
        os.remove(os.path.join('./temporary', f))

    with localconverter(ro.default_converter + pandas2ri.converter):
        output = ro.conversion.rpy2py(output)
    output['Datetime'] = pd.to_datetime(output['Datetime'].values, utc=True).tz_convert('Asia/Hong_Kong')
    
    return output

def agg_trade_measures_multi_tickers(data: pd.DataFrame=None, time_step:int=1, time_unit:str='minutes', time_zone='Asia/Hong_Kong' ) ->pd.DataFrame:
    output_list = []
    rics = data['RIC'].unique()

    for ric in rics:
        df = data[data['RIC']== ric]
        df.to_csv('temporary/temp.csv')
        ro.r.source('./functions/DC_CSV_to_AD_XTS.R')
        output = ro.r['dc_tick_csv_to_ad_trades_xts']('TRUE', 'temporary/', 'temp')
        #print(output)
        ro.r.source('./functions/AD_XTS_to_AD_XTS.r')
        output = ro.r['ad_xts_rds_to_ad_aggtrademeasures_xts']('TRUE','temporary/', 'temp', time_step, time_unit) 
        #print(output)
        ro.r.source('./functions/AD_XTS_to_AD_CSV.R') 
        rds_file_name = 'temp_'+str(time_step)+time_unit + '.rds'
        output = ro.r['ad_xts_to_ad_csv']('temporary/',rds_file_name)

        filelist = [ f for f in os.listdir('./temporary') ]
        for f in filelist:
            os.remove(os.path.join('./temporary', f))

        with localconverter(ro.default_converter + pandas2ri.converter):
            output = ro.conversion.rpy2py(output)

        try:
            output['Datetime'] = pd.to_datetime(output['Datetime'].values, utc=True).tz_convert('Asia/Hong_Kong')
        except:
            pass

        output_list.append(output)
    
    output_multi = pd.concat(output_list)
    return output_multi
