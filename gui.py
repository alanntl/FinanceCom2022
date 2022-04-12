import panel as pn
import numpy as np
import holoviews as hv
import ast
import param
from google_auth_oauthlib import flow
from google.cloud import bigquery
pn.extension('terminal','tabulator', sizing_mode='stretch_width')
import pandas as pd
import inspect
from collections import ChainMap
import os
import datetime
import shutil
import pathlib
import plotly.express as px
import func
import time
import webbrowser

os.environ['R_HOME'] = os.environ["CONDA_PREFIX"]+'/Lib/R'
from rpy2 import robjects as ro

class AuthBlock(param.Parameterized):
    def __init__(self, **params):
        self.appflow = None
        self.client = None
  
        self.file_input =pn.widgets.FileInput( name='Upload credentials file',accept='.json')
        self.messagebox = pn.widgets.TextInput( name='Message')
        self.btn_auth = pn.widgets.Button(name='Authenticate', button_type='primary')
        self.btn_proj = pn.widgets.Button(name='Enable BigQuery project', button_type='primary')
        self.text_project = pn.widgets.TextInput( name='BigQuery project name', value = 'cse-refinitivetickhistory')
        self.file_input.param.watch(self.file_input_get, 'value')
        self.btn_auth.on_click(self.btn_auth_click)
        self.btn_proj.on_click(self.btn_proj_click)

    def btn_proj_click(self,event):
        if self.appflow == None :
            self.messagebox.value = 'Please authenticate API service with the credentials file!'
            return
        if self.text_project.value == '' :
            self.messagebox.value = 'Please input the BigQuery project name!'
            return
        
        self.client = bigquery.Client(project=self.text_project.value, credentials=self.appflow.credentials)
        if self.client == None:
            self.messagebox.value = 'Project name error!'
        else: 
            self.messagebox.value = 'BigQuery is ready to use! '
            self.btn_proj.button_type= 'success'

    def btn_auth_click(self,event):
        if self.file_input.value == None :
            self.messagebox.value = 'Please upload the credentials file!'
            return
        c_config = ast.literal_eval(self.file_input.value.decode("UTF-8"))
        self.appflow = flow.InstalledAppFlow.from_client_config(client_config = c_config, scopes=["https://www.googleapis.com/auth/bigquery"])
        self.messagebox.value = 'Please sign in google account on the browser'
        self.appflow.run_local_server() #appflow.run_console()
        self.messagebox.value = 'Success. Your refresh token is: '+ str(self.appflow.credentials.refresh_token)
        self.btn_auth.button_type= 'success'

    def file_input_get(self,event):
        c_config = ast.literal_eval(self.file_input.value.decode("UTF-8"))
        self.messagebox.value = str(c_config)

    @property
    def view(self):
        return pn.Column(
            pn.widgets.StaticText(value='Upload credential file:'),
            self.file_input,
            self.btn_auth,
            self.text_project,
            self.btn_proj,
            self.messagebox,
            )




query_example = '''SELECT RIC, Date_Time, Price,Volume, Market_VWAP, Qualifiers AS Qualifiers, Ex_Cntrb_ID, Qualifiers AS TradeCategory, GMT_Offset 
FROM `dbd-sdlc-prod.HKG_NORMALISED.HKG_NORMALISED` 
WHERE RIC = "1606.HK" 
AND (Date_Time BETWEEN TIMESTAMP("2016-07-11 00:00:00.000000") AND TIMESTAMP("2016-07-11 23:59:59.999999")) 
AND Type="Trade" AND Volume >0 AND Price >0
'''


class QueryBlock(param.Parameterized):
    def __init__(self, client, datapath='./data/', query_example= query_example):
        self.datapath = datapath
        self.client = client
        self.bqfile =None
        self.data = None
        self.messagebox = pn.widgets.TextInput( name='Message',placeholder='Input query string and click button!')
        self.query_area = pn.widgets.TextAreaInput(name='Query string', placeholder= 'Input query string here...' ,height=150, value=query_example)
        self.btn_query = pn.widgets.Button(name= 'Get data', button_type='primary')
        self.text_filename= pn.widgets.TextInput(name='Save file name as: ', placeholder='data_query_[current_datetime].csv')
        self.btn_query.on_click(self.btn_query_click) 

    def btn_query_click(self,event):
        if self.client == None:
            self.messagebox.placeholder = 'Query client is not registered'
            return
            
        self.messagebox.value = 'System is retriving data, please wait...'
        self.data = None
        query = (self.query_area.value)
        try:
            start_time = time.time()
            self.bqfile = self.client.query(query) 
            #self.data = self.client.query(query) 
            exec_time = time.time() - start_time
            self.messagebox.value = 'Done! '+'Query execution time: '+str( round(exec_time,3))+ ' seconds! Transforming bigquery data object to dataframe...'
            self.data = self.bqfile.to_dataframe() 

            self.messagebox.value = self.messagebox.value +str( round(time.time() - start_time,3))+ ' seconds! '

            if self.text_filename.value =='':
                filename = 'query_' +datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+ '.csv'
            else:
                filename = self.text_filename.value
                filename = filename+'.csv' if '.' not in filename else filename
            
            self.data.to_csv(self.datapath+filename)
            
            self.messagebox.value = self.messagebox.value + '\"' +filename +'\" is saved in the data folder! '+str( round(exec_time,3))+ ' seconds! '
            self.btn_query.button_type='success'            
        except:
            self.messagebox.placeholder = 'Query error!'
            self.btn_query.button_type='primary'

    def feed_client(self,client):
        self.client = client

    @property
    def view(self):
        return pn.Column(
            self.query_area,
            self.text_filename,
            self.btn_query,
            self.messagebox,
            )

class QueryHFDBlock(QueryBlock):
  def __init__(self, client):
    super().__init__(client)
    self.client = client #!
    self.input_select = pn.widgets.TextInput(name='SELECT items', value="RIC, Date_Time, Type, Price, Volume, Market_VWAP, Qualifiers ,Ex_Cntrb_ID, GMT_Offset, Bid_Price, Bid_Size, Ask_Price, Ask_Size")
    self.input_database = pn.widgets.TextInput(name='FROM Database', value="dbd-sdlc-prod.HKG_NORMALISED.HKG_NORMALISED")
    self.input_stock_code =pn.widgets.TextInput(name='Stock code', value= "2800.HK, 0700.HK, 0005.HK, 0001.HK, 0002.HK")
    self.input_start_date =pn.widgets.TextInput(name='Start time', value= "2020-07-11")
    self.input_end_date =pn.widgets.TextInput(name='End time', value= "2020-07-11")
    self.input_conditions =pn.widgets.TextInput(name='Conditions', value= "(Type= \"Trade\" or Type=\"Quote\")")
    self.widget_box = pn.WidgetBox(self.input_select,self.input_database,self.input_stock_code, self.input_start_date,self.input_end_date,self.input_conditions, )
    self.query_area.height = 350

    for object in self.widget_box.objects:
      object.param.watch(self.input_changed, 'value')

  def input_changed(self, event):
    stock_input =''
    for x in self.input_stock_code.value.split(",") : stock_input+= f'\"{x}\", '
    stock_input = 'RIC in ('+stock_input[:-3]+')'

    self.query_area.value= ('SELECT ' +self.input_select.value 
    +'\nFROM ' + '`'+self.input_database.value+ '`'
    +'\nWHERE ' + stock_input
    +'\nAND (Date_Time BETWEEN TIMESTAMP(\"' + self.input_start_date.value +' 00:00:00.000000\")'
    +' AND TIMESTAMP(\"' + self.input_end_date.value +' 23:59:59.999999\"))'
    +'\nAND ' + self.input_conditions.value )

  @property
  def view(self):
      return pn.Column(
            pn.Row(self.widget_box,
            self.query_area),
            self.text_filename,
            self.btn_query,
            self.messagebox,
          )


class FileBlock(param.Parameterized):
    
    def __init__(self, folderpath ,**params):
        self.folderpath = folderpath
        self.selected_file_path = None
        self.messagebox = pn.widgets.TextInput( name='File name',placeholder='Input file name for file rename or duplicate')
        self.filesselector = pn.widgets.MultiSelect(name='Select data files',options = os.listdir(self.folderpath), size=20)
        self.btn_duplicate = pn.widgets.Button(name='Duplicate',button_type='success')
        self.btn_rename = pn.widgets.Button(name='Rename', button_type='primary')
        self.btn_delete = pn.widgets.Button(name='Delete', button_type='danger')
        
        self.file_input =pn.widgets.FileInput( name='Import')
        self.file_save = pn.widgets.Button(name='Import uploaded file')
        self.filedownload = pn.widgets.FileDownload(filename='Download')
        
        self.filesselector.param.watch(self.filesselector_change, 'value')
        self.btn_rename.on_click(self.btn_rename_click)
        self.btn_duplicate.on_click(self.btn_duplicate_click)
        self.btn_delete.on_click(self.btn_delete_click)
        self.file_save.param.watch(self.file_save_clicked, 'value')

    def file_save_clicked(self,event):
        self.file_input.save(self.folderpath+self.file_input.filename)
        self.filesselector.options = os.listdir(self.folderpath)

    def filesselector_change(self,event):
        if self.filesselector.value == []:
            self.messagebox.value = ''
        else:
            self.filesselector.value = [self.filesselector.value[0]]
            self.messagebox.value = self.filesselector.value[0]
            self.filedownload.filename = self.filesselector.value[0]
            self.filedownload.file = self.folderpath + self.filesselector.value[0]
            self.selected_file_path = self.folderpath + self.filesselector.value[0]
            
    def btn_rename_click(self,event):   
        old_filename = self.folderpath + self.filesselector.value[0]
        new_filename = self.folderpath + self.messagebox.value
        os.rename(old_filename, new_filename)
        self.filesselector.options = os.listdir(self.folderpath)

    def btn_duplicate_click(self,event):   
        old_filename = self.folderpath + self.filesselector.value[0]
        new_filename = self.folderpath + self.messagebox.value
        shutil.copyfile(old_filename, new_filename)
        self.filesselector.options = os.listdir(self.folderpath)

    def btn_delete_click(self,event):   
        file = pathlib.Path(self.folderpath + self.filesselector.value[0])
        #self.filesselector.value = []
        file.unlink()
        self.filesselector.options = os.listdir(self.folderpath)

    @property
    def data(self):
        if self.filesselector.value == []:
            return pd.DataFrame({'Empty dataframe' : [np.nan]})
        else:
            file = self.folderpath + self.filesselector.value[0]
            df = pd.read_csv(file)
            df.drop(["Unnamed: 0"], axis=1, inplace=True)
            return df

    @property
    def view(self):
        return pn.Column(
            self.filesselector,
            self.messagebox ,
            pn.Row(self.btn_rename,self.btn_duplicate,self.btn_delete,),
            pn.Row( pn.widgets.StaticText(value='Import file'), self.file_input),
            self.file_save,self.filedownload,
            )


class TabulatorBlock(param.Parameterized):
    def __init__(self,  **params):
        self.tabulator = pn.widgets.Tabulator( pagination='remote', page_size=25)
        
        #self.slide_height = pn.widgets.EditableIntSlider(name='Height', start=0, end=5000, step=50, value=650)
        self.slide_page_size = pn.widgets.EditableIntSlider(name='Page size', start=1, end=1000, step=5, value=25)
        
        #self.slide_height.param.watch(self.slider_changed, 'value')
        self.slide_page_size.param.watch(self.slider_changed, 'value')

    def feed_data(self, data):
        self.tabulator.value = data

    def slider_changed(self, event):
        #self.tabulator.height  = self.slide_height.value
        self.tabulator.page_size = self.slide_page_size.value
        

    @property
    def view(self):
        return pn.Column(
            self.slide_page_size,self, 
            self.tabulator
            )


class ChartBlock(param.Parameterized):
    def __init__(self,  **params):
        self.df= None
        self.tabulator = pn.widgets.Tabulator( pagination='remote')
        self.choidx = pn.widgets.MultiChoice(name='Select index',options=['index'])
        self.mulchocol = pn.widgets.MultiChoice(name='Select value')
        self.seltype = pn.widgets.Select(name='select chart type', options=['scatter','bar','line', 'histogram', 'box'])
        self.plot = pn.pane.Plotly(sizing_mode='fixed')
        #self.choidx.param.watch(self.mulchocol_selected, 'value')
        self.mulchocol.param.watch(self.mulchocol_selected, 'value')
        self.choidx.param.watch(self.mulchocol_selected, 'value')
        self.seltype.param.watch(self.mulchocol_selected, 'value')

        self.slide_height = pn.widgets.EditableIntSlider(name='Height', start=0, end=5000, step=50, value=500)
        self.slide_width = pn.widgets.EditableIntSlider(name='Width',  start=0, end=5000, step=50, value=1500)
        self.slide_height.param.watch(self.slider_changed, 'value')
        self.slide_width.param.watch(self.slider_changed, 'value')

    def slider_changed(self,event):
        self.plot.width = self.slide_width.value
        self.plot.height = self.slide_height.value

    def mulchocol_selected(self,event):
        if self.choidx.value ==[] or self.mulchocol.value == []:
            self.plot.object = None
            return
        
        self.plot.object=None
        df = self.df.convert_dtypes()
        ptype = self.seltype.value
        df['index'] =df.index

        if self.choidx.value[0] != [] or self.choidx.value[0] != 'index':
            index = self.choidx.value[0]
            df.sort_values(by=[index])
            df = df.set_index([index])

        if len(self.choidx.value)> 1:
            event.obj.value = [self.choidx.value[0]]
            return

        df = df[self.mulchocol.value]

        fig =eval('px.'+ptype)(df)
        fig.update_layout(legend=dict(orientation="h",  yanchor="top"))
        self.plot.object = fig
  
    def feed_data(self, data):
        self.choidx.value =[]
        self.mulchocol.value = []
        self.df = data
        self.tabulator.value = data

        columns_list = data.columns.to_list()
        self.choidx.options = ['index']+columns_list
        self.mulchocol.options = columns_list

        self.choidx.value = ['index']
        self.mulchocol.value = [columns_list[0]]

    @property
    def view(self):
        return pn.Column(
            pn.Row(
            self.mulchocol,
            self.choidx,
            self.seltype,),
            pn.Row(self.slide_height, self.slide_width),
            self.plot,
            pn.Card(self.tabulator,collapsed=True, title= 'Expand for table view' )
            )

class FuncBlock(param.Parameterized):
    def __init__(self, module, datapath='./data/'):
        
        self.messagebox = pn.widgets.TextInput( name='Message', placeholder='1 select a function, 2 input parameters , 3 click compute button')
        self.datapath = datapath
        self.module = module
        self.funcs_dict, self.funcs_name_list, self.funcs_argument_list = self.get_function_name() #+type list
        self.btn_compute = pn.widgets.Button(name='Compute', button_type = 'primary')
        self.sel_func = pn.widgets.Select(name='Select a analytic service:', options=['']+self.funcs_name_list)
        self.card = pn.Card()
        self.sel_func.param.watch(self.select_func_change, 'value')
        self.btn_compute.on_click(self.btn_compute_click)
        self.func_name = None
        self.func_param_dict = None
        self.func = None
        self.data_list = None
        self.func_param_dict = [] #!!
        self.result = None
        self.result_name = None
        self.display = pn.Column()
        self.text_input_save = pn.widgets.TextInput(name='Save data name' ,placeholder='result_[current_time].csv')
        self.param_type = {}
        self.data_param_list = []

    def btn_download_click(self,event):
        if type(self.result) == pd.DataFrame:
            self.result.to_csv(self.datapath+self.text_input_save.value)
            self.messagebox.placeholder +=' File saved!'
            data_folder = os.listdir(self.datapath)
            for data_param in self.data_param_list:
                data_param.options = data_folder

    def btn_compute_click(self,event):
        self.btn_compute.button_type = 'primary'
        self.display.clear()
        self.messagebox.placeholder = 'Computing...please wait'
        start_time = time.time()
        self.result = self.func(**self.func_param_dict)
        exec_time = time.time() - start_time
        self.messagebox.placeholder = 'execution time: '+str( round(exec_time,3))+ ' seconds.' +' Saving the file...'
        
        if self.text_input_save.value =='':
            filename = 'result_' +datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+ '.csv'
        else:
            filename = self.text_input_save.value
            filename = filename+'.csv' if '.' not in filename else filename

        self.result.to_csv(self.datapath + filename)
        self.result_name = filename
        self.messagebox.placeholder = str(self.messagebox.placeholder) + filename +' saved!'
        self.btn_compute.button_type = 'success'
    

    def select_func_change(self,event):
        self.display.clear()
        self.btn_compute.button_type = 'primary'
        self.card.title = ''
        self.card.clear()
       
        self.card.title = self.sel_func.value
        self.card.title = self.card.title.replace('{','[').replace('}',']')

        self.func_name = self.sel_func.value
        self.func = eval('self.module.'+self.func_name)

        self.param_type = inspect.getfullargspec(self.func)[6] 
        self.func_param_dict = self.funcs_dict[self.sel_func.value]
        self.param_inputs = []    

        for k,v in self.func_param_dict.items():
            if k =='data':
                arg_input = pn.widgets.MultiChoice(name=k,options=os.listdir(self.datapath) ) # not textinput , is multichoice
                arg_input.param.watch(self.select_data_changed, 'value')
                self.data_param_list.append(arg_input)
            else:
                arg_input = pn.widgets.LiteralInput(name=k, value=v)
                arg_input.param.watch(self.text_changed,'value')
            self.param_inputs.append(arg_input)
        self.card.objects = self.param_inputs

    def select_data_changed(self,event):
        self.display.clear()
        if len(event.obj.value)> 1:
            event.obj.value = [event.obj.value[0]]
            return

        self.btn_compute.button_type = 'primary'
        self.data_list = []
        sinput_list = event.obj.value
        for sinput in sinput_list:
            data = pd.read_csv(self.datapath+sinput)
            #data.drop(["Unnamed: 0"], axis=1, inplace=True)
            self.data_list.append(data)
        self.func_param_dict[event.obj.name] = self.data_list[0] 

    def text_changed(self,event):
        self.btn_compute.button_type = 'primary'
        self.func_param_dict[event.obj.name] = event.obj.value
        self.card.title = self.sel_func.value


    def get_function_name(self):
        funcs = inspect.getmembers(self.module, inspect.isfunction)
        funcs_name_list = [fun[0] for fun in funcs]
        funcs_argument_list = [inspect.getfullargspec(fun[1])[0]  for fun in funcs]
        funcs_argument_default_list = [inspect.getfullargspec(fun[1])[3]  for fun in funcs]
        funcs_dict = [{f:dict(zip(a,d))} for f,a,d in zip(funcs_name_list, funcs_argument_list, funcs_argument_default_list)]
        funcs_dict =dict(ChainMap(*funcs_dict ))
        return funcs_dict, funcs_name_list, funcs_argument_list #+type list

    @property
    def view(self):
        return pn.Column(
            self.sel_func,
            self.card,
            self.text_input_save,
            self.btn_compute,
            self.messagebox,
            pn.layout.Divider(),
            self.display,
            )    
            
class App(param.Parameterized):
    def __init__(self, module= func, datapath='./data/', open_in_browser=True):
        self.authblock = AuthBlock()
        self.fileblock = FileBlock(datapath)
        self.funcblock = FuncBlock(module)
        self.queryblock = QueryHFDBlock(self.authblock.client)
        self.tabulatorblock = TabulatorBlock()
        self.chartblock = ChartBlock()
    

        self.fileblock.filesselector.param.watch(self.filesselector_change_2, 'value')
        self.funcblock.btn_compute.on_click(self.btn_compute_click_2)
        self.authblock.btn_proj.on_click(self.btn_proj_click_2)

        self.template = pn.template.MaterialTemplate(title='Analytics APP for BigQuery + high frequency financial data')
        self.template.sidebar.append(self.fileblock.view)
        self.template.sidebar.append(pn.layout.Divider())
        self.tabs = pn.Tabs(
                        ('BigQuery',pn.Column(pn.Card(self.authblock.view, title='Authentication'),self.queryblock.view)),
                        ('Analytics services',self.funcblock.view),
                        ('Table view',self.tabulatorblock.view),
                        ('Chart view',self.chartblock.view),
                                        )
        self.tabs.dynamic =True
        self.template.main.append(self.tabs)
        self.server= self.template.show(open=open_in_browser)
        self.queryblock.btn_query.on_click(self.btn_query_click) 


    def btn_query_click(self,event):
        self.refresh_directory()
        self.funcblock.sel_func.value =''


    def btn_proj_click_2(self,event):
        self.queryblock.feed_client(self.authblock.client)
        
    def btn_download_click_2(self,event):
        self.refresh_directory()

    def filesselector_change_2(self,event):
        self.feed_data()
        
    def btn_compute_click_2(self,event):
        if type(self.funcblock.result) == pd.DataFrame:
            self.refresh_directory()
            self.feed_data()

    def feed_data(self):
        self.tabulatorblock.feed_data(self.fileblock.data)
        self.chartblock.feed_data(self.fileblock.data)


    def refresh_directory(self):
        self.fileblock.filesselector.options = os.listdir(self.fileblock.folderpath)

    def open_in_browser(self):
        webbrowser.open(f'http://localhost:{self.server.port}')

    @property
    def view(self):
        iframe = pn.pane.HTML(f'<iframe src="http://localhost:{self.server.port}" frameborder="0" scrolling="yes" height="800" width="100%""></iframe>')
        return iframe

