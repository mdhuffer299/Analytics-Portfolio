import pandas as pd
import numpy as np
import os
import airflow
import json
import gspread
import re
import requests
import base64
import sys
import math

from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


"""
    General functions
"""


def create_dag(dag_id, schedule, default_args, dag_def_dict):
    """
        Purpose:
            Master Function used to Dynamically generate DAGs based on configuration files.
            Task dependency order can be defined in passed configuration file

        Parameters:
            dag_id: Name of DAG as defined in DAG definition JSON File
            schedule: Schedule with which the DAG should be executed.
            defaualt_args: default arguments that need to be provided and defining DAG creation
            dag_def_dict:  Configuration File that provides all of task defintions, function calls, and task execution order information

        Return Value:
            DAG to be executed on Airflow platform based on execution schedule defined
    """
    
    dag = DAG(dag_id, default_args = default_args, schedule_interval = schedule)
    
    task_list = []
    task_name_dict = {}
    
    with dag:
        for idx, task in enumerate(dag_def_dict['tasks']):
            task_dict = dag_def_dict['tasks'][task]
            if task_dict['operator'] == "PythonOperator":
                task_name = PythonOperator(task_id = task_dict['task_id']
                                            , provide_context = True
                                            , python_callable = eval(task_dict['python_callable'])
                                            , op_kwargs = task_dict['op_kwargs'])
                
                task_list.append(task_name)
                task_name_dict[task] = task_name

            elif task_dict['operator'] == "BranchPythonOperator":
                task_name = BranchPythonOperator(task_id = task_dict['task_id']
                                                 , provide_context = True
                                                 , python_callable = eval(task_dict['python_callable'])
                                                 , op_kwargs = task_dict['op_kwargs'])
                
                task_list.append(task_name)
                task_name_dict[task] = task_name
                
            elif task_dict['operator'] == "DummyOperator":
                task_name = DummyOperator(task_id = task_dict['task_id'])
                
                task_list.append(task_name)
                task_name_dict[task] = task_name

            elif task_dict['operator'] == "TriggerDagRunOperator":
                task_name = TriggerDagRunOperator(task_id = task_dict['task_id']
                                                    , python_callable = trigger_dag
                                                    , trigger_dag_id = task_dict['trigger_dag_id'])

                task_list.append(task_name)
                task_name_dict[task] = task_name


            if idx not in [0]:
                if type(task_dict['upstream_dependencies']) == list:
                    output_dep_list = []
                    
                    dep_list = task_dict['upstream_dependencies']
                    
                    for idx, val in enumerate(dep_list):
                        tmp_upstream_dep = task_name_dict.get(val)
                        
                        output_dep_list.append(tmp_upstream_dep)
                        
                    current_task = task_name                        
                    output_dep_list >> current_task
                    
                else:
                    upstream_dependency = task_dict['upstream_dependencies']
                
                    upstream_task = task_name_dict.get(upstream_dependency)
                    current_task = task_name
                
                    upstream_task >> current_task           
    
    return(dag)



def param_dict_to_sql_list(param_dict, table_alias = None):
    """
        Purpose:
            Function reads in parameter dictionary that is provided from a sellf-service request to generate a list of sql WHERE clause statements
            to be used in later DAG workflows.

            Currently only handles: Integer and String Filter statements

        Parameters:
            param_dict: dictionary provided through task instance from config file DAG output

        Return Value:
            list of sql statements for use in later sql WHERE clause statements
    """

    output_list = []
    filter_list = []

    if table_alias is not None:
        table_alias = table_alias + '.'
    else:
        table_alias = ''

    for key, value in param_dict.items():
        param_list = []
        db_column_name = key

        filter_dict = value

        for sub_key, sub_val in filter_dict.items():
            param_list.append(sub_val)

        try:
            where_conditions = "'" + "', '".join(param_list) + "'"
        except:
            where_conditions = param_list[0]

        if type(where_conditions) == str:
            where_clause = 'AND ' + table_alias + db_column_name + ' IN (' + where_conditions + ')'
            filter_list.append(where_clause)
        elif type(where_conditions) == int:
            where_clause = 'AND ' + table_alias + db_column_name + ' >= ' + str(where_conditions)
            filter_list.append(where_clause)
        else:
            pass

        output_list = " ".join(filter_list)

    return(output_list)
    
def get_target_col(param_dict):
    """
        Purpose:
            Function reads in parameter dictionary that is provided from a self-service request to generate a DB column name for the target opportunity owner
            to be used in later DAG workflows.

        Parameters:
            param_dict: dictionary provided through task instance from config file DAG output

        Return Value:
            DB column name
    """

    for key, value in param_dict.items():
        sub_dict = value

        for sub_key, sub_val in sub_dict.items():
            target_col_nm = sub_val
            
    return(target_col_nm)




def create_wave_column_metadata_file(dataset_name, df):
    """
        Purpose:
            Generate the required metadata file for the Wave Dataset
            
        Parameters:
            dataset_name: name of the Wave Analytics Dataset
            df: df that you are using to generate Wave Metadata file
            
        Return Value:
            JSON string with required key-value pairs for metadata upload
    """
    output_metadata_dict = {}
            
    df_col_list = list(df.columns)
    object_list = []
    object_field_list = []
    
    output_metadata_dict['fileFormat'] = {
                                            'charsetName': 'UTF-8'
                                            , 'fieldsEnclosedBy': '\"'
                                            , 'fieldDelimetedBy': ','
                                            , 'numberOfLinesToIgnore': 1
                                         }
    
    for idx, val in enumerate(df_col_list):
        
        if df[val].dtypes in ['datetime64[ns]', 'timedelta64[ns]']:
            object_field_dict = {}
            
            object_field_dict['description'] = ""
            object_field_dict['fullyQualifiedName'] = dataset_name + "." + val
            object_field_dict['label'] = val
            object_field_dict['name'] = val
            object_field_dict['type'] = 'Date'
            object_field_dict['format'] = 'yyyy-MM-dd'
            
            object_field_list.append(object_field_dict)
            
        elif df[val].dtypes in ['object', 'bool']:
            object_field_dict = {}
            
            object_field_dict['description'] = ""
            object_field_dict['fullyQualifiedName'] = dataset_name + "." + val
            object_field_dict['label'] = val
            object_field_dict['name'] = val
            object_field_dict['type'] = 'Text'
            
            object_field_list.append(object_field_dict)
            
        elif df[val].dtypes in ['float64', 'int64']:
            object_field_dict = {}
            
            object_field_dict['description'] = ""
            object_field_dict['fullyQualifiedName'] = dataset_name + "." + val
            object_field_dict['label'] = val
            object_field_dict['name'] = val
            object_field_dict['type'] = 'Numeric'
            object_field_dict['precision'] = 18
            object_field_dict['defaultValue'] = 0
            object_field_dict['scale'] = 6
            
            object_field_list.append(object_field_dict)
            
        else:
            print(val + " does not have a type")
            
            
    object_dict = {
                    'connector': 'IntegrationForceCSVUploader'
                    , 'description': ''
                    , 'fullyQualifiedName': dataset_name
                    , 'label': dataset_name
                    , 'name': dataset_name
                    , 'fields': object_field_list
                   }
    
    object_list.append(object_dict)
            
    output_metadata_dict['objects'] = object_list
    
    json_metadata = json.dumps(output_metadata_dict, indent = 4)
    
    return(json_metadata)
    
    
    
def create_wave_json_metadata_file(dataset_name, operation, app_name, column_metadata, notification_email):
    """
        Purpose:
            Generate the required metadata file for the Wave Application Upload/Update
            
        Parameters:
            dataset_name: name of the Wave Analytics Dataset
            operation: Name of operation you wish to perform on the dataset.  Defaults to Overwrite from push_wave_dataset function
            app_name: Name of the Wave Analytics application 
            column_metadata: Metadata file of column metadata generated from create_wave_column_metadata_file
            notifcation_email: Email address of recepient of Wave Success/Failure email
            
        Return Value:
            JSON string with required key-value pairs for metadata upload
    """
    
    output = {
                'Format': 'Csv'
                , 'EdgemartAlias': dataset_name
                , 'EdgemartLabel': dataset_name
                , 'EdgemartContainer': app_name
                , 'FileName': dataset_name
                , 'MetadataJson': column_metadata
                , 'Operation': operation
                , 'Action': 'None'
                , 'NotificationSent': 'Always'
                , 'NotificationEmail': notification_email
              }
    
    json_output = json.dumps(output, indent = 4)
    
    return(json_output)



def trigger_dag(context, dag_run_obj):
    dag_run_obj.payload = {'message': 'Next Dag Run'}
    
    return(dag_run_obj)



def df_branch_func(dag_task_id, success_task_id, failure_task_id, **kwargs):
    """
        Purpose:

        Parameters:

        Return Value:
    """

    ti = kwargs['ti']

    df = ti.xcom_pull(task_ids = dag_task_id)

    if df.empty:
        return(failure_task_id)
    else:
        return(success_task_id)


"""
    DAG Functions - Data Ingestion
"""

def read_config_file(file_path = None, **kwargs):
    """
        Purpose:
            Read in JSON configuration file for automating DAG workflows

        Parameters:
            file_path: full file directory path of JSON configuration file

        Return Value:
            returns a dictionary of parameters defined by user
    """

    with open(file_path) as json_config_file:
        param_dict = json.load(json_config_file)
        
        return(param_dict)


def return_google_sheet_df(dag_task_id, last_run_time = None, days_to_pull = 1, client_secret_file_name = 'client_secret.json', **kwargs):
    """
        Purpose:
            Pass in configuration file dag task id, the last execution time of Google request, and location of credential file.
            Configuration file should contain the google_workbook_name, google_sheet_name, and the client_secret_file_location.

            Configuration file would also need to provide Google Form column name to DB column name mapping, for future processing.
            
        Parameters:
            dag_task_id: task_id of dag that reads in configuration file containing google sheet information.
            

        Return Value:
            pandas DF with most recent parameters requested
    """
    ti = kwargs['ti']
    dag_params = ti.xcom_pull(task_ids = dag_task_id)
    google_form_params = dag_params['google_form']
    column_name_mapping_params = dag_params['db_column_name_mapping']
    
    #### Need to figure out how to store this after each run and additional logic around if doesn't exist
    if last_run_time is not None:
        last_run_time = pd.to_datetime(last_run_time)
    else:
        last_run_time = f"{datetime.now() - timedelta(days = days_to_pull):%Y-%m-%d %H:%M:%S}"
    
    
    # Define the scope needed for the API account
    scope = ['https://www.googleapis.com/auth/drive']
    
    # return the service account credentials from the JSON file
    # JSON fiel must exist in same working directory or fully qualified path
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_form_params.get('client_secret_file_location') + client_secret_file_name, scope)
    
    # Create the authorized client connection
    client = gspread.authorize(creds)

    # Open the workbook that contains multiple sheets
    # Note: Service account must be added to the google sheet in order to access the data.
    # Service Account: monetization-strategy@monetization-strategy-poc.iam.gserviceaccount.com
    workbook = client.open(google_form_params.get('google_workbook_name'))
    
    # Collect specific sheet
    sheet = workbook.worksheet(google_form_params.get('google_sheet_name'))
    
    # Return dictionary of key value pairs
    records = sheet.get_all_records()
    
    # Create a pandas dataframe using the provided dictionary 
    df = pd.DataFrame(data = records)


    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df[df['Timestamp'] >= last_run_time].reset_index(drop = True)
    df['Timestamp'] = df['Timestamp'].astype(str)

    ## Create column name mapping
    ## The value in key-value pair should be DB column name

    
    df.rename(columns = column_name_mapping_params, inplace = True)
    
    return(df)


def get_requester_email(dag_task_id, requester_col_name = 'requester', **kwargs):
    """
        Purpose:
            Retrieve the requester email from Google form to push notifcations to later in DAG workflow process

        Parameters:
            dag_task_id: task_id of dag that returns pandas DF from google form.

        Return Value:
            requester email address
    """

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = dag_task_id)
    
    requester_email = df.loc[0, requester_col_name]
    
    return(requester_email)



"""
    DAG Functions - Data Transformation
"""
    
    
def clean_google_params(dag_task_id, **kwargs):
    """
        Purpose:
            Read in google form data nd clean up via lookups to create config statement to generate SQL WHERE clause filters.
            If requester selects "Exclude Filter", no SQL statement will be provided in query

        Parameters:
            dag_task_id: task_id of dag that returns pandas DF from google form.

        Return Value:
            dictionary with clean key-value pairs of the required SQL filter criteria to be used ini creation of SQL statements
    """
    
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = dag_task_id[0]) 
    dag_params = ti.xcom_pull(task_ids = dag_task_id[1]) 
    
    lookup_value_dict = dag_params.get('lookup_value_dict')

    param_output_dict = {}

    for col_name, col_data in df.iteritems():
        if col_data.values != 'Exclude Filter':
            array_data = col_data.values
            
            for idx, val in enumerate(array_data):
                tmp_dict = {}

                if isinstance(type(val), (int, np.integer)) or np.issubdtype(type(val), (np.integer)) == True:
                   tmp_list = []
                   tmp_list.append(val.tolist())
                else:
                   tmp_list = val.split(', ')
                    
                
                for sub_idx, sub_val in enumerate(tmp_list):
                    if sub_val in lookup_value_dict.keys():
                        output_val = lookup_value_dict.get(sub_val)
                        
                        try:
                            tmp_dict[sub_idx] = int(re.sub('[^A-Za-z0-9 :/_.@-]+', '', output_val))
                        except:
                            tmp_dict[sub_idx] = re.sub('[^A-Za-z0-9 :/_.@-]+', '', output_val)

                    elif np.issubdtype(type(sub_val), (np.integer)):
                        tmp_dict[sub_idx] = sub_val

                    else:
                        try:
                            tmp_dict[sub_idx] = int(re.sub('[^A-Za-z0-9 :/_.@-]+', '', sub_val))
                        except:
                            tmp_dict[sub_idx] = re.sub('[^A-Za-z0-9 :/_.@-]+', '', sub_val)
            param_output_dict[col_name] = tmp_dict
        else:
            pass
    return(param_output_dict)



def clean_google_params_batch(dag_task_id, **kwargs):
    """
        Purpose:
            Read in google form data nd clean up via lookups to create config statement to generate SQL WHERE clause filters.
            If requester selects "Exclude Filter", no SQL statement will be provided in query

        Parameters:
            dag_task_id: task_id of dag that returns pandas DF from google form.

        Return Value:
            dictionary with clean key-value pairs of the required SQL filter criteria to be used ini creation of SQL statements
    """
    
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = dag_task_id[0]) 
    dag_params = ti.xcom_pull(task_ids = dag_task_id[1])
    
    lookup_value_dict = dag_params.get('lookup_value_dict')

    param_output_dict = {}

    for idx, value in df.iterrows():
        tmp_output_dict = {}

        sub_df = df.iloc[[idx]]

        record_name = sub_df.loc[idx, 'requester'] + " " + str(sub_df.loc[idx, 'timestamp'])

        for col_name, col_data in sub_df.iteritems():
            if col_data.values != 'Exclude Filter':
                array_data = col_data.values
                
                for idx, val in enumerate(array_data):
                    tmp_dict = {}

                    if isinstance(type(val), (int, np.integer)) or np.issubdtype(type(val), (np.integer)) == True:
                        tmp_list = []
                        tmp_list.append(val.tolist())
                    else:
                        tmp_list = val.split(', ')
                    
                    for sub_idx, sub_val in enumerate(tmp_list):
                        if sub_val in lookup_value_dict.keys():
                            output_val = lookup_value_dict.get(sub_val)
                            
                            try:
                                tmp_dict[sub_idx] = int(re.sub('[^A-Za-z0-9 :/_.@]+', '', output_val))
                            except:
                                tmp_dict[sub_idx] = re.sub('[^A-Za-z0-9 :/_.@]+', '', output_val)

                        elif np.issubdtype(type(sub_val), (np.integer)):
                            tmp_dict[sub_idx] = sub_val

                        else:
                            try:
                                tmp_dict[sub_idx] = int(re.sub('[^A-Za-z0-9 :/_.@]+', '', sub_val))
                            except:
                                tmp_dict[sub_idx] = re.sub('[^A-Za-z0-9 :/_.@]+', '', sub_val)
                tmp_output_dict[col_name] = tmp_dict
            else:
                pass

        param_output_dict[record_name] = tmp_output_dict
    
    return(param_output_dict)





def create_sql_statements(dag_task_id, input_list_check = None, product_sql_statements = 0, target_col_sql_statements = 0, table_alias = None, **kwargs):
    """
        Purpose:
            Read in dictionary with db_column names as key and the required filter value(s) as the value in key value pair

        Parameters:
            dag_task_id: task_id from previous DAG task that outputs the cleaned parameter dictionary
            input_check_list: List of DB column names that should be returned in the output SQL filter.  
                              Only returns a SQL statement if the filter was not excluded by requester.
            product_sql_statments: Boolean to indicate if they are product specific SQL statements, due to parsing of KEY value pair and function that needs to be executed.
            target_ae_sql_statements: Boolean to indicate if they are target AE specific SQL statements, due to parsing of KEY value pair and function that needs to be executed.

        Return Value:
            List of SQL statements specific to Account, Product, Target AE, etc...
    """
    
    ti = kwargs['ti']
    clean_param_dict = ti.xcom_pull(task_ids = dag_task_id)
    
    output_sql_statements = []
    tmp_output_dict = {}
    
    db_column_names = list(clean_param_dict.keys())
    
    if input_list_check is not None and product_sql_statements == 0:
        for db_col in db_column_names:
            if db_col in input_list_check:
                output_filter = clean_param_dict.get(db_col)
                tmp_output_dict[db_col] = output_filter
                
            else:
                pass

        if target_col_sql_statements == 0:
            output_sql_statements.append(param_dict_to_sql_list(tmp_output_dict, table_alias))
        else:
            output_sql_statements.append(get_target_col(tmp_output_dict))
                
    elif input_list_check is not None and product_sql_statements == 1:
        for db_col in db_column_names:
            if db_col in input_list_check:
                sub_prod_dict = {}
            
                prod_val = db_col[:6]
                col_val = db_col[7:]
            
                output_filter = clean_param_dict.get(db_col)
            
                sub_prod_dict[col_val] = output_filter
                output_sql_statements.append(param_dict_to_sql_list(sub_prod_dict, table_alias))
                
                tmp_output_dict[prod_val] = output_sql_statements
                
            else:
                pass
    else:
        pass
        
    return(output_sql_statements)





def create_sql_statements_batch(dag_task_id, input_list_check = None, product_sql_statements = 0, target_col_sql_statements = 0, table_alias = None, **kwargs):
    """
        Purpose:
            Read in dictionary with db_column names as key and the required filter value(s) as the value in key value pair

        Parameters:
            dag_task_id: task_id from previous DAG task that outputs the cleaned parameter dictionary
            input_check_list: List of DB column names that should be returned in the output SQL filter.  
                              Only returns a SQL statement if the filter was not excluded by requester.
            product_sql_statments: Boolean to indicate if they are product specific SQL statements, due to parsing of KEY value pair and function that needs to be executed.
            target_ae_sql_statements: Boolean to indicate if they are target AE specific SQL statements, due to parsing of KEY value pair and function that needs to be executed.

        Return Value:
            List of SQL statements specific to Account, Product, Target AE, etc...
    """
    
    ti = kwargs['ti']
    clean_param_dict = ti.xcom_pull(task_ids = dag_task_id)
    
    output_dict = {}

    sub_clean_param_dict = list(clean_param_dict.keys())

    for idx, val in enumerate(sub_clean_param_dict):
        output_sql_statements = []
        tmp_output_dict = {}

        dict_name = val

        sub_param_dict = clean_param_dict.get(val)
    
        db_column_names = list(sub_param_dict.keys())
        
        if input_list_check is not None and product_sql_statements == 0:
            for db_col in db_column_names:
                if db_col in input_list_check:
                    output_filter = sub_param_dict.get(db_col)
                    tmp_output_dict[db_col] = output_filter
                    
                else:
                    pass

            if target_col_sql_statements == 0:
                output_sql_statements.append(param_dict_to_sql_list(tmp_output_dict, table_alias))
            else:
                output_sql_statements.append(get_target_col(tmp_output_dict))
                    
        elif input_list_check is not None and product_sql_statements == 1:
            for db_col in db_column_names:
                if db_col in input_list_check:
                    sub_prod_dict = {}
                
                    prod_val = db_col[:6]
                    col_val = db_col[7:]
                
                    output_filter = sub_param_dict.get(db_col)
                
                    sub_prod_dict[col_val] = output_filter
                    output_sql_statements.append(param_dict_to_sql_list(sub_prod_dict, table_alias))
                    
                    tmp_output_dict[prod_val] = output_sql_statements
                    
                else:
                    pass
        else:
            pass

        output_dict[dict_name] = output_sql_statements
        
    return(output_dict)


def create_sql_filter_config(dag_task_id, **kwargs):
    """
        Purpose:
            Read in all SQL Statement lists and process into SQL filter configuartion file

        Parameters:
            dag_task_id: task_id or list of task_ids from previously executed GENERATE_SQL_STATEMENT DAGs

        Return Value:
            Dictionary with SQL statements where 
                Key = DAG task_id that matches with SQL Statements format {} value.
                Value = SQL Statement String
    """
    
    ti = kwargs['ti']
    
    dag_task_id_list = dag_task_id
    
    config_file_output_dict = {}
    
    for idx, dag_id in enumerate(dag_task_id_list):
        sql_statement_list = ti.xcom_pull(task_ids = dag_task_id[idx])
        sql_statement = sql_statement_list[0]
        
        config_file_output_dict[dag_id] = sql_statement
        
    return(config_file_output_dict)




def create_sql_filter_config_batch(dag_task_id, **kwargs):
    """
        Purpose:
            Read in all SQL Statement lists and process into SQL filter configuartion file

        Parameters:
            dag_task_id: task_id or list of task_ids from previously executed GENERATE_SQL_STATEMENT DAGs

        Return Value:
            Dictionary with SQL statements where 
                Key = DAG task_id that matches with SQL Statements format {} value.
                Value = SQL Statement String
    """
    
    ti = kwargs['ti']
    
    dag_task_id_list = dag_task_id
    
    config_file_output_dict = {}
    
    for idx, dag_id in enumerate(dag_task_id_list):
        sql_statement_dict = ti.xcom_pull(task_ids = dag_task_id[idx])

        for key, value in sql_statement_dict.items():
            tmp_dict = {}

            parent_dict_key = key
            sub_dict_key = dag_id
            sql_statement = value[0]

            tmp_dict[sub_dict_key] = sql_statement

            if key in config_file_output_dict.keys():
                config_sub_dict = config_file_output_dict.get(key)
                config_sub_dict[sub_dict_key] = sql_statement

            else:
                config_file_output_dict[parent_dict_key] = tmp_dict
        
    return(config_file_output_dict)



"""
    DAG Functions - Data Execution
"""

def execute_sql(dag_task_id, sql_file_info_key, db_connection, filter_list = None, return_pandas_df = 0, **kwargs):
    """
        Purpose:
            Used to execute SQL files with or without SQL filter statements provided.
            Will look to parse through SQL file spliting on ";" in file.  Can provide one file with multiple tables or a single file for each statement.
            Can return a dataframe for additional processing, output, or storage.

        Parameters:
            dag_task_id: task_id or list of task_ids that provides configuration where SQL file locations and names are stored.
                         second task_id in list if provided should be the SQL filter statement dictionary
            sql_file_info_key: Key name of dictionary in JSON configuration file where SQL Files location and names can be found
            db_connection: Name of Database, you are trying to connect to.  Configured in the Connection Manager of Airflow
            filter_list: If list of SQL filter statments is provided, you must provide the key names from key-value pairs in SQL filter configuration file
            return_pandas_df: Boolean expression if desired to return a pandas DF from SQL statement

        Return Value:
            Execution of SQL statement will either return a DF or will just exit successfully.
    """
    
    ti = kwargs['ti']
    
    # Retrieve the dictionaries from previous tasks
    dag_params = ti.xcom_pull(task_ids = dag_task_id[0])

    try:
        sql_filter_statements = ti.xcom_pull(task_ids = dag_task_id[1])
    except:
        sql_filter_statements = None
    
    # Create the database connection
    try:
        db_connection_hook = OracleHook(oracle_conn_id = db_connection)
    except Exception as msg:
        print(msg)
    
    # Gather the SQL file path and file name to generate the string to open SQL file
    sql_file_info = dag_params.get('sql_file_info')
    sql_file_location = sql_file_info.get('sql_file_location')
    sql_file_name = sql_file_info.get(sql_file_info_key)
    sql_file_path = str(sql_file_location) + str(sql_file_name)
    
    # Create a dictionary that contains the SQL statements specified in CONFIG file to be passed to SQL queries
    sql_param_dict = {}
    
    if filter_list is not None:
        for filter_name in filter_list:
            sql_filter = sql_filter_statements.get(filter_name)
        
            sql_param_dict[filter_name] = sql_filter
    else:
        pass
        
        
    # Open the SQL files and parse contents, splitting statements on ";" character
    sql_file = open(sql_file_path, 'r')
    sql_query_strings = sql_file.read().split(';')
    sql_file.close()
    
    # Loop through query string and try to execute the sql statements with parameters passed via dictionary if exists
    # SQL query will return DF if specified
    for sql_query in sql_query_strings:
        if "DROP TABLE" in sql_query:
            try:
                db_connection_hook.run(sql_query, autocommit = True)
            except Exception as msg:
                print(msg)
        else:
            sql_string = sql_query.format_map(sql_param_dict)
            
            if return_pandas_df == 1:
                try:
                    db_cursor = db_connection_hook.get_cursor()
                    df = pd.read_sql(sql_string, db_cursor.connection)
                except Exception as msg:
                    print(msg)
            
            else:
                try:
                    db_connection_hook.run(sql_string, autocommit = True)
                    df = None
                except Exception as msg:
                    print(msg)
           
    return(df)



def execute_sql_batch(dag_task_id, sql_file_info_key, db_connection, filter_list = None, return_pandas_df = 0, **kwargs):
    """
        Purpose:
            Used to execute SQL files with or without SQL filter statements provided.
            Will look to parse through SQL file spliting on ";" in file.  Can provide one file with multiple tables or a single file for each statement.
            Can return a dataframe for additional processing, output, or storage.

        Parameters:
            dag_task_id: task_id or list of task_ids that provides configuration where SQL file locations and names are stored.
                         second task_id in list if provided should be the SQL filter statement dictionary
            sql_file_info_key: Key name of dictionary in JSON configuration file where SQL Files location and names can be found
            db_connection: Name of Database, you are trying to connect to.  Configured in the Connection Manager of Airflow
            filter_list: If list of SQL filter statments is provided, you must provide the key names from key-value pairs in SQL filter configuration file
            return_pandas_df: Boolean expression if desired to return a pandas DF from SQL statement

        Return Value:
            Execution of SQL statement will either return a DF or will just exit successfully.
    """
    
    ti = kwargs['ti']
    
    # Retrieve the dictionaries from previous tasks
    dag_params = ti.xcom_pull(task_ids = dag_task_id[0])

    try:
        sql_filter_statements = ti.xcom_pull(task_ids = dag_task_id[1])
    except:
        sql_filter_statements = None
    
    # Create the database connection
    try:
        db_connection_hook = OracleHook(oracle_conn_id = db_connection)
    except Exception as msg:
        print(msg)
    
    # Gather the SQL file path and file name to generate the string to open SQL file
    sql_file_info = dag_params.get('sql_file_info')
    sql_file_location = sql_file_info.get('sql_file_location')
    sql_file_name = sql_file_info.get(sql_file_info_key)
    sql_file_path = str(sql_file_location) + str(sql_file_name)


    # Open the SQL files and parse contents, splitting statements on ";" character
    sql_file = open(sql_file_path, 'r')
    sql_query_strings = sql_file.read().split(';')
    sql_file.close()

    if sql_query_strings[-1] == '':
        sql_query_strings = sql_query_strings[:-1]
    else:
        pass

    
    # Create a dictionary that contains the SQL statements specified in CONFIG file to be passed to SQL queries    
    if filter_list is not None:
        for key, value in sql_filter_statements.items():
            sql_param_dict = {}
            sub_sql_dict = value

            for filter_name in filter_list:

                sql_filter = sub_sql_dict.get(filter_name)
        
                sql_param_dict[filter_name] = sql_filter

                sql_param_dict['requester_email'] = "\'" + key.split(' ', 1)[0] + "\'"
                sql_param_dict['requester_timestamp'] = "\'" + key.split(' ', 1)[1] + "\'"

                sql_param_dict['request_number'] = "\'" + key + "\'"


            ## Execute SQL Statements in a loop based on a new parameters
            for sql_query in sql_query_strings:
                if "DROP TABLE" in sql_query:
                    try:
                        db_connection_hook.run(sql_query, autocommit = True)
                    except Exception as msg:
                        print(msg)
                else:
                    sql_string = sql_query.format_map(sql_param_dict)
                    
                    if return_pandas_df == 1:
                        try:
                            db_cursor = db_connection_hook.get_cursor()
                            df = pd.read_sql(sql_string, db_cursor.connection)
                        except Exception as msg:
                            print(msg)
                    
                    else:
                        try:
                            db_connection_hook.run(sql_string, autocommit = True)
                            df = None
                        except Exception as msg:
                            print(msg)
    else:
        for sql_query in sql_query_strings:
            if "DROP TABLE" in sql_query:
                try:
                    db_connection_hook.run(sql_query, autocommit = True)
                except Exception as msg:
                    print(msg)
            else:
                if return_pandas_df == 1:
                    try:
                        db_cursor = db_connection_hook.get_cursor()
                        df = pd.read_sql(sql_query, db_cursor.connection)
                    except Exception as msg:
                        print(msg)
                else:
                    try:
                        db_connection_hook.run(sql_query, autocommit = True)
                        df = None
                    except Exception as msg:
                        print(msg)

    return(df)


"""
    DAG Functions - Data Output
"""


def write_local_config_file(dag_task_id, output_file_name, **kwargs):
    """
        Purpose:
            Write the SQL Filter Statment configuration file for future processing.

        Parameters:
            dag_task_id: List of task_ids from configuration file with output location directory and the formatted dictionary of sql statements
            output_file_name: desired output file name

        Return Value:
            None - writes file to specific file directory
    """
    
    ti = kwargs['ti']
    param_output_dict = ti.xcom_pull(task_ids = dag_task_id[0])
    
    dag_params = ti.xcom_pull(task_ids = dag_task_id[1]) 
    
    output_file_dict = dag_params.get('filter_params_file_location')
    
    with open(output_file_dict.get('param_file') + output_file_name + '.json', 'w') as outfile:
        json.dump(param_output_dict, outfile, indent = 4)


def write_local_copy(dag_task_id, output_file_location, output_file_name, output_file_type, **kwargs):
    """
        Purpose:
            Return flat file of DF for review or future processing

        Parameters:
            dag_task_id: task_id of execute_sql task that returned DF you wish to write to file system.
            output_file_location: Location that file should be written to
            output_file_name: Name of file
            output_file_type: Currently only handles .csv files

        Return Value:
    """
    
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = dag_task_id)

    output_file = output_file_location + output_file_name + output_file_type

    df.to_csv(output_file, index = False)


def push_wave_dataset(dag_task_id, dataset_name, app_name, wave_environment = 'production', operation = 'Overwrite', notification_email = 'email address', **kwargs):
    """
        Purpose:
            Generate the required metadata file for the Wave Dataset, connect to services, and push data to respective Wave Dataset
            
        Parameters:
            df: df that you are using to generate Wave dataset
            dataset_name: name of the Wave Analytics Dataset
            app_name: Name of the Wave Analytics application 
            wave_environment: Name of connection to wave instances as defined in Config file
            operation: Name of operation you wish to perform on the dataset.  Defaults to Overwrite
            notifcation_email: Email address of recepient of Wave Success/Failure email
            
        Return Value:
            True when operation completes
    """
    
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = dag_task_id)
    
    ## Create the connection parameters for API connection from config file
    sf = sfdcutils.SfdcConnection()
    salesforce = sf.connect_sfdc(wave_environment)
    native = sf.native_salesforce_login(wave_environment)
    headers = Misc.getSfHeader(native['session_id'])
    
    
    ## Prepare Data Metadata File and Configure the Upload Payload
    output_json_column_metadata_file = create_wave_column_metadata_file(dataset_name, df)
    wave_column_metadata_contents = base64.b64encode(output_json_column_metadata_file.encode('UTF-8')).decode('ascii')
    
    ## Operation Names:
    ## Append: Append all data to the dataset.  Creates a dataset if it doesn't exist
    ## Delete: Delete the rows from the dataset.  The rows to delete must contain one (and only one) field with a unique identifier
    ## Overwrite: Create a dataset with the given data, and replace the dataset if it exists
    ## Upsert: Insert or Update rows in the dataset, Creates a dataset if it doesn't exist
    wave_json_payload = create_wave_json_metadata_file(dataset_name, operation, app_name, wave_column_metadata_contents, notification_email)
    
    
    ## Connect to Salesforce InsightsExternalData and InsightsExternalDataPart Instances
    wave_url = 'URL'.format(sf = native[''])
    wave_data_push_url = 'UURL'.format(sf = native[''])
    
    
    ## Push the metadata files to Wave Dashboard and return the InsightsExternalData API ID
    wave_response = requests.request("POST", wave_url, data = wave_json_payload, headers = headers)
    

    id = wave_response.json()['id']
    
    
    
    ## Take the Data frame and split the file into multiple parts if large file.  Split into 10 MB files for push
    if sys.getsizeof(df.to_csv(index = False)) > 10000000:
        num_of_splits = int(math.ceil(float(sys.getsizeof(df.to_csv(index = False, header = False))) / float(10000000)))
        split_data = np.array_split(df, num_of_splits)
        
        for idx in range(len(split_data)):
            if idx == 0:
                
                encoded_file_contents = base64.b64encode(split_data[i].to_csv(index = False).encode('UTF-8')).decode('ascii')
                
            else:
                encoded_file_contents = base64.b64encode(split_data[i].to_csv(index = False, header = False).encode('UTF-8')).decode('ascii')
                
            ## If File is large, multiple data files will need to be pushed to the Wave App
            wave_df_payload = {
                                "InsightsExternalDataId": id
                                , "PartNumber": str(idx + 1)
                                , "DataFile": encoded_file_contents
                              }
        
            wave_df_json_payload = json.dumps(wave_df_payload, indent = 4)
        
            wave_data_push_response = requests.request("POST", wave_data_push_url, data = wave_df_json_payload, headers = headers)
    else:
        encoded_file_contents = base64.b64encode(df.to_csv(index = False).encode('UTF-8')).decode('ascii')
        
        
        # If the file is small, then there is only one push for the file
        wave_df_payload = {
                            "InsightsExternalDataId": id
                            , "PartNumber": 1
                            , "DataFile": encoded_file_contents
                          }
        
        wave_df_json_payload = json.dumps(wave_df_payload, indent = 4)
        
        wave_data_push_response = requests.request("POST", wave_data_push_url, data = wave_df_json_payload, headers = headers)
        
        
        
        
    ## Final Step: Prepare payload and Upload the dataset to Wave Dashboard
    wave_upload_payload = {
                            "Action": "Process"
                          }
    
    wave_upload_json_payload = json.dumps(wave_upload_payload, indent = 4)
    
    
    wave_upload_url = 'URL'.format(sf = native[''], id = id)
    wave_upload_response = requests.request("PATCH", wave_upload_url, data = wave_upload_json_payload, headers = headers)
    
    return(True)