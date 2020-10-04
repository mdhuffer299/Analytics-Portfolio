#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 28 13:43:03 2020

@author: mhuffer
"""

import pandas as pd
import os
import airflow
import json
import gspread
import re

from oauth2client.service_account import ServiceAccountCredentials

from datetime import datetime
from airflow.models import DAG, Variable, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dag_factory_functions import *




## Look to store this as variable in Airflow
DAG_CONFIG_FILE_DIRECTORY = "/Users/mhuffer/Desktop/dag_config/"



for file in os.listdir(DAG_CONFIG_FILE_DIRECTORY):
    if file.endswith(".json"):
        
        full_file_name = DAG_CONFIG_FILE_DIRECTORY + file
        
        with open(full_file_name) as json_data:
            dag_def_dict = json.load(json_data)
            schedule = eval(dag_def_dict['schedule'])
            dag_id = dag_def_dict['dag_name']
            
            json_data.close()
            
        default_args = {
                'owner': 'airflow'
                , 'start_date': datetime.today()
        }
            
        globals()[dag_id] = create_dag(dag_id, schedule, default_args, dag_def_dict)