#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 08:32:03 2019

@author: mhuffer
"""

import re
import shap
from sklearn.metrics import roc_curve, roc_auc_score, confusion_matrix, classification_report
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import KFold
from statistics import mean
from itertools import zip_longest
from imblearn.over_sampling import SMOTE, BorderlineSMOTE, SVMSMOTE, KMeansSMOTE

def replace_values(df, char_value = 'None', num_value = 0, default_option = True):
    """
        Create a dictionary with column names and respective value to fill in for the dataframe passed.
        Dictionary is passed in df.fillna(value = dtype_dict_value) to replace those values in original df
    """
    
    dtype_list = ['object', 'float64', 'int64']
    dtype_dict_value = {}

    if default_option == True:

        for item in dtype_list:
            col_list = list(df.select_dtypes(include = [item]).columns)
            
            if item == 'object':
                col_dict = {col: char_value for col in col_list}
                dtype_dict_value.update(col_dict)
                
            elif item == 'float64':
                col_dict = {col: num_value for col in col_list}
                dtype_dict_value.update(col_dict)
                
            elif item == 'int64':
                col_dict = {col: num_value for col in col_list}
                dtype_dict_value.update(col_dict)
                
            else:
                col_dict = {}
                dtype_dict_value.update(col_dict)

    else:
        for item in dtype_list:
            col_list = list(df.select_dtypes(include = [item]).columns)
            
            if item == 'object':
                col_dict = {col: char_value for col in col_list}
                dtype_dict_value.update(col_dict)
                
            elif item == 'float64':
                col_dict = {col: df[col].mean() for col in col_list}
                dtype_dict_value.update(col_dict)
                
            elif item == 'int64':
                col_dict = {col: df[col].mean() for col in col_list}
                dtype_dict_value.update(col_dict)
                
            else:
                col_dict = {}
                dtype_dict_value.update(col_dict)

    return(dtype_dict_value)
    

def convert_cat_to_cat_lvl(df, encode_method = 'Numeric', col_list = None):
    """
        Create a list of string columns and convert them to categorical levels for use in model training.
        Creates dictionary that has mapping of column names to category levels for conversion back to actual values if needed.
                
        Returns dataframe with original column names dropped and dictionary with category to level mapping.
    """
    
    if col_list == None:
        string_col_list = list(df.select_dtypes(include = ['object']).columns)
    else:
        string_col_list = col_list
    
    forward_mapping_dict = {}
    inv_mapping_dict = {}
    encode_col_dict = {}
    
    if encode_method == 'Numeric':
        for col in string_col_list:
            df[col] = df[col].astype('category')
            
            inv_d = dict(enumerate(df[col].cat.categories))
            forward_d = dict((val, key) for key, val in inv_d.items())
            
            df[col + "_CAT"] = df[col].cat.codes
            
            forward_mapping_dict.update({col: forward_d})
            inv_mapping_dict.update({col + "_CAT": inv_d})
            
        df = df.drop(string_col_list, axis = 1)
        
        encoder = None
        
    elif encode_method == 'OneHot':
        encoder = OneHotEncoder(handle_unknown = 'ignore')
        
        df_encode = pd.DataFrame(encoder.fit_transform(df[string_col_list]).toarray(), columns = encoder.get_feature_names(string_col_list))
        
        encode_col_list = list(encoder.get_feature_names(string_col_list))
        
        df = df.merge(df_encode, left_index = True, right_index = True)
        df = df.drop(string_col_list, axis = 1)
        
        regex_non_char = re.compile('[^,0-9A-Z]')

        for col in encode_col_list:
            orig_col = col
            col = col.upper()
            col = regex_non_char.sub('_', col)
            col = col.replace(',', '').replace('____', '_').replace('___', '_').replace('__', '_')
            
            encode_col_dict[orig_col] = col
            
        df = df.rename(columns = encode_col_dict)
    
    return(df, forward_mapping_dict, inv_mapping_dict, encoder)


def low_mean_cols(df, cutoff_value):
    """
        Creates a list of columns that have a mean value lower than the threshold to remove from original df.
        Lower mean on those columns may indicate skewed data that influences the overall model performance.
    """
    
    desc_df = df.describe(include = 'all')
    col_names = desc_df.columns
    
    low_mean_idx = np.where(desc_df.loc['mean', :] < cutoff_value)
    low_mean_idx_list = low_mean_idx[0]
    low_mean_cols = df.iloc[0:6, low_mean_idx_list].columns
    
    return(low_mean_cols)
    

def potential_outliers(df):
    """
        Create a list of indicies that are flagged as potential outliers based on the 1.5 IQR rule
    """
    
    desc_df = df.describe(include = 'all')
    col_names = desc_df.columns
    
    col_q1 = desc_df.loc['25%', :]
    col_q3 = desc_df.loc['75%', :]
    iqr = 1.5*(col_q3 - col_q1)
    
    col_top = col_q3 + iqr
    col_bottom = col_q1 - iqr
    
    col_top_outliers_idx = {}
    col_bottom_outliers_idx = {}
    
    idx_list = []
    
    for col in col_names:
        top_idx = []
        bottom_idx = []
        
        top_idx = np.where(desc_df.loc[:, col] > col_top[col])
        bottom_idx = np.where(desc_df.loc[:, col] < col_bottom[col])
        
        idx_list.append(top_idx[0])
        idx_list.append(bottom_idx[0])
        
    final_idx_list = list(set(value for idx in idx_list for value in idx))
    
    return(final_idx_list)
    
    
def standardize_cols(df, label_col, col_list = None):
    """
        Takes or creates a list of columns to include in the standardization process.  
        If apply_to_all_cols = True, then all columns in dataframe will be standardized using StandardScaler function.
        Data must be converted to numeric data types prior to standardization if all columns are intended to be standardized.
    """
    
    label = pd.DataFrame(df[label_col])
    df_std = df.copy()
    df_std = df_std.drop(label, axis = 1)
    
    if col_list == None:
        column_headers = list(df_std.columns)
        scaler = StandardScaler().fit(df_std[column_headers])

        df_std = pd.DataFrame(scaler.transform(df_std[column_headers]), columns = column_headers)  
        df_std = label.merge(df_std, left_index = True, right_index = True)
    else:
        column_headers = col_list
        scaler = StandardScaler().fit(df_std[column_headers])
        
        df_std_int = pd.DataFrame(scaler.transform(df_std[column_headers]), columns = column_headers)  
        df_std.drop(df_std[df_std_int.columns], axis = 1, inplace = True)
        df_std = df_std_int.merge(df_std, left_index = True, right_index = True)
        
        df_std = label.merge(df_std, left_index = True, right_index = True)
    
    return(df_std, scaler)
    

def run_k_fold(df, label_col, model, num_of_folds = 5, shuffle_folds = False):
    """
        Run a k folds validation on a passed model.
        Returns the recall and precision values for the number of folds.
    """
    
    score_list = []
    tn_list = []
    fp_list = []
    fn_list = []
    tp_list = []
    
    label = df[label_col]
    features = df.drop(label_col, axis = 1)
    
    kf = KFold(n_splits = num_of_folds, random_state = 5, shuffle = shuffle_folds)
    for train_index, test_index in kf.split(features):
        train_features, test_features = features.iloc[train_index], features.iloc[test_index]
        train_label, test_label = label.iloc[train_index], label.iloc[test_index]
        
        model_fit = model.fit(train_features, train_label)
        score = model_fit.score(test_features, test_label)
        predictions = model_fit.predict(test_features)
        tn, fp, fn, tp = confusion_matrix(test_label, predictions).ravel()
        
        tn_list.append(tn)
        fp_list.append(fp)
        fn_list.append(fn)
        tp_list.append(tp)
        score_list.append(score)
        
    score_avg = mean(score_list)
    tn_avg = mean(tn_list)
    fp_avg = mean(fp_list)
    fn_avg = mean(fn_list)
    tp_avg = mean(tp_list)
    
    rec = tp_avg/(tp_avg + fn_avg)
    prec = tp_avg/(tp_avg + fp_avg)

    f_score = 2*((rec * prec)/(rec + prec))
    
    return(rec, prec, f_score)
    
def generate_model_summary(df, model, train_features, train_label, test_features, test_label, is_tree_model = False):
    """
        Generate all model summary values for analysis of model effectiveness.
        Model definition is passed into function for evaluation.
    """
    
    model_fit = model.fit(train_features, train_label)
    model_predictions = model_fit.predict(test_features)
    model_score = model_fit.score(test_features, test_label)
    
    tn, fp, fn, tp = confusion_matrix(test_label, model_predictions).ravel()
    confusion_list = [tn, fp, fn, tp]
    
    model_metrics = classification_report(test_label, model_predictions)
    
    if is_tree_model == False:
        desc_func = model_fit.decision_function(test_features)
        
        fpr, tpr, threshold = roc_curve(test_label, desc_func)
        roc_score = roc_auc_score(test_label, desc_func)
        
    else:
        tree_proba = model_fit.predict_proba(test_features)[:, 1]
        
        fpr, tpr, threshold = roc_curve(test_label, tree_proba)
        roc_score = roc_auc_score(test_label, tree_proba)
        
    model_summary = (model_score, confusion_list, model_metrics, fpr, tpr, threshold, roc_score)
        
    return(model_fit, model_summary)

def generate_summary_output(record, import_col_list, friendly_column_dict):
    """
        Create column that holds each records influential values based on the top 20 influential features.
    """
    
    value_list = []
    
    for item in import_col_list:
        if record[item] != None and type(record[item]) is str:
            if record[item] != 'None':
                friendly_col = friendly_column_dict.get(item) 
                value = str(friendly_col) + ": " + str(record[item])
                value_list.append(value)
                
        elif record[item] > 0 and type(record[item]) is np.int64:
            friendly_col = friendly_column_dict.get(item)
            value = str(friendly_col) + ": " + str(record[item])
            value_list.append(value)
            
        elif record[item] and type(record[item]) is np.float64:
            friendly_col = friendly_column_dict.get(item)
            value = str(friendly_col) + ": " + str(record[item])
            value_list.append(value)
            
    output_list = list(filter(None, value_list))
    output_list = '\n'.join(value_list)
            
    return(output_list)
    
    
def corr_vars(df, corr_threshold = .9):
    """
        Return dataframe with top correlated column names and their correlation value.
        Returns list of column names that can be removed from DataFrame.
    """
    corr = df.corr().abs()

    upper_corr = pd.DataFrame(np.triu(corr), index = corr.index, columns = corr.columns)

    unstack_corr = upper_corr.unstack()
    sort_corr_df = unstack_corr.sort_values(kind="quicksort", ascending = False).reset_index()
    sort_corr_df.columns = ['column_1', 'column_2', 'corr_value']

    sort_corr_df.drop(sort_corr_df.loc[sort_corr_df['column_1'] == sort_corr_df['column_2']].index, inplace = True)

    top_corr_df = sort_corr_df.loc[sort_corr_df['corr_value'] >= corr_threshold]

    unique_corr_cols = top_corr_df['column_1'].unique()

    return(top_corr_df, unique_corr_cols)
    
    
def generate_smote_sample(smote_method = 'smote', random_state = 5, n_jobs = 1):
    """
        Method to generate different SMOTE objects to be fit on sample of data.
    """
    if smote_method == 'smote':
        over_samp = SMOTE(random_state = random_state, n_jobs = n_jobs)
    elif smote_method == 'borderline':
        over_samp = BorderlineSMOTE(random_state = random_state, n_jobs = n_jobs)
    elif smote_method == 'svm':
        over_samp = SVMSMOTE(random_state = random_state, n_jobs = n_jobs)
    elif smote_method == 'kmeans':
        over_samp = KMeansSMOTE(random_state = random_state, n_jobs = n_jobs)
        
    return(over_samp)
    

def shap_summary(model, feature_df, friendly_column_dict, standardized_df = False, scaler_fit = None):
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(feature_df)
    
    shap_values_df = pd.DataFrame(shap_values, columns = feature_df.columns)
    
    if standardized_df == False:
        inverse_features = feature_df
    else:
        inverse_features = pd.DataFrame(scaler_fit.inverse_transform(feature_df), columns = feature_df.columns)
    
    output_list = []
    
    for idx, val in shap_values_df.iterrows():
        import_col_list = list(shap_values_df.iloc[idx,:].sort_values(axis = 0, ascending = False, kind = 'quicksort').head(20).index)
        
        temp_series = inverse_features.loc[idx, import_col_list]
        sub_series = temp_series[temp_series >= 1]
          
        value_list = []
        
        for sub_idx, sub_val in enumerate(sub_series):
            if sub_val >= 1 and sub_val < 2:
                str_val = 'Yes'
                friendly_col = friendly_column_dict.get(sub_series.index[sub_idx])
                value = str(friendly_col) + ': ' + '{0}'.format(str_val)
            else:
                friendly_col = friendly_column_dict.get(sub_series.index[sub_idx])
                value = str(friendly_col) + ': ' + '{:,.0f}'.format(sub_val)
                

            value_list.append(value)
            
        output_value = '\n'.join(value_list)
    
        output_list.append(output_value)
    
    return(output_list)
    
    
def create_encode_col_dict(df, encoder):
    string_col_list = list(df.select_dtypes(include = ['object']).columns)
    encode_col_list = list(encoder.get_feature_names(string_col_list))

    regex_non_char = re.compile('[^,0-9A-Z]')

    encode_col_dict = {}

    for col in encode_col_list:
        orig_col = col
        col = col.upper()
        col = regex_non_char.sub('_', col)
        col = col.replace(',', '').replace('____', '_').replace('___', '_').replace('__', '_')

        encode_col_dict[orig_col] = col
    
    return(encode_col_dict)