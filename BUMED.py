#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 07:14:46 2018

@author: matthewparis
"""

import pandas as pd
import time
# =============================================================================
# Functions
# =============================================================================
start = time.time()
def find_sum(x):
    try:
        num = sum_if.loc[sum_if['PE'] == x + 'N'].iloc[0][2]
    except:
        num = 0
    return num

def smart_value(y, x):
    try:
        value = smart[y].loc[x]
    except:
        value = 0
    return value 

def fsa_function(col_name1, col_name2, x):
    try:
        value = fsa_nmrc[[col_name1, col_name2]].set_index(col_name1).loc[x][0]
    except:
        value = 0
    return value

def func(df):
    benchmark = 1.0
    df['Obs+Coms'] = df[['Obligations', 'Commitments']].sum(axis=1)
    df['Unobligated and/or Uncommitted'] = df['Authority'] - df['Obs+Coms']
    df['Delta (AP-O/C)'] = df['Control'] - df['Obs+Coms']
    df['Obs/Ctrl'] = df['Obligations'] / df['Control']
    df['Obs + Comms/Ctrl'] = df['Obs+Coms'] / df['Control']
    df['Obs/Auth'] = df['Obligations'] / df['Authority']
    df['Planned Benchmark Dollar Value'] = df['Control'] * benchmark
    df['Dollar Value Behind/Ahead Benchmark'] = df['Obligations'] - df['Planned Benchmark Dollar Value']
    return df

# =============================================================================
# DataSets, fill blanks with 0s
# =============================================================================
baers = pd.read_excel('FY18 BAERS APF Data 10.01.18.xlsx')
smart = pd.read_excel('FY18 SMART BENCHMARK PASTE PE REPORT ALL ACTIVITIES.xls', header = 1, skiprows = 10)
smart = smart.set_index('Unnamed: 0')
smart = smart.fillna(0)
# =============================================================================
# Import BAG, PE Numbers and PE Title
# =============================================================================
Navy_Med_East = pd.read_excel('Navy_Med_East.xlsx')
Navy_Med_West = pd.read_excel('Navy_Med_West.xlsx')
fsa_nmrc = pd.read_excel('FSA_NMRC.xlsx')
fsa = pd.read_excel('FSA.xlsx')
Bummed = pd.read_excel('bummed.xlsx')

# =============================================================================
# Create Navy Med East Sheet
# =============================================================================
sum_if = baers.groupby(['PE', 'REGION'])['2018'].sum().reset_index()
sum_if = sum_if[sum_if['REGION'].isin(['E'])]
Navy_Med_East['Control'] = Navy_Med_East['PE'].apply(lambda x: find_sum(x)*1000)
Navy_Med_East['Authority'] = Navy_Med_East['PE'].apply(lambda x: smart_value('*689080', x))
Navy_Med_East['Obligations'] = Navy_Med_East['PE'].apply(lambda x: smart_value('*689080-1', x))
Navy_Med_East['Commitments'] = Navy_Med_East['PE'].apply(lambda x: smart_value('*689080-2', x))
Navy_Med_East.set_index('BAG', inplace = True)
for i in [1, 3, 4, 5, 6, 7]:
    try:
        x = Navy_Med_East.groupby('BAG')['Control'].sum()[i]
        Navy_Med_East.at['Total Bag ' + str(i), 'Control'] = x
    except:
        next
Navy_Med_East.reset_index(inplace = True)

# =============================================================================
# Create Navy Med West Sheet
# =============================================================================
sum_if = baers.groupby(['PE', 'REGION'])['2018'].sum().reset_index()
sum_if = sum_if[sum_if['REGION'].isin(['H'])]
Navy_Med_West['Control'] = Navy_Med_West['PE'].apply(lambda x: find_sum(x)*1000)
Navy_Med_West['Authority'] = Navy_Med_West['PE'].apply(lambda x: smart_value('*689060', x))
Navy_Med_West['Obligations'] = Navy_Med_West['PE'].apply(lambda x: smart_value('*689060-1', x))
Navy_Med_West['Commitments'] = Navy_Med_West['PE'].apply(lambda x: smart_value('*689060-2', x))
Navy_Med_West.set_index('BAG', inplace = True)
for i in [1, 3, 4, 5, 6, 7]:
    try:
        x = Navy_Med_West.groupby('BAG')['Control'].sum()[i]
        Navy_Med_West.at['Total Bag ' + str(i), 'Control'] = x
    except:
        next
Navy_Med_West.reset_index(inplace = True)
# =============================================================================
# Create FSA-NMRC Sheet
# =============================================================================
sum_if = baers.groupby(['PE', 'UIC'])['2018'].sum().reset_index()
sum_if = sum_if[sum_if['UIC'].isin(['32398'])]
fsa_nmrc['Control'] = fsa_nmrc['PE'].apply(lambda x: find_sum(x)*1000)
fsa_nmrc['Authority'] = fsa_nmrc['PE'].apply(lambda x: smart_value('*628110', x))
fsa_nmrc['Obligations'] = fsa_nmrc['PE'].apply(lambda x: smart_value('*628110-1', x))
fsa_nmrc['Commitments'] = fsa_nmrc['PE'].apply(lambda x: smart_value('*628110-2', x))
fsa_nmrc.set_index('BAG', inplace = True)
for i in [1, 3, 4, 5, 6, 7]:
    try:
        x = fsa_nmrc.groupby('BAG')['Control'].sum()[i]
        fsa_nmrc.at['Total Bag ' + str(i), 'Control'] = x
    except:
        next
fsa_nmrc.reset_index(inplace = True)

# =============================================================================
# Create BUMMED Sheet
# =============================================================================
sum_if = baers.groupby(['PE', 'REGION'])['2018'].sum().reset_index()
sum_if = sum_if[sum_if['REGION'].isin(['M'])]
Bummed['Control'] = Bummed['PE'].apply(lambda x: find_sum(x)*1000)
Bummed['Authority'] = Bummed['PE'].apply(lambda x: smart_value('*371000', x))
Bummed['Obligations'] = Bummed['PE'].apply(lambda x: smart_value('*371000-1', x))
Bummed['Commitments'] = Bummed['PE'].apply(lambda x: smart_value('*371000-2', x))
Bummed.set_index('BAG', inplace = True)
for i in [1, 3, 4, 5, 6, 7]:
    try:
        x = Bummed.groupby('BAG')['Control'].sum()[i]
        Bummed.at['Total Bag ' + str(i), 'Control'] = x
    except:
        next
Bummed.reset_index(inplace = True)

# =============================================================================
# Create FSA File
# =============================================================================
sum_if = baers.groupby(['PE', 'REGION'])['2018'].sum().reset_index()
sum_if = sum_if[sum_if['REGION'].isin(['E'])]
fsa['Control'] = fsa['PE'].apply(lambda x: (find_sum(x)*1000) - fsa_function('PE', 'Control', x))
fsa['Authority'] = fsa['PE'].apply(lambda x: smart_value('*000180', x) - fsa_function('PE', 'Authority', x))
fsa['Obligations'] = fsa['PE'].apply(lambda x: smart_value('*000180-1', x) - fsa_function('PE', 'Obligations', x))
fsa['Commitments'] = fsa['PE'].apply(lambda x: (smart_value('*000180-2', x) - fsa_function('PE', 'Commitments', x)))
fsa.set_index('BAG', inplace = True)
for i in [1, 3, 4, 5, 6, 7]:
    try:
        x = fsa.groupby('BAG')['Control'].sum()[i]
        fsa.at['Total Bag ' + str(i), 'Control'] = x
    except:
        next
fsa.reset_index(inplace = True)

# =============================================================================
# Total BSO
# =============================================================================
total1 = [Navy_Med_East.set_index(['BAG', 'PE', 'PE Title']), Navy_Med_West.set_index(['BAG', 'PE', 'PE Title']), 
         fsa_nmrc.set_index(['BAG', 'PE', 'PE Title']), fsa.set_index(['BAG', 'PE', 'PE Title']), Bummed.set_index(['BAG', 'PE', 'PE Title'])]

df = pd.concat((total1))
df.reset_index(inplace = True)
total_bso = df.groupby(['BAG', 'PE', 'PE Title']).sum().reset_index()

# =============================================================================
# Add Additional Fields to DataFrames
# =============================================================================
total = [Navy_Med_East, Navy_Med_West, fsa_nmrc, fsa, Bummed, total_bso]
for i in range(len(total)):
    func(total[i])
    
end = time.time()
print('Script took ' + str(end-start) + ' seconds.')

# =============================================================================
# Clear out Variables
# =============================================================================
del smart, baers, x, i, sum_if, total, df, total1, end, start