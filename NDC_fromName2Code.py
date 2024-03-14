# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 17:13:07 2022

@author: JuanChamieQuintero
"""

import pandas as pd

# Original file at: https://s3.us-west-2.amazonaws.com/com.bstis.tpafiles/DM_transfer_files/Juan/NDC_all.csv
# Web source: https://data.world/johnsnowlabs/detailed-drug-facts-label-structure-by-ndc/workspace/file?filename=Detailed+Drug+Facts+Label+Structure+by+NDC.csv.gz
df_ndc = pd.read_csv(r'C:\Users\JuanChamieQuintero\Documents\VBA Smart Cards\NDC_all.csv', encoding = 'ISO-8859-1')

df_ndc.columns


df_ndc['name'] = df_ndc['Proprietary_Name'].str.upper()

# SUBSTITUTABLE DRUGS
subst = ['OLMESARTAN-HYDROCHLOROTHIAZIDE', 'SOLODYN', 'BENICAR', 'DESVENLAFAXINE SUCCINATE ER', 
      'OLOPATADINE HCL', 'BENICAR HCT', 'VYTORIN', 'ZIANA', 'DUEXIS', 'LOESTRIN FE', 'CELEBREX', 'OLMESARTAN MEDOXOMIL-HCTZ',
      'OLMESARTAN-AMLODIPINE-HCTZ', 'ZAFIRLUKAST', 'BYSTOLIC', 'DEXILANT', 'LO LOESTRIN FE', 'TREXIMET', 'DUAC', 'CONTRAVE', 
      'OLMESARTAN MEDOXOMIL', 'DESVENLAFAXINE ER', 'DUAC CS', 'ACANYA', 'VIMOVO', 'BENZACLIN', 'BEYAZ', 'DUEXIS']

df_ndc['in_list'] = df_ndc['name'].apply( lambda x : True if x in subst else False)

df_ndc['in_list3'] = df_ndc['name'].apply( lambda x : True if x in subst else False)


df_ndc['NDC8'] = df_ndc['Code'].replace('-','',regex=True)

df_ndc['NDC8'] = df_ndc['NDC8'].apply( lambda x : x[:9] )

subst_ndc = df_ndc.loc[df_ndc.in_list,'NDC8'].apply( lambda x : x[:9] ).drop_duplicates().to_list()

df_ndc['in_list2'] = df_ndc['NDC8'].apply( lambda x : True if x in subst_ndc else False)


test = df_ndc[df_ndc.in_list2 == True]

print(df_ndc['in_list'].sum())
print(df_ndc['in_list2'].sum())

final_list = df_ndc.loc[df_ndc.in_list==True,'NDC8'].to_list()

print(final_list)
