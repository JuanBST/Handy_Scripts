
import pandas as pd
#import numpy as np
import time
from pyhive import presto
conn = presto.connect('presto.bstis.com')


######################################################################################################################
######################################################################################################################  
######################################################################################################################
# Getting Multiple NPI group Rates

start = time.perf_counter()
sSQL = '''
SELECT 
     CASE
        WHEN npi in ('1083937593','1871619254','1184179194','1497871628','1659525236','1689627655','1972557965','1952348021','1124074273') 
            THEN 'METHODIST' 
        WHEN npi in ('1679691729','1871626143','1922061993','1093779704','1265772362','1396138970','1265568638','1962497800','1154315307','1477516466') 
            THEN 'SCOTTE & WHITE' 
        WHEN npi in ('1740233782','1982666111','1295843787','1295788735','1083631972','1710985098','1730132234','1932152337') 
            THEN 'HERMANN HOSP' 
        WHEN npi in ('1093810327','1588997233','1164526786','1447355771','1750499273','1124137054','1942795133','1003833013','1730412388','1619115383',
        '1578780870','1093708679')
            THEN 'ASC SETON' 
        WHEN npi in ('1518904689','1053683185','1881648871','1881648863','1699729681','1720025885','1629021845','1649223645') 
            THEN 'ST DAVIDS' 
        WHEN npi in ('1386278406','1134774037','1184078743','1174731145','1477643690','1568866234','1508298878') 
            THEN 'TX CHILD HOSP' 
        WHEN npi in ('1750334272','1639124332','1750351375','1972579365','1942270566','1811962673','1578533196','1710959135','1851361471','1760452767',
        '1104891050','1700831484','1285798918')
            THEN 'Univ Tx SW' 
        WHEN npi = '1548208564'
            THEN 'Anestes United' 
        WHEN npi = '1548387418'
            THEN 'The Methodist'     
        WHEN npi in ('1821157579','1659858454','1265896096','1679878391','1871955286','1659691624','1124323845','1295291789','1124505920','1568949360',
        '1861797581','1710443205')
            THEN 'Austin Reg' 
        WHEN npi in ('1821087164','1508855578','1487993630','1114991718','1174620546')
            THEN 'Hendrick MC' 
       WHEN  npi in ('1497403661','1073198156','1528064649','1063418937','1598761652','1932326055','1427601673','1861498735','1255337309','1831796689',
       '1437756293','1184233785','1871246876','1750990388')
            THEN 'Lubbock County'  
       WHEN  npi in ('1437171568','1972517365')
            THEN 'Covenant MC'        
       ELSE 'Others' END as hospital_system,
     r.plan_group_alias as plan,
     billing_code,
     billing_code_type,
     negotiated_type,    
     AVG(negotiated_rate) as rate_avg,
     MAX(negotiated_rate) as rate_max
FROM mrf.mrf_in_network_rates r 
JOIN
    (  
      SELECT
      npi,
      group_id,
      plan_group_alias
      FROM 
      mrf.mrf_provider_references
      WHERE 
      plan_group_alias in ('bcbs_tx_ppo', 'bcbs_tx_hmo','uhc_choice_plus','uhc_navigate_plus','uhc_option_ppo')
      and npi in (
       '1083937593','1871619254','1184179194','1497871628','1659525236','1689627655','1972557965','1952348021','1124074273',
       '1679691729','1871626143','1922061993','1093779704','1265772362','1396138970','1265568638','1962497800','1154315307','1477516466',
       '1740233782','1982666111','1295843787','1295788735','1083631972','1710985098','1730132234','1932152337', 
       '1093810327','1588997233','1164526786','1447355771','1750499273','1124137054','1942795133','1003833013','1730412388','1619115383',
        '1578780870','1093708679',
       '1518904689','1053683185','1881648871','1881648863','1699729681','1720025885','1629021845','1649223645', 
       '1386278406','1134774037','1184078743','1174731145','1477643690','1568866234','1508298878', 
       '1750334272','1639124332','1750351375','1972579365','1942270566','1811962673','1578533196','1710959135','1851361471','1760452767',
        '1104891050','1700831484','1285798918',
       '1548208564',
       '1548387418',
       '1821157579','1659858454','1265896096','1679878391','1871955286','1659691624','1124323845','1295291789','1124505920','1568949360',
       '1861797581','1710443205',
       '1821087164','1508855578','1487993630','1114991718','1174620546',
       '1497403661','1073198156','1528064649','1063418937','1598761652','1932326055','1427601673','1861498735','1255337309','1831796689',
       '1437756293','1184233785','1871246876','1750990388',
       '1437171568','1972517365') 
      GROUP BY 
      1,2,3
    ) n
ON n.group_id = r.provider_reference
   and n.plan_group_alias = r.plan_group_alias
WHERE
   r.plan_group_alias in ('bcbs_tx_ppo', 'bcbs_tx_hmo','uhc_choice_plus','uhc_navigate_plus','uhc_option_ppo')
Group by 1,2,3,4,5   
'''

df_rates = pd.read_sql(sSQL, conn)
print('\nQuery time : ' + str(round(time.perf_counter() - start, 1)) + ' secs')

df_rates = df_rates.pivot(index=['hospital_system','billing_code', 'billing_code_type', 'negotiated_type'], columns=[
                        'plan'], values=['rate_avg'])
df_rates.columns = df_rates.columns.droplevel(0)
df_rates = df_rates.reset_index()

df_rates = df_rates.rename(columns={'billing_code':'procedurecode','billing_code_type':'code_type'})
df_rates.procedurecode = df_rates.procedurecode.astype(str)
df_rates.procedurecode = df_rates.procedurecode.apply(lambda x : x.lstrip('0') )


######################################################################################################################
######################################################################################################################
######################################################################################################################
# Query Claims

start = time.perf_counter()
sSQL = '''
SELECT 
CASE
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1083937593','1871619254','1184179194','1497871628','1659525236','1689627655','1972557965','1952348021','1124074273') 
      THEN 'METHODIST' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1679691729','1871626143','1922061993','1093779704','1265772362','1396138970','1265568638','1962497800','1154315307','1477516466') 
      THEN 'SCOTTE & WHITE' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1740233782','1982666111','1295843787','1295788735','1083631972','1710985098','1730132234','1932152337') 
      THEN 'HERMANN HOSP' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1093810327','1588997233','1164526786','1447355771','1750499273','1124137054','1942795133','1003833013','1730412388','1619115383',
  '1578780870','1093708679')
      THEN 'ASC SETON' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1518904689','1053683185','1881648871','1881648863','1699729681','1720025885','1629021845','1649223645') 
      THEN 'ST DAVIDS' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1386278406','1134774037','1184078743','1174731145','1477643690','1568866234','1508298878') 
      THEN 'TX CHILD HOSP' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1750334272','1639124332','1750351375','1972579365','1942270566','1811962673','1578533196','1710959135','1851361471','1760452767',
  '1104891050','1700831484','1285798918')
      THEN 'Univ Tx SW' 
  WHEN COALESCE(billingprovidernpi,providernpi) = '1548208564'
      THEN 'Anestes United' 
  WHEN COALESCE(billingprovidernpi,providernpi) = '1548387418'
      THEN 'The Methodist'     
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1821157579','1659858454','1265896096','1679878391','1871955286','1659691624','1124323845','1295291789','1124505920','1568949360',
  '1861797581','1710443205')
      THEN 'Austin Reg' 
  WHEN COALESCE(billingprovidernpi,providernpi) in ('1821087164','1508855578','1487993630','1114991718','1174620546')
      THEN 'Hendrick MC' 
 WHEN  COALESCE(billingprovidernpi,providernpi) in ('1497403661','1073198156','1528064649','1063418937','1598761652','1932326055','1427601673','1861498735','1255337309','1831796689',
 '1437756293','1184233785','1871246876','1750990388')
      THEN 'Lubbock County'  
 WHEN  COALESCE(billingprovidernpi,providernpi) in ('1437171568','1972517365')
      THEN 'Covenant MC'        
 ELSE 'Others' END as hospital_system,

CASE
  WHEN SUBSTR(providerzip,1,2) = '79'
    THEN 'West' 
  ELSE 'Other' END as tx_region,
servicecategory,
procedurecode,
proceduretype,
drg_code,
revenue,
sum(amtallowed) as allowed
FROM bcbstx_nonev_prod.claims 
WHERE 
tenantid = 'nonev_238000' 
and filename in ('medBenSci_Claims_20220814_Extract.txt.gz','medBenSci_Claims_20221014_Extract.txt.gz','medBenSci_Claims_20221114_Extract.txt.gz','medBenSci_Claims_20230316_Extract.txt.gz',
    'medBenSci_Claims_20230414_Extract.txt.gz','medBenSci_Claims_Extract20220914.txt','medBenSci_Claims_Extract_00.txt','medBenSci_Claims_Extract_01.txt','medBenSci_Claims_Extract_02.txt',
    'medBenSci_Claims_Extract_03.txt','medBenSci_Claims_Extract_04.txt','medBenSci_Claims_Extract_05.txt','medBenSci_Claims_Extract_06.txt','medBenSci_Claims_Extract_07.txt',
    'medBenSci_Claims_Extract_08.txt','medBenSci_Claims_Extract_09.txt','medBenSci_Claims_Extract_10.txt','medBenSci_Claims_Extract_11.txt','medBenSci_Claims_Extract_12.txt',
    'medBenSci_Claims_Extract_13.txt','medBenSci_Claims_Extract_14.txt','medBenSci_Claims_Extract_15.txt','medBenSci_Claims_Extract_16.txt','medBenSci_Claims_Extract_17.txt',
    'medBenSci_Claims_Extract_18.txt','medBenSci_Claims_Extract_19.txt','medBenSci_Claims_Extract_20.txt','medBenSci_Claims_Extract_21.txt','medBenSci_Claims_Extract_22.txt',
    'medBenSci_Claims_Extract_23.txt','medBenSci_Claims_Extract_24.txt','medBenSci_Claims_Extract_25.txt','medBenSci_Claims_Extract_26.txt','medBenSci_Claims_Extract_27.txt',
    'medBenSci_Claims_Extract_28.txt','medBenSci_Claims_Extract_29.txt','medBenSci_Claims_Extract_30.txt','medBenSci_Claims_Extract_31.txt','medBenSci_Claims_Extract_32.txt',
    'medBenSci_Claims_Extract_33.txt','medBenSci_Claims_Extract_34.txt','medBenSci_Claims_Extract_35.txt','medBenSci_Claims_Extract_36.txt','medBenSci_Claims_Extract_37.txt',
    'medBenSci_Claims_Extract_38.txt','medBenSci_Claims_Extract_39.txt','medBenSci_Claims_Extract_40.txt','medBenSci_Claims_Extract_41.txt','medBenSci_Claims_Extract_42.txt',
    'medBenSci_Claims_Extract_43.txt','medBenSci_Claims_Extract_44.txt','medBenSci_Claims_Extract_45.txt','medBenSci_Claims_Extract_46.txt','medBenSci_Claims_Extract_47.txt',
    'medBenSci_Claims_Extract_48.txt','medBenSci_Claims_Extract_49.txt','medBenSci_Claims_Extract_50.txt','medBenSci_Claims_Extract_51.txt','medBenSci_Claims_Extract_52.txt',
    'medBenSci_Claims_Extract_53.txt','medBenSci_Claims_Extract_54.txt')
Group by 1,2,3,4,5,6,7
'''

df_claims = pd.read_sql(sSQL, conn)
print('\nQuery time : ' + str(round(time.perf_counter() - start, 1)) + ' secs')

df_claims.allowed = df_claims.allowed.astype(float)

a1 = df_claims.loc[(df_claims.proceduretype == 'CPT')|(df_claims.proceduretype == 'HCPCS')].copy()
a1 = a1.drop(['proceduretype','drg_code','revenue'],axis=1)
a1 = a1.groupby(['hospital_system', 'tx_region', 'servicecategory', 'procedurecode']).sum().reset_index()
a1['code_type'] = 'CPT'

a2 = df_claims.loc[df_claims.drg_code.notnull()].copy()
a2 = a2.drop(['proceduretype','procedurecode','revenue'],axis=1)
a2 = a2.groupby(['hospital_system', 'tx_region', 'servicecategory', 'drg_code']).sum().reset_index()
a2 = a2.rename(columns={'drg_code':'procedurecode'})
a2['code_type'] = 'MS-DRG'

a3 = df_claims.loc[df_claims.revenue.notnull()].copy()
a3 = a3.drop(['proceduretype','procedurecode','drg_code'],axis=1)
a3 = a3.groupby(['hospital_system', 'tx_region', 'servicecategory', 'revenue']).sum().reset_index()
a3 = a3.rename(columns={'revenue':'procedurecode'})
a3['code_type'] = 'RC'


df = a1.append(a2).append(a3)
df.procedurecode = df.procedurecode.apply(lambda x : x.lstrip('0') )
df.procedurecode = df.procedurecode.astype(str)

del a1,a2,a3,sSQL

######################################################################################################################
######################################################################################################################
######################################################################################################################
# Matching Data
df0 = pd.merge(df,df_rates,on=['procedurecode','code_type','hospital_system'],how='left')
