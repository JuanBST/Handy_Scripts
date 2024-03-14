
import pandas as pd
# import numpy as np
import time
from pyhive import presto
conn = presto.connect('presto.bstis.com')

tenantid = 'nonev_238000'

def get_rates(npis):
    sSQL = ''' 
    SELECT 
         --npi,
         r.plan_group_alias as plan,
         --payer,
         billing_code,
         billing_code_type,
         negotiated_type,
         --MAX(negotiated_rate) as rate_max,
         --MIN(negotiated_rate) as rate_min,     
         AVG(negotiated_rate) as rate_avg
         --COUNT(negotiated_rate) as rate_count
    FROM mrf.mrf_in_network_rates r 
    JOIN
        (  
          SELECT
          group_id,
          --npi,
          plan_group_alias
          FROM 
          mrf.mrf_provider_references
          WHERE 
          npi in ({a})
          and 
          plan_group_alias in ('bcbs_tx_ppo', 'bcbs_tx_hmo','uhc_choice_plus','uhc_navigate_plus','uhc_option_ppo')
          --plan_group_alias in ('bcbs_tx_ppo','uhc_option_ppo')        
          GROUP BY 
          1,2
        ) n
    ON n.group_id = r.provider_reference
       and n.plan_group_alias = r.plan_group_alias
    WHERE
       r.plan_group_alias in ('bcbs_tx_ppo', 'bcbs_tx_hmo','uhc_choice_plus','uhc_navigate_plus','uhc_option_ppo')
       --r.plan_group_alias in ('bcbs_tx_ppo','uhc_option_ppo')
        
       --and negotiated_type = 'negotiated'
    Group by 1,2,3,4 '''.format(a=npis)

    start = time.perf_counter()  
    df_temp = pd.read_sql(sSQL,conn)   
    df_temp = df_temp.pivot(index=['billing_code', 'billing_code_type', 'negotiated_type'],columns=['plan'],values=['rate_avg'])
    df_temp.columns = df_temp.columns.droplevel(0)
    df_temp = df_temp.reset_index()
    
    print('\nQuery time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    return df_temp


def get_allowed(npis):    
    sSQL = '''
    (
    SELECT 
    procedurecode,
    proceduretype,
    drg_code,
    revenue,
    sum(amtallowed) as allowed
    FROM bcbstx_nonev_prod.claims
    WHERE 
    tenantid = '{a}'
    and claimtype = 'MED'
    and innetworkflag <> 0
    and
    (
        billingprovidernpi in ({b})
        or
        providernpi in ({c})
    )
    and dosstart between date'2022-01-01' and date'2022-12-31'
    Group by 1,2,3,4
    )
    '''.format(a=tenantid,b=npis,c=npis)
    
    start = time.perf_counter()  
    df_temp = pd.read_sql(sSQL,conn)
    
    print('\nQuery time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    
    return df_temp






# Methodist
npis = "'1083937593','1871619254','1184179194','1497871628','1659525236','1689627655','1972557965','1952348021','1124074273','1437171568'"
df_method = get_rates(npis)
rt_method = get_allowed(npis)
df_method = df_method.to_csv('rates_Methodist.csv')
print("Methodist done\n")

#Scott & White
npis = "'1679691729','1871626143','1922061993','1093779704','1265772362','1396138970','1265568638','1962497800','1154315307','1477516466'"
df_scott_white = get_rates(npis)
df_scott_white.to_csv('rates_scott_white.csv')
print("SCott White done\n")

# MEMORIAL HERMANN HEALTH SYSTEM
npis = "'1740233782','1982666111','1295843787','1295788735','1083631972','1710985098','1730132234','1932152337'"
df_hermann = get_rates(npis)
df_hermann.to_csv('rates_hermann.csv')
print("Hermann done\n")

# SETON FAMILY OF HOSPITALS
npis = "'1093810327','1588997233','1164526786','1447355771','1750499273','1124137054','1942795133','1003833013','1730412388','1619115383','1578780870','1093708679'"
df_seton = get_rates(npis)
df_seton.to_csv('rates_seton.csv')
print("Seton done\n")

# TEXAS ONCOLOGY PA
npis = "'1821265265','1003083445','1811944101','1912174343','1972770303','1346794385','1912174350','1194992537','1033386578'"
df_Tx_Onco = get_rates(npis)
df_Tx_Onco.to_csv('rates_Tx_Onco.csv')
print("tX oNCO done\n")

# ST DAVIDS
npis = "'1518904689','1053683185','1881648871','1881648863','1699729681','1720025885','1629021845','1649223645'"
df_st_davids = get_rates(npis)
df_st_davids.to_csv('rates_st_davids.csv')
print("Davids done\n")


# TEXAS CHILDREN'S HOSPITAL
npis = "'1386278406','1134774037','1184078743','1174731145','1477643690','1568866234','1508298878'"
df_Tx_children = get_rates(npis)
df_Tx_children.to_csv('rates_Tx_children.csv')
print("Tx Children done\n")

# TX SOUTH
npis = "'1750334272','1639124332','1750351375','1972579365','1942270566','1811962673','1578533196','1710959135','1851361471','1760452767','1104891050','1700831484','1285798918'"
df_U_Tx_SouthW = get_rates(npis)
df_U_Tx_SouthW.to_csv('rates_U_Tx_SouthW.csv')
print("UTx SouthW done\n")

# Anest P
npis = "'1548208564'"
df_Anest_partn = get_rates(npis)
df_Anest_partn.to_csv('rates_Anest_partn.csv')
print("Anesthes done\n")


# THE METHODIST
npis = "'1548387418'"
df_the_methodist = get_rates(npis)
df_the_methodist.to_csv('rates_the_methodist.csv')
print("The Methodist done\n")


#AUSTIN REGIONAL CLINIC
npis = "'1821157579','1659858454','1265896096','1679878391','1871955286','1659691624','1124323845','1295291789','1124505920','1568949360','1861797581','1710443205'"
df_Austin_Regional = get_rates(npis)
df_Austin_Regional.to_csv('rates_Austin_Regional.csv')
print("Austin done\n")


# LUBBOCK COUNTY
npis = "'1821087164','1508855578','1487993630','1114991718','1174620546'"
df_Lubbock_County = get_rates(npis)
df_Lubbock_County.to_csv('rates_Lubbock_County.csv')
print("Lubbock done\n")




df_all = pd.DataFrame()
df_Lubbock_County['system'] = 'Lubbock_County'
df_all = df_all.append(df_Lubbock_County)

df_Austin_Regional['system'] = 'Austin_Regional'
df_all = df_all.append(df_Austin_Regional)

df_the_methodist['system'] = 'the_methodist'
df_all = df_all.append(df_the_methodist)

df_Anest_partn['system'] = 'Anest_partn'
df_all = df_all.append(df_Anest_partn)

df_U_Tx_SouthW['system'] = 'U_Tx_SouthW'
df_all = df_all.append(df_U_Tx_SouthW)

df_Tx_children['system'] = 'Tx_children'
df_all = df_all.append(df_Tx_children)

df_st_davids['system'] = 'st_davids'
df_all = df_all.append(df_st_davids)

df_Tx_Onco['system'] = 'Tx_Onco'
df_all = df_all.append(df_Tx_Onco)

df_seton['system'] = 'seton'
df_all = df_all.append(df_seton)

df_hermann['system'] = 'hermann'
df_all = df_all.append(df_hermann)

df_scott_white['system'] = 'scott_white'
df_all = df_all.append(df_scott_white)

df_method['system'] = 'method'
df_all = df_all.append(df_method)



