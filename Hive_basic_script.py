
import os
import pandas as pd
import time
from pyhive import presto
conn = presto.connect('presto.bstis.com')


only_inpatient = False


hospital_npis ={'1043719560': 'JACKSONVILLE HOSPITAL LLC',
                '1174021695': 'REHABILITATION HOSPITAL, LLC',
                '1184132524': 'PITTSBURG HOSPITAL LLC',
                '1205335726': 'QUITMAN HOSPITAL LLC',
                '1306345764': 'QUITMAN HOSPITAL LLC',
                '1326546797': 'HENDERSON HOSPITAL, LLC',
                '1407355860': 'CARTHAGE HOSPITAL, LLC',
                '1407364847': 'TYLER REGIONAL HOSPITAL LLC',
                '1417465824': 'ATHENS HOSPITAL, LLC',
                '1417941295': 'UNIVERSITY OF TEXAS HEALTH SCIENCE CENTER AT TYLER (NORTH)',
                '1477061885': 'QUITMAN HOSPITAL LLC',
                '1497254858': 'PITTSBURG HOSPITAL LLC',
                '1538667035': 'CARTHAGE HOSPITAL, LLC',
                '1639678030': 'CARTHAGE HOSPITAL, LLC',
                '1730697350': 'JACKSONVILLE HOSPITAL LLC',
                '1770082299': 'JACKSONVILLE HOSPITAL LLC',
                '1861991226': 'JACKSONVILLE HOSPITAL LLC',
                '1932608452': 'QUITMAN HOSPITAL LLC',
                '1952800310': 'HENDERSON HOSPITAL, LLC',
                '1962900472': 'SPECIALTY HOSPITAL, LLC (LTAC)'}


match_codes = {'MS-DRG':'DRG',
               'CPT':'CPT', 
               'HCPCS':'CPT',
               'RC':'REV', 
               'ICD':'ICDP',
               'ICDP':'ICDP',
               'Revenue':'REV',
               'DRG':'DRG'
               }


###############################################################################
# BCBS CLAIMS 2022


try:
    
    df_c = pd.read_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npi_claims.csv')
    
except:

    sSQL ='''
    (
    SELECT 
    'DRG' as proceduretype,
    drg_code as procedurecode,
    providernpi,
    servicecategory,
    sum(amtallowed) as sum_amt,
    avg(amtallowed) as avg_amt,
    count (distinct concat(personid, cast(dosstart as varchar))) as claims
    FROM allbcbstxev.claims
    WHERE
    providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    and filename in ('bcbstx_med_20220701.txt', 'bcbstx_med_20220501.txt', 'bcbstx_med_20220301.txt', 
                     'bcbstx_med_20220401.txt', 'bcbstx_med_20220201.txt', 'bcbstx_med_20220101.txt', 
                     'bcbstx_med_20220901.txt', 'bcbstx_med_20220801.txt', 'bcbstx_med_20221101.txt', 
                     'bcbstx_med_20221201.txt', 'bcbstx_med_20221001.txt', 'bcbstx_med_20220601.txt', 
                     'bcbstx_med_20230101.txt')
    and dosstart >= date'2022-01-01'
    --and servicecategory = 'Inpatient'
    --and tenantid = 'quanta'
    and drg_code <> ''
    and dosstart >= date'2022-01-01'
    GROUP BY
    1,2,3,4
    )
    UNION
    (
    SELECT 
    'CPT' as proceduretype,
    cpt_code as procedurecode,
    providernpi,
    servicecategory,
    sum(amtallowed) as sum_amt,
    avg(amtallowed) as avg_amt,
    count (distinct concat(personid, cast(dosstart as varchar))) as claims
    FROM allbcbstxev.claims
    WHERE
    providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    and filename in ('bcbstx_med_20220701.txt', 'bcbstx_med_20220501.txt', 'bcbstx_med_20220301.txt', 
                     'bcbstx_med_20220401.txt', 'bcbstx_med_20220201.txt', 'bcbstx_med_20220101.txt', 
                     'bcbstx_med_20220901.txt', 'bcbstx_med_20220801.txt', 'bcbstx_med_20221101.txt', 
                     'bcbstx_med_20221201.txt', 'bcbstx_med_20221001.txt', 'bcbstx_med_20220601.txt', 
                     'bcbstx_med_20230101.txt')
    and dosstart >= date'2022-01-01'
    --and servicecategory = 'Inpatient'
    --and tenantid = 'quanta'
    and dosstart >= date'2022-01-01'
    and cpt_code <> ''
    GROUP BY
    1,2,3,4
        )  
    '''
    #and tenantid = 'quanta'

    start = time.perf_counter()  
    df_c01 = pd.read_sql(sSQL,conn)
    print('\nTotal time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    del start, sSQL    
    # 10 minutes to run


    # sSQL ='''
    # SELECT 
    # cpt_code,
    # proceduretype,
    # procedurecode,
    # providernpi,
    # billingprovidernpi,
    # drg_code,
    # servicecategory,
    # sum(amtallowed) as sum_amt,
    # avg(amtallowed) as avg_amt,
    # count(distinct tpaclaimid) as claims
    # FROM allbcbstxev.claims
    # WHERE
    # providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    # '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    # '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    # and filename in ('bcbstx_med_20220701.txt', 'bcbstx_med_20220501.txt', 'bcbstx_med_20220301.txt', 
    #                  'bcbstx_med_20220401.txt', 'bcbstx_med_20220201.txt', 'bcbstx_med_20220101.txt', 
    #                  'bcbstx_med_20220901.txt', 'bcbstx_med_20220801.txt', 'bcbstx_med_20221101.txt', 
    #                  'bcbstx_med_20221201.txt', 'bcbstx_med_20221001.txt', 'bcbstx_med_20220601.txt', 
    #                  'bcbstx_med_20230101.txt')
    # and dosstart >= date'2022-01-01'
    # --and servicecategory = 'Inpatient'
    # --and tenantid = 'quanta'
    # GROUP BY
    # 1,2,3,4,5,6,7      
    # '''
    # #and tenantid = 'quanta'

    # start = time.perf_counter()  
    # df_c1 = pd.read_sql(sSQL,conn)
    # print('\nTotal time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    # del start, sSQL


 
    sSQL ='''
    (
    SELECT 
    'DRG' as proceduretype,
    drg_code as procedurecode,
    providernpi,
    servicecategory,
    sum(amtallowed) as sum_amt,
    avg(amtallowed) as avg_amt,
    count (distinct concat(personid, cast(dosstart as varchar))) as claims
    FROM bcbstx_nonev_prod.claims
    WHERE
    providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    and dosstart >= date'2022-01-01'
    --and servicecategory = 'Inpatient'
    --and tenantid = 'quanta'
    and drg_code <> ''
    and dosstart >= date'2022-01-01'
    GROUP BY
    1,2,3,4
    )
    UNION
    (
    SELECT 
    'CPT' as proceduretype,
    cpt_code as procedurecode,
    providernpi,
    servicecategory,
    sum(amtallowed) as sum_amt,
    avg(amtallowed) as avg_amt,
    count (distinct concat(personid, cast(dosstart as varchar))) as claims
    FROM bcbstx_nonev_prod.claims
    WHERE
    providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    and dosstart >= date'2022-01-01'
    --and servicecategory = 'Inpatient'
    --and tenantid = 'quanta'
    and dosstart >= date'2022-01-01'
    and cpt_code <> ''
    GROUP BY
    1,2,3,4
        )  
    '''
    #and tenantid = 'quanta'

    start = time.perf_counter()  
    df_c02 = pd.read_sql(sSQL,conn)
    print('\nTotal time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    del start, sSQL      
    # 5 minutes to run


    # sSQL ='''
    # SELECT 
    # cpt_code,
    # proceduretype,
    # procedurecode,
    # providernpi,
    # billingprovidernpi,
    # drg_code,
    # servicecategory,
    # sum(amtallowed) as sum_amt,
    # avg(amtallowed) as avg_amt,
    # count(distinct tpaclaimid) as claims
    # FROM bcbstx_nonev_prod.claims
    # WHERE
    # providernpi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
    # '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
    # '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
    # and dosstart >= date'2022-01-01'
    # --and servicecategory = 'Inpatient'
    # --and tenantid = 'quanta'
    # GROUP BY
    # 1,2,3,4,5,6,7
    # '''
    # #and tenantid = 'quanta'

    # start = time.perf_counter()  
    # df_c2 = pd.read_sql(sSQL,conn)
    # print('\nTotal time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    # del start, sSQL    
 
    df_c = pd.concat([df_c01, df_c02])
    df_c.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npi_claims.csv',index=False)
    # df_cx = df_c.copy()
    del df_c01, df_c02



df_c.sum_amt = df_c.sum_amt.astype(float)
df_c.avg_amt = df_c.avg_amt.astype(float)


df_c.procedurecode = df_c.procedurecode.apply( lambda x : int(x) if x.isdigit() else x ).astype(str)

#df_c.drg_code = df_c.drg_code.astype(str).apply( lambda x : x.replace('.0','') )
#df_c.drg_code = df_c.drg_code.apply( lambda x : int(x) if x.isdigit() else x ).astype(str)

df_c['providernpi'] = df_c['providernpi'].astype(str)

df_c['hospital'] = df_c['providernpi'].map(hospital_npis)
#df_c = df_c.drop(['providernpi'],axis=1)

#df_c.columns


###############################################################################

if only_inpatient == True:      
    df_c0 = df_c[df_c.servicecategory == 'Inpatient'].drop(['servicecategory'],axis=1)     
else:   
    df_c0 = df_c.drop(['servicecategory'],axis=1)    
    

    
###############################################################################

# DRG 

df_c_drg = df_c0.loc[ df_c0.proceduretype == 'DRG', :].drop(['proceduretype'],axis=1)  

df_c_drg = df_c_drg.groupby(['procedurecode', 'hospital']).sum().reset_index()

df_c_drg['avg_amt'] = df_c_drg['sum_amt'] / df_c_drg['claims'] 

df_c_drg['hospital-code'] = df_c_drg['hospital'] + '-' + df_c_drg.procedurecode


# REV CODE

# df_c_rev = df_c0.loc[ df_c0.proceduretype == 'Revenue', :].drop(['cpt_code', 'proceduretype', 'drg_code'],axis=1)  

# df_c_rev = df_c_rev.groupby(['procedurecode', 'hospital' ]).sum().reset_index()

# df_c_rev['avg_amt'] = df_c_rev['sum_amt'] / df_c_rev['claims'] 

# df_c_rev['hospital-code'] = df_c_rev['hospital'] + '-' + df_c_rev.procedurecode


# CPT CODE

df_c_cpt = df_c0.loc[ df_c0.proceduretype == 'CPT', :].drop(['proceduretype'],axis=1)  

df_c_cpt = df_c_cpt.groupby(['procedurecode', 'hospital' ]).sum().reset_index()

df_c_cpt['avg_amt'] = df_c_cpt['sum_amt'] / df_c_cpt['claims'] 

df_c_cpt['hospital-code'] = df_c_cpt['hospital'] + '-' + df_c_cpt.procedurecode


del df_c0



###############################################################################
###############################################################################
# PANORAMA DATA


try:
    
    df_p = pd.read_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npis_panorama.csv')
    
except:

    sSQL ='''
    SELECT 
        npi,
        r.plan_group_alias,
        payer,
        billing_code,
        billing_code_type,
        negotiated_type,
        SUM(negotiated_rate) as rate_sum,
        MAX(negotiated_rate) as rate_max,
        MIN(negotiated_rate) as rate_min,
        COUNT(negotiated_rate) as rate_count
    FROM mrf.mrf_in_network_rates r 
    JOIN
    (  
      SELECT
          group_id,
          npi,
          plan_group_alias
      FROM 
          mrf.mrf_provider_references
      WHERE 
          plan_group_alias in ('aetna_choice_pos_ii','aetna_open_access_managed',
                               'bcbs_tx_ppo',
                               'uhc_choice_plus','uhc_option_ppo','uhc_allsavers_ppo')
          and npi in ('1043719560','1174021695','1184132524','1205335726','1306345764','1326546797',
                      '1407355860','1407364847','1417465824','1417941295','1477061885','1497254858','1538667035','1639678030',
                      '1730697350','1770082299','1861991226','1932608452','1952800310','1962900472')
      GROUP BY 
          1,2,3
    ) n
    ON n.group_id = r.provider_reference
    and n.plan_group_alias = r.plan_group_alias
    WHERE r.plan_group_alias in ('aetna_choice_pos_ii','aetna_open_access_managed',
                               'bcbs_tx_ppo',
                               'uhc_choice_plus','uhc_option_ppo','uhc_allsavers_ppo')
    GROUP BY 1,2,3,4,5,6
    '''
    
    start = time.perf_counter()  
    df_p = pd.read_sql(sSQL,conn)
    df_p.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npis_panorama.csv',index=False)
    print('\nTotal time : ' + str(round(time.perf_counter() - start,1)) + ' secs'  )
    del start
    
    # 8 minutes to run

df_p.rate_sum = df_p.rate_sum.astype(float)
df_p.rate_max = df_p.rate_max.astype(float)
df_p.rate_min = df_p.rate_min.astype(float)
df_p.rate_count = df_p.rate_count.astype(int)


# Renane NPI to group hospital rates
df_p.columns
df_p['npi'] = df_p['npi'].astype(str)

df_p['hospital'] = df_p['npi'].map(hospital_npis)
df_p = df_p.drop(['npi'],axis=1)


df_p.billing_code = df_p.billing_code.apply( lambda x : int(x) if x.isdigit() else x )
df_p.billing_code = df_p.billing_code.astype(str)

#df_p.negotiated_type.unique()
df_p.negotiated_type = df_p.negotiated_type.apply(lambda x : x.replace('fee schedule','fee').replace('negotiated','fee'))


# Combine plans




#df_h = df_h[df_h.rate_avg > 100]

df_p.columns

###############################################################################
# DRG

df_p_drg = df_p.loc[df_p.billing_code_type == 'MS-DRG',:].drop(['plan_group_alias','billing_code_type'],axis=1)

df_p_drg = df_p_drg.groupby(['payer','billing_code','negotiated_type','hospital']).agg(
                        {'rate_sum':'sum','rate_max':'max','rate_min':'min','rate_count':'sum'} ).reset_index()

df_p_drg['rate_avg'] = df_p_drg['rate_sum'] / df_p_drg['rate_count']


df_p_drg['hospital-code'] = df_p_drg.hospital + '-' + df_p_drg.billing_code



# REV CODE

df_p_rev = df_p.loc[df_p.billing_code_type == 'RC',:].drop(['plan_group_alias','billing_code_type'],axis=1)

df_p_rev = df_p_rev.groupby(['payer','billing_code','negotiated_type','hospital']).agg(
                        {'rate_sum':'sum','rate_max':'max','rate_min':'min','rate_count':'sum'} ).reset_index()

df_p_rev['rate_avg'] = df_p_rev['rate_sum'] / df_p_rev['rate_count']


df_p_rev['hospital-code'] = df_p_rev.hospital + '-' + df_p_rev.billing_code

del hospital_npis



# CPT

df_p_cpt = df_p.loc[df_p.billing_code_type == 'CPT',:].drop(['plan_group_alias','billing_code_type'],axis=1)

df_p_cpt = df_p_cpt.groupby(['payer','billing_code','negotiated_type','hospital']).agg(
                        {'rate_sum':'sum','rate_max':'max','rate_min':'min','rate_count':'sum'} ).reset_index()

df_p_cpt['rate_avg'] = df_p_cpt['rate_sum'] / df_p_cpt['rate_count']


df_p_cpt['hospital-code'] = df_p_cpt.hospital + '-' + df_p_cpt.billing_code



###############################################################################
###############################################################################
# HOSPITALS DATA



try:
    
    df_h = pd.read_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npis_hospitals.csv')

except:

    # Get the list of all files and directories
    path = r"C:\Users\JuanChamieQuintero\Documents\Panorama"
    dir_list = os.listdir(path)
    
    payers = {'AETNA [101529]':'AETNA',
              'UMR UNITED HEALTHCARE [203502]':'UHC',
              'BLUE CROSS TRADITIONAL OF TEXAS [115503], BLUE CRO':'BCBS',
              'CIGNA PPO [126025]':'CIGNA'}
    
    hospitals ={'EIN_82-4037220_2022_UTHealthJacksonvilleHospital_standardcharges.csv': 'JACKSONVILLE HOSPITAL LLC',
                'EIN_82-3913174_2022_UTHealthRehabHospitall_standardcharges.csv': 'REHABILITATION HOSPITAL, LLC',
                'EIN_82-3953636_2022_UTHealthPittsburgHospital_standardcharges.csv': 'PITTSBURG HOSPITAL LLC',
                'EIN_82-3817196_2022_UTHealthQuitmanHospital_standardcharges.csv': 'QUITMAN HOSPITAL LLC',
                'EIN_82-4019349_2022_UTHealthHendersonHospital_standardcharges.csv': 'HENDERSON HOSPITAL, LLC',
                'EIN_82-4005981_2022_UTHealthCarthageHospital_standardcharges.csv': 'CARTHAGE HOSPITAL, LLC',
                'EIN_82-3878395_2022_UTHealthTylerHospital_standardcharges.csv': 'TYLER REGIONAL HOSPITAL LLC',
                'EIN_82-3934511_2022_UTHealthAthensHospital_standardcharges.csv': 'ATHENS HOSPITAL, LLC',
                'EIN_30-1163729_2022_UTHealthNorthHospital_standardcharges.csv': 'UNIVERSITY OF TEXAS HEALTH SCIENCE CENTER AT TYLER (NORTH)',
                'EIN_82-3970937_2022_UTHealthSpecialtyHospital_standardcharges.csv': 'SPECIALTY HOSPITAL, LLC (LTAC)'}

    df_h = pd.DataFrame()
    #file =  dir_list[4]
    
    for file in dir_list:
        if file[:3] == 'EIN':
            print('Reading',file)
            file_path = path + '\\' + file
            df_temp = pd.read_csv(file_path,delimiter='|',header=1)
            df_temp = df_temp.rename(columns={'Code Type':'code_type',
                                              'Inpatient Expected Reimbursement':'Amt',
                                              'Procedure Description':'Description'})
            df_temp = df_temp[ df_temp.code_type == 'DRG' ]
            df_temp = df_temp.loc[:,['Procedure','Plan','Amt','Description']]
            
            df_h0 = pd.DataFrame()
            for payer in payers.keys():
                try:
                    # payer = 'CIGNA PPO [126025]'
                    df_temp2 = df_temp[ df_temp.Plan == payer ]
                    #df_temp2 = df_temp2.fillna(0)
                    df_temp2 = df_temp2[ df_temp2.Amt > 0 ]
                    df_temp2['payer'] = payers[payer]
                    df_temp2['hospital'] = hospitals[file]
                    
                    df_h0 = pd.concat([df_h0,df_temp2])
                    print(payers[payer],'done.')
                except:
                    print(payers[payer],'failed.')
                    
            df_h = pd.concat([df_h,df_h0])
    
    # df_rates = df_h.copy()
    # df_h = df_rates.copy()
    
    df_h['Procedure'] = df_h['Procedure'].apply( lambda x : x.replace('MS',''))
    df_h.Procedure = df_h.Procedure.apply( lambda x : int(x) if x.isdigit() else x )
    df_h.Procedure = df_h.Procedure.astype(str) 
    
    df_h = df_h.drop(['Plan','Description'],axis=1)
    
    df_h['npi-code'] = df_h.hospital + '-' + df_h.Procedure
    
    
    
    df_h.columns

    del df_temp, df_temp2, dir_list, file, file_path, hospitals, path, payer
    
    df_h.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\npis_hospitals.csv',index=False)







###############################################################################
###############################################################################
# COMBINING THE DATA - DRG

payers = df_h.payer.unique()


# DRG in PANORAMA DATA

negoc = df_p_drg.negotiated_type.unique()
rates = ['rate_max','rate_min','rate_avg']

df_temp0 = df_p_drg.copy()
df_temp0.columns

df_final_drg = df_c_drg.copy()

for payer in payers:
    # payer = 'AETNA'
    df_temp1 = df_temp0[df_temp0.payer == payer]
    
    for neg in negoc:
    # neg = 'fee'
    
        df_temp2 = df_temp1[df_temp1.negotiated_type == neg]  
    
        for rate in rates:
        # rate = 'rate_max'
        
            map_rates = dict( zip ( df_temp2.loc[:,'hospital-code'], df_temp2.loc[:,rate] ) )
        
            col_name = payer + '-' + rate + '-' + neg
            df_final_drg[col_name] = df_final_drg['hospital-code'].map(map_rates)


del negoc, rates, df_temp0, payer, df_temp1, neg, rate, map_rates, col_name, df_temp2



# DRG in HOSPITAL DATA

df_temp0 = df_h.copy()

for payer in payers:
    # payer = 'AETNA'
    df_temp1 = df_temp0[df_temp0.payer == payer]
       
    map_rates = dict( zip ( df_temp1.loc[:,'npi-code'], df_temp1.loc[:,'Amt']  ) )

    col_name = payer + '-hosp_data'
    df_final_drg[col_name] = df_final_drg['hospital-code'].map(map_rates)

del df_temp0, payer, map_rates, col_name



#df_final_drg.columns

df_final_drg = df_final_drg.loc[:,['hospital','procedurecode',
                                   'sum_amt', 'avg_amt','claims',
                                   'AETNA-rate_max-fee', 'AETNA-rate_min-fee','AETNA-rate_avg-fee', 
                                   #'AETNA-rate_max-per diem','AETNA-rate_min-per diem', 'AETNA-rate_avg-per diem',
                                   'UHC-rate_max-fee', 'UHC-rate_min-fee', 'UHC-rate_avg-fee',
                                   #'UHC-rate_max-per diem', 'UHC-rate_min-per diem','UHC-rate_avg-per diem',
                                   'BCBS-rate_max-fee', 'BCBS-rate_min-fee','BCBS-rate_avg-fee',
                                   #'BCBS-rate_max-per diem', 'BCBS-rate_min-per diem','BCBS-rate_avg-per diem',
                                   'CIGNA-rate_max-fee', 'CIGNA-rate_min-fee','CIGNA-rate_avg-fee',
                                   #'CIGNA-rate_max-per diem','CIGNA-rate_min-per diem', 'CIGNA-rate_avg-per diem',
                                   'AETNA-hosp_data','UHC-hosp_data', 'BCBS-hosp_data', 'CIGNA-hosp_data']]

if only_inpatient == True:
    df_final_drg.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\drg_IP_matching.csv',index=False)
else:
    df_final_drg.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\drg_matching.csv',index=False)



###############################################################################

# CPT in PANORAMA DATA


negoc = df_p_cpt.negotiated_type.unique()
rates = ['rate_max','rate_min','rate_avg']

df_temp0 = df_p_cpt.copy()

df_final_cpt = df_c_cpt.copy()


for payer in payers:
    # payer = 'AETNA'
    df_temp1 = df_temp0[df_temp0.payer == payer]
    
    for neg in negoc:
    # neg = 'fee'
    
        df_temp2 = df_temp1[df_temp1.negotiated_type == neg]  
    
        for rate in rates:
        # rate = 'rate_max'
        
            map_rates = dict( zip ( df_temp2.loc[:,'hospital-code'], df_temp2.loc[:,rate] ) )
        
            col_name = payer + '-' + rate + '-' + neg
            df_final_cpt[col_name] = df_final_cpt['hospital-code'].map(map_rates)


del negoc, rates, df_temp0, payer, df_temp1, neg, rate, map_rates, col_name


df_final_cpt.columns


df_final_cpt = df_final_cpt.loc[:,['hospital', 'procedurecode',
                                   'sum_amt', 'avg_amt','claims',
                                   'AETNA-rate_max-fee', 'AETNA-rate_min-fee','AETNA-rate_avg-fee', 
                                   #'AETNA-rate_max-percentage','AETNA-rate_min-percentage', 'AETNA-rate_avg-percentage',
                                   'UHC-rate_max-fee', 'UHC-rate_min-fee', 'UHC-rate_avg-fee',
                                   #'UHC-rate_max-percentage', 'UHC-rate_min-percentage','UHC-rate_avg-percentage',
                                   'BCBS-rate_max-fee', 'BCBS-rate_min-fee','BCBS-rate_avg-fee',
                                   #'BCBS-rate_max-percentage','BCBS-rate_min-percentage', 'BCBS-rate_avg-percentage',
                                   'CIGNA-rate_max-fee', 'CIGNA-rate_min-fee', 'CIGNA-rate_avg-fee',
                                   #'CIGNA-rate_max-percentage', 'CIGNA-rate_min-percentage','CIGNA-rate_avg-percentage'
                                   ]]



# CPT in HOSPITAL DATA

# df_temp0 = df_h.copy()

# for payer in payers:
#     # payer = 'AETNA'
#     df_temp1 = df_temp0[df_temp0.payer == payer]
    
    
#     map_rates = dict( zip ( df_temp1.loc[:,'npi-code'], df_temp1.loc[:,'Amt']  ) )

#     col_name = payer + '-hosp_data'
#     df_final_cpt[col_name] = df_final_cpt['hospital-code'].map(map_rates)


if only_inpatient == True:
    df_final_cpt.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\cpt_IP_matching.csv',index=False)
else:
    df_final_cpt.to_csv(r'C:\Users\JuanChamieQuintero\Documents\Panorama\cpt_matching.csv',index=False)


del match_codes, only_inpatient, payers
###############################################################################
###############################################################################


# sSQL =  '''DESCRIBE mrf.mrf_in_network_rates'''
# df001 = pd.read_sql(sSQL,conn)
# sSQL =  '''DESCRIBE mrf.mrf_provider_references'''
# df002 = pd.read_sql(sSQL,conn)
# sSQL =  '''DESCRIBE allbcbstxev.claims'''
# df003 = pd.read_sql(sSQL,conn)
# sSQL =  '''DESCRIBE southe.claims'''
# df004 = pd.read_sql(sSQL,conn)


###############################################################################
###############################################################################

