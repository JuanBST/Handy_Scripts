# To run in EMR -> 
# Load the script into S3
# Open and login into EMR
# Run: spark-submit s3://com.bstis.<s3 path>/<py file with the script>
# or spark-submit https://raw.githubusercontent.com/<user>/<repo>/<branch>/<path_to_python_script>

from pyspark.sql import SparkSession
import pandas as pd 
from pyspark.sql import SQLContext
from datetime import date, timedelta
import time


def custom_date(current, day_of_the_month, months_difference):
    # Validate the day_of_the_month input
    if day_of_the_month not in ['first', 'last']:
        raise ValueError("day_of_the_month should be either 'first' or 'last'")
    # Extract the month and year values
    month = int(current[:2])
    year = int(current[2:])
    # Create a date object
    given_date = date(year, month, 1)
    # Compute the target month and year
    target_month = (month - 1 + months_difference) % 12 + 1
    target_year = year + (month - 1 + months_difference) // 12
    # If the day of the month is the last, then compute the last day of the target month
    if day_of_the_month == 'last':
        # If target_month is December, then next_month is January of the next year
        if target_month == 12:
            next_month_first_day = date(target_year + 1, 1, 1)
        else:
            next_month_first_day = date(target_year, target_month + 1, 1)
        return next_month_first_day - timedelta(days=1)
    # If the day of the month is the first, then simply return the first day of the target month
    return str(date(target_year, target_month, 1))


period = '012024'
tenants = ['trinet','vba_prod','swairc','labcor','jbsppc','halliburton','ochsne','basfco',
               'wastec','energytransfer','oshkosh','thorind','goodma','nvgmines','danalim',
               'newellb','bright','gbait','voya','nhcc'] 

start = custom_date(period, 'first', -12)
end = custom_date(period, 'last', 0)
current = custom_date(period, 'first', 0)

def diab_icds():

    def get_query(tenant):
        # To get all ICDs in the category.
        return '''
        SELECT
            '{current}' as month,
            CASE WHEN icd9_1_category = 'Diabetes mellitus without complication' THEN 0 ELSE 1 END with_complication,
            icd9_1
        FROM hive.{tenant}.claims
            WHERE
            icd9_1_category IN ( 'Diabetes mellitus with complication' , 'Acute and unspecified renal failure'  , 'Diabetes mellitus without complication' )
            and dosstart between date'{start}' and date'{end}'
            group by 1,2,3'''.format(tenant=tenant,start=start,end=end,current=current)


    query_list = [get_query(tenant) for tenant in tenants]
    final_query = " UNION ALL ".join(query_list)
    final_query = "select * from (" + final_query + ") group by 1,2,3"

    return final_query

    

def sc_1():

    def get_query(tenant):
        return '''
        SELECT 
            {current} as month,
            c.personid,
            c.tenantid, 
            COUNT ( distinct  COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid ,'') ) as sc1_diabcomp_claims,
            MAX(CASE WHEN LENGTH(c.er_visitid) > 0 THEN 1 ELSE 0 END) as sc1_diabcomp_er,
            MAX(CASE WHEN LENGTH(c.inpatient_visitid) > 0 THEN 1 ELSE 0 END) as sc1_diabcomp_hosp,
            sum(COALESCE(amtallowed,0)) sc1_diabcomp_allowed
        FROM {tenant}.claims c 
        JOIN
            (
            SELECT
                inpatient_visitid,
                er_visitid
            FROM {tenant}.claims c
            JOIN  
                smartcards.diab_icd d
            ON
                d.icd9_1 = c.icd9_1
            WHERE
                ( c.inpatient_visitid <> ''
                OR c.er_indicator IN ('ER', 'ER,Emergency','ER,Non-Emergency' ))
                AND dosstart between date'{start}' and date'{end}
                --AND status = 'ACTIVE'
            GROUP BY
                1,2,3
            ) f
        ON
            COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid,'')  = COALESCE(f.inpatient_visitid,'') || COALESCE(f.er_visitid,'')
            AND ( COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid,'')) <> ''
        WHERE
            dosstart between date'{start}' and date'{end}
        GROUP BY 
            1'''.format(tenant=tenant,start=start,end=end,current=current)

    query_list = [get_query(tenant) for tenant in tenants]
    final_query = " UNION ALL ".join(query_list)

    return final_query



def get_query(tenant, from_date, to_date):
    return '''
  with tmp_var as
    (
      SELECT 
          CAST('{start}' AS DATE) AS StartDate,
          CAST('{end}' AS DATE) AS EndDate,
          CAST('{tenant_period}' AS DATE) AS LastMonth,
          CAST(50000 AS INT) AS HHC_limit
    ),

  -- Diabetes Adverse Events Opportunity    diabetes-related ER and inpatient visits
  diab_icd  as
    (
      SELECT
        CASE WHEN icd9_1_category = 'Diabetes mellitus without complication' THEN 0 ELSE 1 END with_complication,
        icd9_1
      FROM {cluster}.claims
        WHERE
        icd9_1_category IN ( 'Diabetes mellitus with complication' , 'Acute and unspecified renal failure'  , 'Diabetes mellitus without complication' )
        and dosstart between date'{start}' and date'{end}'
        group by 1,2
    ),

  -- ER and Inpatient visits Diabetic related.
  sc1_diabcomp  as
    (
      SELECT 
        c.personid,
        --c.company_tag, 
        --c.division,
        --c.division3,

        COUNT ( distinct  COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid ,'') ) as sc1_diabcomp_claims,
        MAX(CASE WHEN LENGTH(c.er_visitid) > 0 THEN 1 ELSE 0 END) as sc1_diabcomp_er,
        MAX(CASE WHEN LENGTH(c.inpatient_visitid) > 0 THEN 1 ELSE 0 END) as sc1_diabcomp_hosp,
        sum(COALESCE(amtallowed,0)) sc1_diabcomp_allowed
      FROM {cluster}.claims c 
      JOIN
        (
          SELECT
            inpatient_visitid,
            er_visitid
          FROM {cluster}.claims c
          JOIN  
            diab_icd d
          ON
            d.icd9_1 = c.icd9_1
          WHERE
            ( c.inpatient_visitid <> ''
              OR c.er_indicator IN ('ER', 'ER,Emergency','ER,Non-Emergency' ))
            AND dosstart between date'{start}' and date'{end}
            --AND status = 'ACTIVE'
          GROUP BY
            1,2
        ) f
      ON
        COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid,'')  = COALESCE(f.inpatient_visitid,'') || COALESCE(f.er_visitid,'')
        AND ( COALESCE(c.inpatient_visitid,'') || COALESCE(c.er_visitid,'')) <> ''
      WHERE
        dosstart between date'{start}' and date'{end}
      GROUP BY 
        1
    ),
    
  -- 2 Brand-to-Generic Drug Opportunity    brand w/ generic spend
  sc2_b2g  as
    (
      WITH y_cross AS
        (
        SELECT 
        SUBSTR(therapeutic_gpi_code,1,8) as gpi, 
        sum(CAST(reversalstatus AS INTEGER)) as n, 
        SUM(amtallowed) as amt,
        sum(dayssupply) as days, 
        sum(amtallowed)/sum(dayssupply) as y_per_day
        FROM {cluster}.claims
        where upper(multi_source_code) = 'Y'

        and dosstart between date'{start}' and date'{end}
        group by 1
        having 
        sum(dayssupply) > 0 
        and sum(CAST(reversalstatus AS INTEGER)) > 0 
        and sum(amtallowed) > 0
        )

      SELECT    
      personid,
      --c.company_tag, 
      --division, 
      --division3, 
      SUM(o_claims1-y_claims1) as sc2_b2g_savings,
      SUM(claims) as sc2_b2g_claims
      FROM
        (
          SELECT 
            --c.company_tag, 
            c.personid,
            --c.division, 
            --c.division3, 
            SUBSTR(c.therapeutic_gpi_code,1,8) as therap,
            count(distinct tpaclaimid) as claims,
            sum(COALESCE(c.amtallowed,0)) o_claims1,
            sum(dayssupply) as totaldays,
            sum(CASE WHEN COALESCE(c.amtallowed,0) > y_per_day*dayssupply THEN y_per_day*dayssupply ELSE 0 END) as y_claims1
          FROM {cluster}.claims c
            left join y_cross y on y.gpi = SUBSTR(c.therapeutic_gpi_code,1,8)
          WHERE UPPER(multi_source_code) IN ('O')
            AND UPPER(formularyindicator) IN ('Y','1')
            AND c.dosstart between date'{start}' and date'{end}
        
          GROUP BY 1,2--,3,4
          --Having 
          --totaldays > 0 and o_claims1 > 0
        )    
      GROUP BY 1--,2,3
    ),
    
  -- 3 Substitutable Drugs Opportunity  fill rate of substitutable drugs
  sc3_substit  as
    (
      SELECT
        c.personid,
        --
        --c.division,
        --c.division3,  
        sum(c.amtallowed) as sc3_subsit_allowed,
        count(distinct tpaclaimid) as sc3_substit_claims
      FROM {cluster}.claims c
      WHERE
      UPPER(ndc_name) IN ('OLMESARTAN-HYDROCHLOROTHIAZIDE', 'SOLODYN', 'BENICAR', 'DESVENLAFAXINE SUCCINATE ER', 
            'OLOPATADINE HCL', 'BENICAR HCT', 'VYTORIN', 'ZIANA', 'DUEXIS', 'LOESTRIN FE', 'CELEBREX', 'OLMESARTAN MEDOXOMIL-HCTZ',
            'OLMESARTAN-AMLODIPINE-HCTZ', 'ZAFIRLUKAST', 'BYSTOLIC', 'DEXILANT', 'LO LOESTRIN FE', 'TREXIMET', 'DUAC', 'CONTRAVE', 
            'OLMESARTAN MEDOXOMIL', 'DESVENLAFAXINE ER', 'DUAC CS', 'ACANYA', 'VIMOVO', 'BENZACLIN', 'BEYAZ', 'DUEXIS')
        AND claimtype = 'Rx'
        AND c.dosstart between date'{start}' and date'{end}
    
        -- AND c.status <> 'TERMED' or c.status is null
      GROUP BY 1--,2,3
    ),
    
  -- 4 Risk Score Opportunity   prevalence of high-risk members
  sc4_rscore as
    (
      SELECT 
      e.personid,
      --
      --e.division,
      --e.division3,
      MIN(CASE WHEN e.riskscore > 3 THEN 1 ELSE 0 END) AS sc4_rs_over3,
      MIN(CASE WHEN e.riskscore = 3 THEN 1 ELSE 0 END) AS sc4_rs_3
      FROM {cluster}.elig e
      JOIN
        (
          SELECT 
          personid,
          --
          --division, 
          --division3,
          --riskscore,
          MIN(month) as month
          FROM {cluster}.elig
          WHERE 
          riskscore IS NOT NULL
          -- and status = 'ACTIVE'
          AND month between date'{start}' and date'{end}
      
          GROUP BY 1--,2,3
        ) e2
      ON
        e.personid = e2.personid AND
        --
        --e.division = e2.division AND
        --e.division3 = e2.division3 AND
        e.month = e2.month
      WHERE
      e.month between date'{start}' and date'{end}

      -- and c.status = 'ACTIVE'
      GROUP BY 1--,2,3
    ),
    
  -- 5 HCC Claims Audit Opportunity catastrophic claimant spend
  sc5_catast as
    (
      SELECT 
      c.personid,
      --
      --c.division,
      --c.division3,
      1 as sc5_is_catastrophic,
      sum(c.amtallowed) AS sc5_catast_allowed,
      COUNT(DISTINCT c.tpaclaimid) as sc5_catast_claims
      FROM {cluster}.claims c
      JOIN
      sc4_rscore r
      ON
      r.personid = c.personid
      --
      --and r.division = c.division
      --and r.division3 = c.division3
      WHERE
      c.dosstart between date'{start}' and date'{end}
      and r.sc4_rs_over3 = 0

      GROUP BY 1--,2,3
      HAVING COUNT(DISTINCT c.tpaclaimid) > (SELECT HHC_limit FROM tmp_var)
    ),
    
  -- 6 ER Utilization Opportunity   ER visit rate per 1,000 among members w/out CCs
  sc6_er_nocc  as
    (
      SELECT 
        c.personid,
        --
        --c.division,
        --c.division3,
        COUNT ( DISTINCT c.er_visitid ) as sc6_er_nocc_visits,
        sum(COALESCE(amtallowed,0)) sc6_er_nocc_allowed
      FROM {cluster}.claims c 
      WHERE
        dosstart between date'{start}' and date'{end}
        and er_indicator IN ('ER', 'ER,Emergency','ER,Non-Emergency' )
        and chronic_indicator not in ('C','C/A')
      --  and status = 'ACTIVE'
      GROUP BY 
        1--,2,3
    ),

  -- 7 OON
  sc7_oon  as
    (
      SELECT 
        c.personid,
        --
        --c.division,
        --c.division3,
        COUNT(DISTINCT CASE WHEN innetworkflag <> 1 THEN CONCAT(c.personid, CAST(c.dosstart AS VARCHAR)) ELSE NULL END) AS sc7_oon_days,
        sum(case when innetworkflag <> 1 then amtallowed else 0 end) as sc7_OONallowed,
        sum(amtcoins + amtcopay + amtdeductible) as sc7_OOP
      FROM {cluster}.claims c 
      WHERE
        dosstart between date'{start}' and date'{end}
      --  and status = 'ACTIVE'
      GROUP BY 
        1--,2,3
    ),

  -- 8 MSK - IS INCLUDED IN CCMAP CONDITIONS TABLE

  -- CCMAP CONDITIONS
  ccmap_comb  as
    (
      SELECT 
      personid,
      --
      --division,
      --division3,
      MAX(CASE WHEN cconditionname = 'Glaucoma' THEN 1 ELSE 0 END) as CGlaucoma,
      MAX(CASE WHEN cconditionname = 'Rheumatoid Arthritis/Osteoarthritis' THEN 1 ELSE 0 END) as CArtri,
      MAX(CASE WHEN cconditionname = 'ADHD Conduct Disorders and Hyperkinetic Syndrome' THEN 1 ELSE 0 END) as CAdd,
      MAX(CASE WHEN cconditionname in ('Lung Cancer','Leukemias and Lymphomas','Prostate Cancer','Colorectal Cancer','Breast Cancer',
            'Liver Disease Cirrhosis and Other Liver Conditions (except Viral Hepatitis)','Endometrial Cancer') THEN 1 ELSE 0 END) as CCancer,
      MAX(CASE WHEN cconditionname in ('Ischemic Heart Disease', 'Acute Myocardial Infarction', 'Stroke / Transient Ischemic Attack',
            'Atrial Fibrillation','Heart Failure') THEN 1 ELSE 0 END) as CHeart,
      MAX(CASE WHEN cconditionname in ('Hepatitis A', 'Hepatitis C (acute)', 'Hepatitis B (chronic)', 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)',
            'Liver Disease  Cirrhosis and Other Liver Conditions (except Viral Hepatitis)', 'Hepatitis C (chronic)', 'Viral Hepatitis (General)',
            'Hepatitis B (acute or unspecified)') THEN 1 ELSE 0 END) as CViralInf,
      MAX(CASE WHEN cconditionname in ('Hip/Pelvic Fracture', 'Mobility Impairments', 'Pressure and Chronic Ulcers') THEN 1 ELSE 0 END) as CMSKplus,
      MAX(CASE WHEN cconditionname in ('Epilepsy', 'Multiple Sclerosis and Transverse Myelitis') THEN 1 ELSE 0 END) as CNervSystem,
      MAX(CASE WHEN cconditionname in ('Chronic Kidney Disease') THEN 1 ELSE 0 END) as CKidney,
      MAX(CASE WHEN cconditionname in ('Anxiety Disorders', 'Depression', 'Depressive Disorders') THEN 1 ELSE 0 END) as CDepress,
      CASE WHEN 
          (
            MAX(CASE WHEN cconditionname = 'Hyperlipidemia' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN cconditionname = 'Hypertension' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN cconditionname = 'Obesity' THEN 1 ELSE 0 END)
          ) > 2 THEN 1 ELSE 0 END as CUnfit,
      MAX(CASE WHEN cconditionname = 'Diabetes' THEN 1 ELSE 0 END) as CDiabetes,
      SUM(CASE WHEN cconditionname in ('Hip/Pelvic Fracture', 'Mobility Impairments') THEN condition_claims_allowed ELSE 0 END) as sc8_mskallowed
      FROM {cluster}.ccmap
      WHERE
      ccondition_indicator in ('Chronic','Extended Duration') 
      and date between date'{start}' and date'{end}

      GROUP BY 1--,2,3
    ),

  -----------------------------------------------------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------------------------------------------------
  -- FINAL TABLE
  claims2 as
    ( 
      SELECT
      c.personid,
      --
      --c.division,
      --c.division3,
      --sum(c.amtpaid) as amtpaid,
      --sum(CASE WHEN c.claimtype = 'MED' THEN c.amtpaid ELSE 0 END) as amtpaidMED,
      --sum(CASE WHEN c.claimtype = 'Rx' THEN c.amtpaid ELSE 0 END) as amtpaidRx,
      sum(c.amtallowed) as amtallowed,
      sum(CASE WHEN c.claimtype = 'MED' THEN c.amtallowed ELSE 0 END) as amtallowedMED,
      sum(CASE WHEN c.claimtype = 'Rx' THEN c.amtallowed ELSE 0 END) as amtallowedRx
      FROM {cluster}.claims c
      WHERE
      c.dosstart between date'{start}' and date'{end}
      AND (c.status <> 'TERMED' or c.status is null)
      Group by 1--,2,3
    ),
  elig2 as
    (
      SELECT
      e.personid,
      --
      --e.division,
      --e.division3,
      max(CASE WHEN e.gender = 'F' THEN 'F' else 'M' end) as gender,   
      max(e.age) as age,
      max(e.personstate) as personstate,
      max(e.relation) as relation,  
      count( distinct e.month) as months
      FROM {cluster}.elig e
      WHERE
      e.month between date'{start}' and date'{end}
      and (e.status <> 'TERMED' or e.status is null)
      Group by 1--,2,3
    )


  SELECT 
    '{tenant_period}' as sc_month,
    e.personid,
    --
    --e.division,
    --e.division3,
    e.gender,   
    e.age,
    e.personstate,
    e.relation,  
    e.months,
    cc.CGlaucoma,
    cc.CArtri,
    cc.CAdd,
    cc.CCancer,
    cc.CHeart,
    cc.CViralInf,
    cc.CMSKplus,
    cc.CNervSystem,
    cc.CKidney,
    cc.CDepress,
    cc.CUnfit,
    cc.CDiabetes,
    sc1_diabcomp_claims,
    sc1_diabcomp_er,
    sc1_diabcomp_hosp,
    sc1_diabcomp_allowed,
    sc2_b2g_savings,
    sc2_b2g_claims,
    sc3_subsit_allowed,
    sc3_substit_claims,
    sc4_rs_over3,
    sc4_rs_3,
    sc5_catast_allowed,
    sc5_catast_claims,
    sc6_er_nocc_visits,
    sc6_er_nocc_allowed,
    sc7_OON_days,
    sc7_OONallowed,
    sc7_OOP,
    cc.sc8_mskallowed,
    c.amtallowed,
    c.amtallowedMED,
    c.amtallowedRx
  FROM elig2 e

  LEFT JOIN ccmap_comb cc
  ON e.personid = cc.personid

  --and e.division = cc.division
  --and e.division3 = cc.division3

  LEFT JOIN claims2 c
  ON c.personid = e.personid
  --
  --and c.division = e.division
  --and c.division3 = e.division3

  LEFT JOIN sc1_diabcomp sc1
  ON sc1.personid = e.personid 
  --
  --and sc1.division = e.division
  --and sc1.division3 = e.division3

  LEFT JOIN sc2_b2g sc2
  ON sc2.personid = e.personid 
  --
  --and sc2.division = e.division
  --and sc2.division3 = e.division3

  LEFT JOIN sc3_substit sc3
  ON sc3.personid = e.personid 
  --
  --and sc3.division = e.division
  --and sc3.division3 = e.division3

  LEFT JOIN sc4_rscore sc4
  ON sc4.personid = e.personid 
  --
  --and sc4.division = e.division
  --and sc4.division3 = e.division3

  LEFT JOIN sc5_catast sc5
  ON sc5.personid = e.personid 
  --
  --and sc5.division = e.division
  --and sc5.division3 = e.division3

  LEFT JOIN sc6_er_nocc sc6
  ON sc6.personid = e.personid 
  --
  --and sc6.division = e.division
  --and sc6.division3 = e.division3

  LEFT JOIN sc7_oon sc7
  ON sc7.personid = e.personid 
  --
  --and sc7.division = e.division
  --and sc7.division3 = e.division3

  -- LEFT JOIN company_info ci
  -- ON ci.division = e.division

  -- WHERE
  -- (e.status <> 'TERMED' or c.status is null)

  GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40

  '''.format(cluster=cluster,start=start,end=end,tenant_period=tenant_period)

        '''.format(tenant=tenant, from_date=from_date, to_date=to_date)


def custom_date(current, day_of_the_month, months_difference):
    # Validate the day_of_the_month input
    if day_of_the_month not in ['first', 'last']:
        raise ValueError("day_of_the_month should be either 'first' or 'last'")
    # Extract the month and year values
    month = int(current[:2])
    year = int(current[2:])
    # Create a date object
    given_date = date(year, month, 1)
    # Compute the target month and year
    target_month = (month - 1 + months_difference) % 12 + 1
    target_year = year + (month - 1 + months_difference) // 12
    # If the day of the month is the last, then compute the last day of the target month
    if day_of_the_month == 'last':
        # If target_month is December, then next_month is January of the next year
        if target_month == 12:
            next_month_first_day = date(target_year + 1, 1, 1)
        else:
            next_month_first_day = date(target_year, target_month + 1, 1)
        return next_month_first_day - timedelta(days=1)
    # If the day of the month is the first, then simply return the first day of the target month
    return str(date(target_year, target_month, 1))


if __name__ == "__main__":

    start_time = time.time()

    tenants = ['trinet','vba_prod','swairc','labcor','jbsppc','halliburton','ochsne','basfco',
               'wastec','energytransfer','oshkosh','thorind','goodma','nvgmines','danalim',
               'newellb','bright','gbait','voya','nhcc'] 
       
    period = '012024'
    from_date = custom_date(period, 'first', -6)
    to_date = custom_date(period, 'last', 0)

    spark = SparkSession.builder \
        .appName("Hive data reader") \
        .enableHiveSupport() \
        .getOrCreate()

    query_list = [get_query(tenant, from_date, to_date) for tenant in tenants]
    final_query = " UNION ALL ".join(query_list)

    data_elig = spark.sql(final_query)

    data_elig.write.mode("overwrite").parquet("s3a://com.bstis.colossus/bstunified_ds/validation_elig_ccmap/")

    # data_elig = spark.read.csv("s3a://com.bstis.tpafiles/DM_transfer_files/Juan/cccode1.csv", header=True, inferSchema=True)
    # data_elig.show()
    # data_elig.write.mode("overwrite").csv("s3a://com.bstis.tpafiles/DM_transfer_files/Juan/test_data2.csv", header=True)

    # Creating SQL context object    
    sqlContext = SQLContext(spark)

    # Define SQL command
 
    # Define DROP TABLE SQL command
    sql_drop_cmd = "DROP TABLE IF EXISTS unified_ds.validation_elig_ccmap;"
    sqlContext.sql(sql_drop_cmd)
    
    # Define CREATE TABLE SQL command
    sql_create_cmd = '''
  
    sqlContext.sql(sql_create_cmd)


    spark.stop()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")




from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Hive data reader") \
    .enableHiveSupport() \
    .getOrCreate()


########################################################################################################################

final_query = diab_icds()
data_elig = spark.sql(final_query)
data_elig.write.mode("overwrite").parquet("s3a://com.bstis.colossus/bstunified_ds/diab_icds/")

sqlContext = SQLContext(spark)
 
# Define DROP TABLE SQL command
sql_drop_cmd = "DROP TABLE IF EXISTS unified_ds.diab_icds;"
sqlContext.sql(sql_drop_cmd)

# Define CREATE TABLE SQL command
sql_create_cmd = '''
CREATE EXTERNAL TABLE unified_ds.diab_icds (
    with_complication STRING,
    icd9_1 STRING
)
PARTITIONED BY (month date)
STORED AS PARQUET
LOCATION 's3://com.bstis.colossus/bstunified_ds/diab_icds';
'''
sqlContext.sql(sql_create_cmd)

########################################################################################################################

final_query = sc_1()
data_elig = spark.sql(final_query)
data_elig.write.mode("overwrite").parquet("s3a://com.bstis.colossus/bstunified_ds/sc_1/")

sqlContext = SQLContext(spark)
 
# Define DROP TABLE SQL command
sql_drop_cmd = "DROP TABLE IF EXISTS unified_ds.sc_1;"
sqlContext.sql(sql_drop_cmd)

# Define CREATE TABLE SQL command
sql_create_cmd = '''
CREATE EXTERNAL TABLE unified_ds.sc_1 (
    personid STRING,
    tenantid STRING,
    sc1_diabcomp_claims DOUBLE,
    sc1_diabcomp_er BOOLEAN,
    sc1_diabcomp_hosp BOOLEAN,
    sc1_diabcomp_allowed DOUBLE
)
PARTITIONED BY (month date)
STORED AS PARQUET
LOCATION 's3://com.bstis.colossus/bstunified_ds/sc_1';
'''
sqlContext.sql(sql_create_cmd)




