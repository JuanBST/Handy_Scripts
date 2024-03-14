with
    total_amt_allowed1 as (
        select
            personid,
            sum(amtallowed) as total_amt_allowed1
        from 
            bcbstx_nonev_prod.claims
        where 
            tenantid = 'nonev_238000'
            and dosstart between date '2022-01-01' and date '2022-12-31'
        group by 
            personid
    ),
    total_amt_allowed2_and_claim_count as (
        select
            personid,
            icd9_1_chapter,
            sum(amtallowed) as total_amt_allowed2,
            count(distinct tpaclaimid) as distinct_tpa_claim_count
        from 
            bcbstx_nonev.claims 
        where 
            tenantid = 'nonev_238000'
            and dosstart between date '2022-01-01' and date '2022-12-31'
        group by 
            personid,
            icd9_1_chapter
    )

select 
    a2.personid,
    a2.icd9_1_chapter,
    a1.total_amt_allowed1,
    a2.total_amt_allowed2,
    rank() over (partition by a2.personid order by a2.total_amt_allowed2 desc) as rank,
    a2.distinct_tpa_claim_count
from 
    total_amt_allowed1 a1
join
    total_amt_allowed2_and_claim_count a2 on a1.personid = a2.personid
order by 
    a2.personid,
    rank;


SELECT 
personid,
case when date < date '2022-01-01' then 0
  else 1 end as year,
sum(case when CCondition_Category in ('Cancer') 
  then condition_claims else 0 end) as cancer,
sum(case when CCondition_Category in ('Heart Disease','Stroke/Transient Ischemic Attack') 
  then condition_claims else 0 end) as cardiac,
sum(case when CCondition_Category in ('Chronic Kidney Disease')
  then condition_claims else 0 end) as ckd,
sum(case when CCondition_Category in ('Lung Disease')
  then condition_claims else 0 end) as lung,
sum(case when CCondition_Category in ('ADHD Conduct Disorders and Hyperkinetic Syndrome','Alzheimer''s Disease and Related Disorders or Senile Dementia','Autism Spectrum Disorders',
    'Cerebral Palsy','Drug and Substance Use','Epilepsy','Intellectual Disabilities and Related Conditions','Learning Disabilities','Mental Health','Migraine',
    'Multiple Sclerosis and Transverse Myelitis','Muscular Dystrophy','Other Developmental Delays') 
  then condition_claims else 0 end) as mental,  
sum(case when CCondition_Category in ('Diabetes') 
  then condition_claims else 0 end) as diabetes,
sum(case when CCondition_Category in ('Acquired Hypothyroidism','Cystic Fibrosis and Other Metabolic Developmental Disorders','Diabetes','Hyperlipidemia',
    'Liver Disease','Obesity','Hypertension') 
  then condition_claims else 0 end) as metabolic,
sum(case when CCondition_Category in ('Chronic Low Back Pain','Fibromyalgia Chronic Pain and Fatigue','Hip/Pelvic Fracture','Mobility','Osteoporosis',
      'RA/OA (Rheumatoid Arthritis/Osteoarthritis)','Traumatic Brain Injury and Nonpsychotic Mental Disorders due to Brain Damage') 
  then condition_claims else 0 end) as msk,
sum(condition_claims) as ccond

FROM bcbstx_nonev_prod.ccmap 
WHERE tenantid = 'nonev_238000'
and date between date '2021-01-01' and date '2022-12-31'
--and personid = '644582981'
group by
  1,2
having 
  sum(condition_claims) > 10000
--Limit 1000
;

select
    --personid,
    icd9_1_chapter,
    sum(amtallowed) as total_amt_allowed2,
    count(distinct tpaclaimid) as distinct_tpa_claim_count
from 
    bcbstx_nonev_prod.claims 
where 
    tenantid = 'nonev_238000'
    and dosstart between date '2022-01-01' and date '2022-12-31'
group by 
    --personid,
    icd9_1_chapter
    ;
    

WITH classified_claims AS (
    SELECT 
        personid,
        claim,
        amt,
        CASE
            WHEN icd9_1_chapter = 'Certain conditions originating in the perinatal period' THEN 'ER'
            WHEN icd9_1_chapter = 'Certain infectious and parasitic diseases'  THEN 'IP'
            -- add more conditions here
            ELSE NULL
        END AS service,
        CASE
            WHEN condition3 THEN 'ER'
            WHEN condition4 THEN 'IP'
            WHEN condition5 THEN 'Office'
            WHEN condition5 THEN 'OP_Surg'
            ELSE NULL
        END AS chapter
    FROM claims
)
SELECT 
    id, 
    SUM(CASE WHEN service = 'ER' AND chapter = 'A' THEN amt ELSE 0 END) AS "ER_A_Total",
    COUNT(DISTINCT CASE WHEN service = 'ER' AND chapter = 'A' THEN claim ELSE NULL END) AS "ER_A_Count",
    -- repeat for each service/chapter combination
FROM classified_claims
GROUP BY id;

select
    --icd9_1_chapter,
    substr(icd9_1,1,3) as icd_1,
    sum(amtallowed) as allowed
from 
   bcbstx_nonev_prod.claims 
where 
  tenantid = 'nonev_238000'
group by
  1
  ;
  
SELECT 
    id, 
    SUM(CASE WHEN service = 'ER' AND chapter = 'A' THEN amt ELSE 0 END) AS "ER_A_Total",
    COUNT(DISTINCT CASE WHEN service = 'ER' AND chapter = 'A' THEN claim ELSE NULL END) AS "ER_A_Count",
    -- repeat for each service/chapter combination
FROM classified_claims
GROUP BY id;

select *
from 
   bcbstx_nonev_prod.claims 
where 
  tenantid = 'nonev_238000'
  and substr(icd9_1,1,3) = '780'
limit 10
