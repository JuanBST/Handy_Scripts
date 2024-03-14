import requests
from bs4 import BeautifulSoup
import pandas as pd

# non-billable:
#sURL = "https://www.icd10data.com/ICD10CM/Codes/Rules/Non_Billable_Specific_Codes/"
# billable:
sURL = "https://www.icd10data.com/ICD10CM/Codes/Rules/Billable_Specific_Codes/"
pages = 737 # 237 non-billable, 737 billable

dfAll = pd.DataFrame()

for j in range(1,pages,1):
    # Send a request to the website
    r = requests.get(sURL+str(j) )
    # Parse the HTML content
    soup = BeautifulSoup(r.text, 'html.parser')

    data = []
    for x in soup.find("ul",class_=None):
        if x.text != '\n':
            a = x.text
            icd_code = a[0:a.find(' ') ]
            icd_desc = a[ a.find(' '): ]
            data.append({'icd_code':icd_code,'icd_desc':icd_desc})
    df = pd.DataFrame(data)
    dfAll = pd.concat([dfAll, df], axis=0)


# Final cleanup
dfAll.icd_code = dfAll.icd_code.str.strip()
dfAll.icd_desc = dfAll.icd_desc.str.strip()