import pandas as pd
import random
from datetime import timedelta

##############################################################
# Sample records creation

num_people=20
max_visits=5
start_date='2020-01-01'
end_date='2020-03-31'
  
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)
  
# List to keep records
records = []
  
# Generate data for each person
for person_id in range(1, num_people + 1):
# Random number of visits for each person
  num_visits = random.randint(1, max_visits)
  
  # Generate random dates for each visit
  visit_dates = sorted([start_date + timedelta(days=random.randint(0,
  (end_date - start_date).days)) for _ in range(num_visits)])
  
  # Create records for each visit
  records.extend([(person_id, date) for date in visit_dates])
  
  # Create DataFrame from records
  df = pd.DataFrame(records, columns=['person_id', 'date']).drop_duplicates()


# df.head(10) 


##############################################################
##############################################################
# Finding the events!!!

# Function to process each person's visits
def process_visits(visits):
  # List to store eligibility for each event
  eligible_for_incentive = [True]
  # Keep track of the last incentivized event date
  last_incentivized_visit = visits.iloc[0]['date']
  
  for i in range(1, len(visits)):
    days_since_last_incentive = (visits.iloc[i]['date'] -
    last_incentivized_visit).days
    if days_since_last_incentive >= 30:
      eligible_for_incentive.append(True)
      last_incentivized_visit = visits.iloc[i]['date']
    else:
      eligible_for_incentive.append(False)
  
  # Assign the list to the DataFrame
  visits['eligible_for_incentive'] = eligible_for_incentive
  return visits


# Ensure the dates are in datetime format and sort the DataFrame
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=['person_id', 'date'])


# Apply the function to each person_id
df_final = df.groupby('person_id').apply(process_visits).reset_index(drop=True)


df_final.head(10) # Display the first 10 rows of the sample data
