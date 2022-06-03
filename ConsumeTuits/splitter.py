'''
This file will split a large CSV files into several files 
classified by month & day
'''

import sys
import os
import pandas as pd

if len(sys.argv) < 3:
    raise ValueError("Please, provide a file URL for the CSV file")

file_url = sys.argv[1]
prefix = sys.argv[2]

print(f"Searching for file at {file_url}")
print(f"File extension is {file_url.split('.')[-1].lower()}")

if file_url.split('.')[-1].lower() != "csv":
    raise ValueError(f"Please, provide a valid CSV file and a prefix name")

if not os.path.isfile(file_url):
    raise ValueError(f"No file found at {file_url}")

file = pd.read_csv(file_url, sep="|", encoding="utf-16")

print("Drop null values...")
file = file.dropna(subset=['created_at'])

# Applying transformation 
print("Applying transformation to the next table, please wait")
print(file.head())

# month encoding
month_encode = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'
}

# searching for all the months

# Transforms Twitter date into month & year only
def transform_date(date_list):
    new_year = [] # list of years in order
    new_month = [] # list of months in order according to [date_list]
    new_days = [] # the name of the final file

    for date in date_list:
        month, day, year = ('', )*3
        splitted = date[0].split(' ')

        if len(splitted) < 2:
            month = ''
            year = ''
            day = ''
        else:
            month = month_encode[splitted[1]]
            year = splitted[-1]
            day = splitted[2]


        new_year.append(year)
        new_month.append(month)
        new_days.append(day)
    
    return new_year, new_month, new_days

print('Parsing dates...')
temp_df = file.copy()

years, months, days = transform_date(temp_df.loc[:, ['created_at']].values)

temp_df['years'] = years
temp_df['months'] = months
temp_df['days'] = days

print('Creating directories')

tmp_year = set(years)
tmp_month = set(months)
tmp_days = set(days)

for year in tmp_year:

    # avoiding empty year
    if year is None or year == "":
        continue

    records = temp_df[temp_df['years'] == year]

    for month in tmp_month:

        # avoiding empty month
        if month is None or month == "":
            continue

        records2 = records[records['months'] == month]

        if len(records2.values) > 0:
            # verifying route
            path = f"./data/{year}/{month}/"
            if not os.path.exists(path):
                os.makedirs(path)

            for i in tmp_days:
                # avoiding empty days 
                if i is None or i == "":
                    continue

                df = records2[records2['days'] == i]

                # do not save empty CSV
                if df.shape[0] == 0:
                    continue

                # removing temporal columns
                df = df.drop(['days', 'years', 'months'], axis=1)

                # Storing the CSV
                df.to_csv(f"{path}{prefix}_{year}_{month}_{i}.csv", sep="|", encoding="utf-16", index=False)

print("\n\nSucceed\n\n")
