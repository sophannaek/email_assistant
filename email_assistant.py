from extract_emails import extract_mails
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import os
import datetime
import sqlite3
import json 
import requests
import pandas as pd

# check if df empty, remove duplicates and empty fields 
def validate_data(df:pd.DataFrame) -> bool: 
    # Check if dataframe is empty
    if df.empty: 
        print("No emails has been added to the list. Finishing exection")
        return False

    # Check if Primary key is unique 
    if pd.Series(df['date']).is_unique:
        pass
    else: 
        print("One or more Primary Keys are not unique")
        # print(df)
        print("Removing the duplicated entries!")
        df.sort_values("date", inplace=True)
        df.drop_duplicates(subset=["date"], inplace = True)
        # print(df)
        # raise Exception("One or more Primary Keys are not unique")
   
    if df.isnull().values.any(): 
        raise Exception("Null values found!")
        
 
    timestamps = df["date"].tolist()
    # check if the email is int the same month 
    today = datetime.datetime.now()
    for timestamp in timestamps:
        # if datetime.datetime.strptime(str(timestamp), '%Y-%m-%d') != yesterday:
        # if timestamp < period:
        if timestamp.strftime("%m") != today.strftime('%m'):
            raise Exception("At least one of the emails is not from this month")

    # check if the data is only from yesterday's data 
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)     
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # for timestamp in timestamps: 
    #     if datetime.datetime.strptime(str(timestamp), '%Y-%m-%d') < yesterday: 
    #         raise Exception("At least one of the email is not recent as of yesterday timestamp")

    return True

# Generate montly report on the 1st day of the month
def generate_excel(): 
    today = datetime.datetime.now() 
    if today.strftime("%d") =="1":
        print("it is the first day of the month")
        #TODO: generate the excel file 
   

if __name__== "__main__":
    # database file directory 
    basedir = os.path.abspath(os.path.dirname(__file__))
    DATABASE_LOCATION = 'sqlite:///' + os.path.join(basedir, 'financial_activities.db')

    # extract financial activities related emails 
    mails_df = extract_mails()
    # print(mails_df)
  
    # check if data is valid 
    if validate_data(mails_df):
        print("Data valid, proceed to Load stage")


    # Load -- store in the sqlite database 
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect('financial_activities.db')
    cursor = connection.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS financial_activities(
        company VARCHAR(200),
        transaction_type VARCHAR(200),
        amount VARCHAR(200),
        description VARCHAR(200),
        date DATETIME,
        CONSTRAINT primary_key_constraint PRIMARY KEY (date)
    )
    """

    cursor.execute(sql_query)
    print("Connect to the database successfully")
    # mails_df.to_sql("financial_activities", con=engine, index=False, if_exists='append')
    df = mails_df
    
    try:
        mails_df.to_sql("financial_activities", con=engine, index=False, if_exists='append')
        print("Data has been added to the database")
    except:
        print("Some data are already exists in the database...")
        act = engine.execute("SELECT * FROM financial_activities")
        print('df: ', mails_df)
        print("******inside the table********")
        emails = act.fetchall()
        timestamps = []
        for email in emails: 
            # print(email['date'])
            timestamps.append(email['date'][:19])
        for index, row in df.iterrows():
            if str(row['date'])[:20] in timestamps:
                # remove this email -- already in the database 
                df = df.drop(index, axis=0, inplace=False)
        print("df..",df)
        try: 
            df.to_sql("financial_activities", con=engine, index=False, if_exists='append')
            print("Data has been added to the database")
        except: 
            print("There are some issues when trying to add data into the database ")

    
    connection.close()
    print("Close database successfully")

    generate_excel()



# TODO: schedule to extract every day -- transform - load daily -- using Airflow 
# TODO: generate a excel file monthly -- 1st day of the month
# TODO: create a pie chart corresponding to financial activities monthly 

