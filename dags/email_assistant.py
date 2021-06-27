from extract_transform_emails import extract_mails
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import os
import datetime
import sqlite3
import json 
import requests
import pandas as pd



# database file directory 
basedir = os.path.abspath(os.path.dirname(__file__))
DATABASE_LOCATION = 'sqlite:///' + os.path.join(basedir, 'financial_activities.db')


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
        print("Removing the duplicated entries!")
        df.sort_values("date", inplace=True)
        df.drop_duplicates(subset=["date"], inplace = True)
   
    if df.isnull().values.any(): 
        raise Exception("Null values found!")
        
    timestamps = df["date"].tolist()
    # check if the email is int the same month 
    today = datetime.datetime.now()
    for timestamp in timestamps:
        if timestamp.strftime("%m") != today.strftime('%m'):
            raise Exception("At least one of the emails is not from this month")

    return True

# Generate montly report on the 1st day of the month
def generate_excel(): 
    today = datetime.datetime.now() 
    day = today.strftime('%Y-%m')
    month = today.strftime('%m')
    if today.strftime("%d") != "1":
        print('Generating report!')  
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        connection = sqlite3.connect('financial_activities.db')

        # aggregate the data -- group by transaction type and sum the amount of money 
        query = engine.execute("SELECT transaction_type, SUM(amount), COUNT(transaction_type) \
                                FROM financial_activities \
                                WHERE strftime('%m',date)=? \
                                GROUP BY transaction_type",  (month,))
        report = query.fetchall()
        df = pd.DataFrame(report, columns=['transaction_type', 'amount','number of transaction'])
        report_name= str(day)+'-report.csv'
        print('report' ,df)
        df.to_csv(report_name,encoding='utf-8',index=False)


# load extracted and transformed data to the database
def load(mails_df): 
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
            timestamps.append(email['date'][:19])
        for index, row in df.iterrows():
            if str(row['date'])[:20] in timestamps:
                print("date..",str(row['date'])[:20])
                # remove this email -- already in the database 
                df = df.drop(index, axis=0, inplace=False)
        try: 
            df.to_sql("financial_activities", con=engine, index=False, if_exists='append')
            print("Data has been added to the database")
        except: 
            print("There are some issues when trying to add data into the database ")
    
    connection.close()
    print("Close database successfully")
