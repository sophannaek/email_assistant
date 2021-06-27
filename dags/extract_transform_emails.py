import imaplib
import email
from email.header import decode_header
import webbrowser
import os
import html2text
import re
from text_summary import text_summarization
from nltk.tokenize import word_tokenize, sent_tokenize
import pandas as pd
import datetime
from decouple import config
import string

# # account credentials
USERNAME = config('USERNAME')
PASSWORD = config('PASSWORD')

h = html2text.HTML2Text()
h.ignore_links = True


def getCompany(text):
    # remove the link <>
    bracket = text.find('<', 0, len(text))
    if bracket > 0: 
        text = text[0:bracket]
    
    return text

# normalize the text content
def normalize_text(text):
    # Remove non-ASCII chars.
    text = re.sub('[^\x00-\x7F]+',' ', text)
    # Remove URLs
    text = re.sub('https?:\/\/.*[\r\n]*', ' ', text) 
    # Remove special chars.
    text = re.sub('[?!+%{}:;.,"\'()\[\]_]', '',text)
    # Remove double spaces.
    text = re.sub('\s+',' ',text)
    
    return text

def extract_specific_emails(From):
    From = From.lower()
    From = getCompany(From)
    print("From ", From)
    companies = ["vanguard", "robinhood", "schwab","wells fargo","chase"]
    for com in companies: 
        if com in From: 
            return True
    return False


# extract the amount of money in the activity 
def find_number(content):
    num = []
        
    # find the amount $ 
    if "$" in content: 
        index = content.find("$",0, len(content))
        if index > 0 : 
            summ = content[index:index+20]
            num = re.findall('\d+\.\d+|\d+',summ)

    if len(num) == 0: 
        return 0.0

    return num[0]


# categorize emails based on its content text summary 
def categorize(summary, From):
    summary = summary.lower()
    transaction_type = ""
    if "executed" in summary: 
        transaction_type = "Executed Order"
    elif "deposit" in summary or "transfer" in summary: 
        transaction_type = "Deposit"
    elif "placed" in summary: 
        transaction_type = "Placed Order"
    else: 
        transaction_type = "Promotional"
   
    company = getCompany(From)
    return transaction_type, company

def clean(text):
    # clean text for creating a folder
    return "".join(c if c.isalnum() else "_" for c in text)

# get information from email content and header
def getInfo(content,subject,From,time):
    # get company name, transaction type, amount , details of transaction
    transaction_type, company = categorize(content, From)
    amount = find_number(content)
    info = {'company':company, 'transaction_type':transaction_type, 'amount' :amount,"description":subject, "date":time}
    return info 


# extract emails 
def extract_transform_emails():
    mails = []
    # create an IMAP4 class with SSL 
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    # authenticate
    mail.login(USERNAME, PASSWORD)
    # print(imap.list())

    status, messages = mail.select("INBOX")
    # extract only emails that is in the primary category
    status, response = mail.uid('search', 'X-GM-RAW "category:' + 'Primary' + '"')
    # print(status, messages)

    # total number of emails
    messages = int(messages[0])
    response = response[0].decode('utf-8').split()
    response.reverse()
    response = response[:min(80, len(response))]

    # for i in range(response, response-N, -1):
    for i in response:
        # fetch the email message by ID
        res, msg = mail.uid('fetch', i, "(RFC822)") 
        for response in msg:
            if isinstance(response, tuple):
                # parse a bytes email into a message object
                msg = email.message_from_bytes(response[1])
                # decode the email subject
                subject, sub_encoding = decode_header(msg["Subject"])[0]
                if sub_encoding != None and isinstance(subject, bytes):
                    # if it's a bytes, decode to str
                    subject = subject.decode(sub_encoding)
                # decode email sender
                From, encoding = decode_header(msg.get("From"))[0]
                if isinstance(From, bytes):
                    From = From.decode(encoding)
                # print("Subject:", subject)
                # print("From:", From)
                print('********')

                date_tuple = email.utils.parsedate_tz(msg['Date'])
                if date_tuple:
                    local_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                    local_message_date = "%s" %(str(local_date.strftime("%a, %d %b %Y %H:%M:%S")))

                # if extract_specific_emails(str(subject)):
                if extract_specific_emails(From):
                    # if the email message is multipart
                    print("Subject:", subject)
                    print("From:", From)
                    if msg.is_multipart():
                        # iterate over email parts
                        for part in msg.walk():
                            # extract content type of email
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))
                            try:
                                # get the email body
                                body = part.get_payload(decode=True).decode()
                            except:
                                pass
                            
                            # print('content type ', content_type)
                            if content_type == "text/plain" and "attachment" not in content_disposition:
                                # print text/plain emails and skip attachments
                                print("Printing the body...")
                                body = h.handle(body)
                                # print(body)
                                
                                # if text summary is none --> return the subject headline 
                                mail_info = getInfo(body,subject,From,local_date)
                                if mail_info['transaction_type'] != "Promotional":
                                    mails.append(mail_info)

                            elif "attachment" in content_disposition:
                                # download attachment
                                filename = part.get_filename()
                                if filename:
                                    folder_name = clean(subject) 
                                    folder_name = "./downloads/"+folder_name   
                                    if not os.path.isdir(folder_name):
                                        # make a folder for this email (named after the subject)
                                        
                                        os.mkdir(folder_name)
                                    filepath = os.path.join(folder_name, filename)
                                    # download attachment and save it
                                    open(filepath, "wb").write(part.get_payload(decode=True))
                            
                    else:
                        # extract content type of email
                        content_type = msg.get_content_type()
                        # get the email body
                        body = msg.get_payload(decode=True).decode()
                    if content_type == "text/html":
                        body = h.handle(body)
                        # print( body)

                        # if it's HTML, create a new HTML file and open it in browser
                        folder_name = clean(subject)
                        folder_name = "./downloads/"+folder_name
                        if not os.path.isdir(folder_name):
                            # make a folder for this email (named after the subject)
                            os.mkdir(folder_name)
                        filename = "index.html"
                        filepath = os.path.join(folder_name, filename)
                        # write the file
                        open(filepath, "w").write(body)
                        # open in the default browser
                        webbrowser.open(filepath)
                    print("="*100)


    

    companies = []
    transaction_types = []
    amount = []
    dates = []
    description = []
    for m in mails:
        companies.append(m['company'])
        transaction_types.append(m['transaction_type'])
        amount.append(m['amount'])
        dates.append(m['date'])
        description.append(m['description'])
    df = pd.DataFrame({'company':companies, 'transaction_type':transaction_types, 'amount':amount,"description":description, 'date':dates})
    
    # close the connection and logout
    mail.close()
    mail.logout()

    return df




