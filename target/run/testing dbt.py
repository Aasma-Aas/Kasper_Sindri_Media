#import os
#import yaml
import mysql.connector
import json
import requests
import logging
#import time
import subprocess
#import traceback
#import ssl
#import smtplib
#import mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def get_database_connection():
    # Establish a connection to the MySQL database
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host = '51.158.56.32',
            port = 1564,
            user = 'Site',
            password = "515B]_nP0;<|=pJOh35I"
        )
        cursor = connection.cursor()
        return cursor, connection
        

#host: 195.154.196.76
#port: 13498
#site_dev
#87]N_8J!IlO+Y?(DB'B$
    
# host = '51.158.56.32',
# port = 1564,
# user = 'Site',
# password = '515B]_nP0;<|=pJOh35I'
    
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)


def authenticate():
    try:
        url = 'https://sindri.media/strapi/api/auth/local'
        payload = {
            'identifier': 'import@dashboardapp.com',
            'password': 'JSyzkFtR9Cw5v5t'
        }
        auth_response = requests.post(url, data=payload)
        
        # Assuming the JWT token is returned in the 'token' field of the response JSON
        token = auth_response.json().get('jwt')
        return token
        
    except requests.exceptions.RequestException as req_err:
        print("Request error occurred:", req_err)
        return None
    except Exception as ex:
        print("An unexpected exception occurred:", ex)
        return None

        
def post(token,url,json_data_list):
    while token is None:  # Keep trying until a valid token is obtained
        print("Invalid or None token. Re-authenticating...")
        token = authenticate()
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        print('len: ',len(json_data_list))
        c = 0
        for json_data in json_data_list:
            response = requests.post(url, headers=headers, json=json_data)
            if response.status_code == 200:
                c=c+1
                print(response.text,c)
            elif response.status_code == 401:  # Unauthorized, possibly due to invalid token
                print("Unauthorized access. Refreshing token.")
                token = authenticate()
                while token is None:  # Keep trying until a valid token is obtained
                    print("Invalid or None token. Re-authenticating...")
                    token = authenticate()
                headers = {
                                'Authorization': f'Bearer {token}',
                                'Content-Type': 'application/json'}
                response = requests.post(url, headers=headers, json=json_data)
                if response.status_code == 200:
                    c = c + 1
                    print(response.text, c)
                else:
                    print(f'Error: {response.status_code} - {response.text}')
            else:
                print(f'Error: {response.status_code} - {response.text}')
    except Exception as e:
        print(e)
    
def post_batch(token,url,batch):
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        print('len: ',len(batch))
        print("URL:", url)
        response = requests.post(url, headers=headers, json=batch)

        if response.status_code == 200:
            print(response.text)
            print("Batch posted successfully.")
        else:
            print(f"Failed to post batch. Status code: {response.status_code}")
    except Exception as e:
        print(e)

def compare_json_format(json1, json2):
    try:
        json2 = json.dumps(json2)
        # Load JSON strings into Python dictionaries
        data1 = json.loads(json1)
        data2 = json.loads(json2)

        # Extract keys from the dictionaries
        keys1 = set(data1.keys())
        keys2 = set(data2.keys())

        # Compare the keys
        if keys1 == keys2:
            logging.info("=====================The JSON formats have the same keys========================")
            return True
        else:
            logging.info("The JSON formats have different keys.")
            return False
    except Exception as e:
        print(e)
        return False
    
            
def server_resour():
    # List of commands to run
    commands = [
        'df -h',
        'free -m',
        'top -b -n 1 | grep Cpu'
    ]
    res_list =[]

    for command in commands:
        try:
            output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, text=True)
            res_list.append(output)
        except subprocess.CalledProcessError as e:
            res_list.append(e.output)
    return res_list


def start_execution():
    
    # Select the  values of the credentials column
    cursor,connection = get_database_connection()
    select_query = f"select * from prod.credentials_detail where status ='active' and siteid  = 15;"
    cursor.execute(select_query)
    cred = cursor.fetchall()
    print(cred)
    cursor.close()
    connection.close()
    for row in cred:
        siteid = row[1]
        start_endpoint_to_post(siteid)
            


def start_endpoint_to_post(siteid):
    try:
        
        print('Data posting started......')
        subject = f"[COMPLETED]Data Posted Completed via Swagger: Site ID 17"
        message = f"HY"
        message = str(message)
         
        # ---------------------ENDPOINT and Function name get-------------------
        
        select_query = f"""call pre_stage.SelectINputBasedOnSiteId_dbt(17);"""
        #  database connection handle
        try:
            # Select the  values of the credentials column
            cursor,connection = get_database_connection()
            cursor.execute(select_query)
            endp = cursor.fetchall()
        except mysql.connector.errors.OperationalError as op_err:
            if "is locked" in str(op_err):
                print("Database is busy. Please try again later.")
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
            elif "Can't connect" in str(op_err):
                print("Unable to connect to the database.")
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
            else:
                print("An unexpected operational error occurred:", op_err)
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
        except mysql.connector.errors.IntegrityError as int_err:
            print("Integrity error occurred:", int_err)
        except mysql.connector.errors.ProgrammingError as prog_err:
            print("Programming error occurred:", prog_err)
        except mysql.connector.errors.InterfaceError as intf_err:
            print("Interface error occurred:", intf_err)
        except Exception as e:
            print(e)
            cursor,connection = get_database_connection()
            cursor.execute(select_query)
            endp = cursor.fetchall()
        
        
        connection.close()
        cursor.close()
        print('siteid : ',siteid)
        #getting authenticate token
        token = authenticate()
        
        
            # ------------------- Query starting-----------
        for i in endp:
            try:
                json_data = 0
                json_data_list = []
                # print('start')
                req_json_format = i[3]
                parameter = str(i[2])
                print('\n',i[0])
                label_val = None
                hint_val = None
                event_action_val = None
                series = None
                category = None
                ty = False
                
                if i[0] in [99,100,101]:
                    
                    event_action_val = None
                    print(i[0])
                    cursor,connection = get_database_connection()
                    
                    if i[5] is not None and len(i[5]) > 0:

                        data_dict = json.loads(i[5])
                        for key, val in data_dict.items():
                            if key == "event_action":
                                event_action_val = val
                            
                    if event_action_val is not None:
                        query = f"call prod.{i[1]}({siteid},  '{event_action_val}' );"
                    else:
                        query = f"call prod.{i[1]}({siteid});"
                        
                    print(query)
                    cursor.execute(query)
                    result = cursor.fetchall()

                    json_data_list = []
                    batch_size = 100
                    url = 'https://sindri.media' + parameter

                    for row in result:
                        json_data = json.loads(row[0])
                        json_data['site'] = '2'
                        json_data_list.append(json_data)

                        if len(json_data_list) == batch_size:
                            print(json_data_list)
                            post_batch(token,url,json_data_list)
                            print('batch posted')
                            json_data_list = []

                        # Check if there are any remaining JSON data
                    if json_data_list:
                        print(json_data_list)
                        post_batch(token,url,json_data_list)
                        print('batch posted')
                else:

                    # -------------- label and hints get in the json input column---------
                    if i[7] is not None and len(i[7]) > 0:
                        data_dict = json.loads(i[7])
                        word_to_find = "table"
                        # Check if the word is present in the string
                        if i[1].lower().find(word_to_find.lower()) != -1:
                            if 'label' in data_dict:
                                label_val = data_dict["label"]
                            ty = True
                        else:
                            if 'label' in data_dict:
                                label_val = data_dict["label"]
                            if 'hint' in data_dict:
                                hint_val = data_dict["hint"]


                    # -------------- query specific input ---------
                    if i[5] is not None and len(i[5]) > 0:

                        data_dict = json.loads(i[5])
                        for key, val in data_dict.items():
                            if key == "event_action":
                                event_action_val = val
                            elif key == "series":
                                series = val
                                series_val1 = series[0]
                                series_val2 = series[1]
                            elif key == "category":
                                category = val
                                print('category', category)
                            elif key == "dtitle":
                                dtitle = val
                                print('dtitle', dtitle)
                            else:
                                print('No find')
                    # ---------------query , SP , Parameter , input
                    
                    # if (event_action_val is not None) and (category is not None):
                    #     query = f"call prod.{i[1]}({siteid}, '''{label_val}''' ,  '''{hint_val}''' ,'{category}','{dtitle}','{event_action_val}');"
                    
                    
                    # elif (event_action_val is not None) and (series is not None):
                    #     query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}', '{series_val1}' , '{series_val2}');"
                    
                    
                    # elif event_action_val is not None:
                        
                    #     if (label_val is not None) and (hint_val is not None):
                    #         query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}' );"
                    #     elif ty == True:
                    #         label_val=json.dumps(label_val)
                    #         query = f"call prod.{i[1]}({siteid},  '{event_action_val}', '{label_val}' );"
                    #     else:
                    #         query = f"call prod.{i[1]}({siteid},  '{event_action_val}' );"
                    
                    # elif series is not None:

                    #     query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' ,  '{series_val1}' , '{series_val2}');"

                    # elif category is not None:
                    #     query = f"call prod.{i[1]}({siteid}, '''{label_val}''' ,  '''{hint_val}''' , '{category}','{dtitle}' );"
                    
                    # elif ty == True: 
                    #     query = f"call prod.{i[1]}({siteid}, '{label_val}'  );"
                    
                    # elif (label_val is not None) and (hint_val is not None):
                     
                    #     query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' );"
                    
                    # else:
                    query = f"call prod.{i[1]}();"
                    
                    print(query)
                    
                    try:
                        cursor,connection = get_database_connection()
                        cursor.execute(query)
                        

                    except mysql.connector.errors.OperationalError as op_err:
                        if "is locked" in str(op_err):
                            print("Database is busy. Please try again later.")
                            cursor,connection = get_database_connection()
                            cursor.execute(query)
                        elif "Can't connect" in str(op_err):
                            print("Unable to connect to the database.")
                            cursor,connection = get_database_connection()
                            cursor.execute(query)

                        else:
                            print("An unexpected operational error occurred:", op_err)
                            cursor,connection = get_database_connection()
                            cursor.execute(query)

                    except mysql.connector.errors.IntegrityError as int_err:
                        print("Integrity error occurred:", int_err)
                    except mysql.connector.errors.ProgrammingError as prog_err:
                        print("Programming error occurred:", prog_err)
                    except mysql.connector.errors.InterfaceError as intf_err:
                        print("Interface error occurred:", intf_err)
                    except Exception as e:
                        print(e)
                        cursor,connection = get_database_connection()
                        cursor.execute(query)
                    results = cursor.fetchall()
                    connection.close()
                    cursor.close()
    #                 print(results)

                    
                    if len(results)  == 0:
                        print('Blank result of this query: ',query)
                        json_data =  json.loads(i[3])
                        json_data['site'] = '2'
                        json_data_list.append(json_data)
                        print(json_data_list)
                    # Process the results
                    elif None in results[0]:
                        json_data =  json.loads(i[3])
                        json_data['site'] =  '2'
                        json_data_list.append(json_data)
                        print(json_data_list)
                    else:
                        # print('result: ',results)
                        for j in results:
                            json_data = json.loads(j[0])
                            json_data['site'] =  '2'
                            json_data_list.append(json_data)
                        print('json_data_list')
    #                 print(query)
                    url = 'https://sindri.media' + parameter
                    print(url)
                    print('output json:' , json_data)
                    my_for = json_data
                    if my_for != 0:
                        verf = compare_json_format(req_json_format,my_for)
                        verf =True
                        print('Json Verified: ',verf)
                        if verf == True:
                            post(token,url,json_data_list)
                            print('Data posted')
                        else:
                            print(f'this query {query} of json not match in Swagger json: ',url)
                    else:
                        print(f'this query {query} result is empty')
            except Exception as e:
                print(e)
         
    except Exception as e:
        print(e)



def schema_post():
    json_data_list = []
    schema = {
  "site": 17,
  "data": {
    "columns": [
      {
        "field": "id",
        "label": "ID"
      },
      {
        "field": "article",
        "label": "ARTIKEL"
      },
      {
        "field": "category",
        "label": "BRUGERBEHOV"
      },  
      {
        "field": "sektion",
        "label": "KATEGORI",
        "hidden": True
      },  
      {
        "field": "tags",
        "label": "TAGS",
        "hidden": True
      },
      {
        "field": "date",
        "label": "DATO",
      },
      {
        "field": "brugerbehov",
        "label": "SIDEVISNINGER"
      },
      {
        "field": "clicks",
        "label": "NEXT CLICK (%)"
      }
    ]
  }
} 
   
    token = authenticate()
    json_data_list.append(schema)
    print(json_data_list)
    url = 'https://sindri.media/api/v1/set-widget/periodeoversigt::articles::columns/' 
    post(token,url,json_data_list)








# create a object of class  
def dev_etl():
    logging.info("Site Archive Post")
    start_execution()  


dev_etl()
    


