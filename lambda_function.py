import json
import boto3
import csv
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    csv_file_obj = s3_client.get_object(Bucket=bucket, Key=csv_file)
    lines = csv_file_obj['Body'].read().decode('utf-8').split()
    results = []
    for row in csv.DictReader(lines):
        results.append(row.values())
    print(results)
    connection = mysql.connector.connect(host='13.233.192.139',database='salesdatabase',user='admin',password='gadde234')
    insert_query = "INSERT INTO sales_info (Region,country,item_type,sales_channel,order_priority,order_date,order_id,ship_date,units_sold,unit_price,unit_cost,total_revenue,total_cost,total_profit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    report1= "create table salesrpt1 As (select year(ship_date),month(ship_date),sum(units_sold),sum(total_revenue),sum(total_profit) from sales_info group by year(ship_date), month(ship_date), region)"
    report2= "create table salesrpt2 As (select year(ship_date),sum(units_sold),sum(total_revenue),sum(total_profit) from sales_info group by year(ship_date),region)" 
    report3= "create table salesrpt3 As (select sales_channel,sum(units_sold),sum(total_revenue),sum(total_profit) from sales_info group by sales_channel)"
    cursor = connection.cursor()
    cursor.executemany(insert_query,results)
    cursor.execute(report1)
    cursor.execute(report2)
    cursor.execute(report3)
    connection.commit()
        
    message = {"report_status": "SUCCESS"}
    client = boto3.client('sns')
    response = client.publish(
        TargetArn='arn:aws:sns:ap-south-1:310838476048:sns-sales:2a7f1c49-9221-4fa1-9e7c-0b5d3d903fdb',
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }