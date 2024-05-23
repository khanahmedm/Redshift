# Redshift Data API

## Create tables
```sql
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
  R_REGIONKEY bigint NOT NULL,
  R_NAME varchar(25),
  R_COMMENT varchar(152))
diststyle all;

CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME varchar(25),
  N_REGIONKEY bigint,
  N_COMMENT varchar(152))
diststyle all;

create table customer (
  C_CUSTKEY bigint NOT NULL,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY bigint,
  C_PHONE varchar(15),
  C_ACCTBAL decimal(18,4),
  C_MKTSEGMENT varchar(10),
  C_COMMENT varchar(117))
diststyle all;

create table orders (
  O_ORDERKEY bigint NOT NULL,
  O_CUSTKEY bigint,
  O_ORDERSTATUS varchar(1),
  O_TOTALPRICE decimal(18,4),
  O_ORDERDATE Date,
  O_ORDERPRIORITY varchar(15),
  O_CLERK varchar(15),
  O_SHIPPRIORITY Integer,
  O_COMMENT varchar(79))
distkey (O_ORDERKEY)
sortkey (O_ORDERDATE);
```

## Load data
```sql
COPY region FROM 's3://redshift-immersionday-labs/data/region/region.tbl.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

COPY nation FROM 's3://redshift-immersionday-labs/data/nation/nation.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy customer from 's3://redshift-immersionday-labs/data/customer/customer.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy orders from 's3://redshift-immersionday-labs/data/orders/orders.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;
```

## Create policy
```
{
"Version": "2012-10-17",
"Statement": [
  {
    "Effect": "Allow",
    "Action": ["redshift:GetClusterCredentials"],
    "Resource": [
      "arn:aws:redshift:*:[AWS_Account]:dbname:[Redshift_Cluster_Identifier]/[Redshift_Cluster_Database]",
      "arn:aws:redshift:*:[AWS_Account]:dbuser:[Redshift_Cluster_Identifier]/[Redshift_Cluster_User]"
    ]
  },
  {
    "Effect": "Allow",
    "Action": "redshift-data:*",
    "Resource": "*"
  }
]
}
```

## Create Lambda function
```
#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import boto3
import json
import datetime

# initialize redshift-data client in boto3
redshift_client = boto3.client("redshift-data")

def call_data_api(redshift_client, redshift_database, redshift_user, redshift_cluster_id, sql_statement, with_event=True):
    # execute the input SQL statement
    api_response = redshift_client.execute_statement(Database=redshift_database, DbUser=redshift_user
                                                    ,Sql=sql_statement, ClusterIdentifier=redshift_cluster_id, WithEvent=True)

    # return the query_id
    query_id = api_response["Id"]
    return query_id

def check_data_api_status(redshift_client, query_id):
    desc = redshift_client.describe_statement(Id=query_id)
    status = desc["Status"]

    if status == "FAILED":
        raise Exception('SQL query failed:' + query_id + ": " + desc["Error"])
    return status.strip('"')

def get_api_results(redshift_client, query_id):
    response = redshift_client.get_statement_result(Id=query_id)
    return response

def lambda_handler(event, context):
    redshift_cluster_id = os.environ['redshift_cluster_id']
    redshift_database = os.environ['redshift_database']
    redshift_user = os.environ['redshift_user']

    action = event['queryStringParameters'].get('action')
    try:
        if action == "execute_report":
            country = event['queryStringParameters'].get('country_name')
            # sql report query to be submitted
            sql_statement = "select c.c_mktsegment as customer_segment,sum(o.o_totalprice) as total_order_price,extract(year from o.o_orderdate) as order_year,extract(month from o.o_orderdate) as order_month,r.r_name as region,n.n_name as country,o.o_orderpriority as order_priority from public.orders o inner join public.customer c on o.o_custkey = c.c_custkey inner join public.nation n on c.c_nationkey = n.n_nationkey inner join public.region r on n.n_regionkey = r.r_regionkey where n.n_name = '"+ country +"' group by 1,3,4,5,6,7 order by 2 desc limit 10"
            api_response = call_data_api(redshift_client, redshift_database, redshift_user, redshift_cluster_id, sql_statement)
            return_status = 200
            return_body = json.dumps(api_response)

        elif action == "check_report_status":            
            # query_id to input for action check_report_status
            query_id = event['queryStringParameters'].get('query_id')            
            # check status of a previously executed query
            api_response = check_data_api_status(redshift_client, query_id)
            return_status = 200
            return_body = json.dumps(api_response)

        elif action == "get_report_results":
            # query_id to input for action get_report_results
            query_id = event['queryStringParameters'].get('query_id')
            # get results of a previously executed query
            api_response = get_api_results(redshift_client, query_id)
            return_status = 200
            return_body = json.dumps(api_response)

            # total number of rows
            nrows=api_response["TotalNumRows"]
            # number of columns
            ncols=len(api_response["ColumnMetadata"])
            print("Number of rows: %d , columns: %d" % (nrows, ncols) )

            for record in api_response["Records"]:
                print (record)
        else:
            return_status = 500
            return_body = "Invalid Action: " + action
        return_headers = {
                        "Access-Control-Allow-Headers" : "Content-Type",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "GET"}
        return {'statusCode' : return_status,'headers':return_headers,'body' : return_body}
    except NameError as error:
        raise NameError(error)
    except Exception as exception:
        error_message = "Encountered exeption on:" + action + ":" + str(exception)
        raise Exception(error_message)
```

## Integrate  with API gateway


## Test API in browser
```
<html>
<head><meta http-equiv="Access-Control-Allow-Origin" content="*"></head>
<script>
var endpoint;
var counter;

function updateStatus(endpoint, querystring, callback) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
     callback(this.responseText);
    }
  };
  console.log(endpoint+querystring);
  xhttp.open("GET", endpoint+"/"+querystring, true);
  xhttp.send();
}

function submitQuery() {
  endpoint = document.getElementById('endpoint').value;
  var country = document.getElementById('country').value;
  querystring = "?action=execute_report&country_name="+country;

  updateStatus(endpoint, querystring, function(status){
    query_id = status.split('"').join('');
    document.getElementById("status").innerHTML = query_id;
    querystring = "?action=check_report_status&query_id="+query_id;
    counter = 1;
    checkStatus(endpoint, querystring, function(){
      querystring = "?action=get_report_results&query_id="+query_id;
      updateStatus(endpoint, querystring, function(status){
        var jsonString = "<pre>" + JSON.stringify(JSON.parse(status),null,2) + "</pre>";
        document.getElementById("status").innerHTML = jsonString;
      });
    });
  });
}

function checkStatus(endpoint, querystring, callback) {
  updateStatus(endpoint, querystring, function(status){
    if (status == "\"FINISHED\"")
      callback();
    else {
      document.getElementById("status").innerHTML = counter + ": " + status;
      setTimeout('', 1000);
      counter++;
      checkStatus(endpoint, querystring, callback)
    }
  });
}

</script>
<label for=endpoint>Endpoint:</label><input id=endpoint type=text style="width:100%"><br>
<label for=country>Country:</label><input id=country type=text style="width:100%">
<button type="button" onclick=submitQuery()>Submit</button>
<div id=status>
</div>
```

