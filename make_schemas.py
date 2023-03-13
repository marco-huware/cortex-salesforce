# import salesforce_bulk
import pandas as pd
from simple_salesforce import Salesforce
import typing
from google.cloud import bigquery
import json
from google.oauth2 import service_account
from airflow.models.dag import DagRun, State
from airflow.models.dagbag import DagBag
# from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
# from simple_salesforce import Salesforce
from datetime import datetime
user = "USER"
pwd = "PASSWORD"
client_id = "CLIENT_ID"
client_secret = "SECRET"


sf = Salesforce(password = pwd,
        username = user,
        consumer_key=client_id,
        consumer_secret = client_secret)

api_names = {
    'Account':'accounts',
    'Case':'cases',
    'Contact':'contacts',
    'Event':'events',
    'Lead':'leads',
    'Opportunity':'opportunities',
    'RecordType':'record_types',
    'Task':'tasks',
    'User':'users'
}


type_mapping: typing.List[typing.Tuple[typing.List[str], str]] = [
            ([
                "id", "string", "textarea", "base64", "phone", "url", "email",
                "encryptedstring", "picklist", "multipicklist", "reference",
                "junctionidlist", "datacategorygroupreference", "masterrecord",
                "combobox", "anytype"
            ], "STRING"), (["double", "currency", "percent"], "FLOAT64"),
            (["date"], "DATE"), (["time"], "TIME"), (["datetime"], "TIMESTAMP"),
            ([
                "int",
                "long",
                "byte",
            ], "INT64"), (["boolean"], "BOOL")
        ]


def make_schema(api_names,sf_connection,type_mapping) -> None:
    for api_name in api_names:
        acc_fields = getattr(sf_connection,api_name).describe()
        acc_fields = {field['name']:field['type'] for field in acc_fields['fields']}

        og_cols = pd.read_csv(f'./cortex-salesforce/src/table_schema/{api_names[api_name]}.csv')

        schema = {'SourceField':[],'TargetField':[],'DataType':[]}
        for col in acc_fields: 
            try:
                operation = "queryAll"
                query = f"SELECT {col} FROM {api_name} limit 1"
                # print(query)
                request_body = {
                    "operation": operation,
                    "query": query,
                    "contentType": "CSV",
                    "columnDelimiter": "COMMA",
                    "lineEnding": "LF",
                }

                # Start a job
                job = sf.restful(path="jobs/query",
                                method="POST",
                                data=json.dumps(request_body))
                # print(f'Col {col} working!')
            except Exception as e:
                if col in og_cols.SourceField.values:
                    print('Cortex expected column {col} to be imported, however API is giving the following error:')
                    print(e)
                continue
                
            bq_type = type_mapping[[i for i,type in enumerate(type_mapping) if acc_fields[col] in type[0]][0]][1]
            schema['DataType'].append(bq_type)
            
            if 'date' in col.lower() and bq_type.lower() == 'timestamp' and not col.lower().endswith('stamp'):
                schema['TargetField'].append(f'{col}stamp')
                schema['SourceField'].append(col)
            elif col.lower() == 'id':
                schema['SourceField'].append(col)
                schema['TargetField'].append(f'{api_name}Id')
            else:
                schema['TargetField'].append(col)
                schema['SourceField'].append(col)
        
        schema['TargetField'].append("Recordstamp")
        schema['SourceField'].append("RecordStamp")
        schema['DataType'].append("TIMESTAMP")
        pd.DataFrame.from_dict(schema).to_csv(f'./cortex-salesforce/src/table_schema/{api_names[api_name]}.csv', index=False)
    return None

make_schema(api_names = api_names, sf_connection = sf, type_mapping = type_mapping)


# Create tables on BQ with the specified schemas
project_id = 'PROJECT_ID'
raw_dataset = 'REPLICATION_DATASET'
cdc_dataset = 'CDC_DATASET'
report_dataset = 'REPORTING'
bq_client = bigquery.Client(project=project_id, credentials=service_account.Credentials.from_service_account_file('./cortex-salesforce-hw-demo-03d133b3a4f2.json'))


# Create datasets or delete tables present in them if created alrady before
datasets = [raw_dataset,cdc_dataset,report_dataset]
for dataset in datasets:
    bq_client.create_dataset(dataset,exists_ok=True)

    try:
        dataset_ref = bq_client.get_dataset(dataset)
        tables = bq_client.list_tables(dataset_ref)
        for table in tables:
            # print(table)

            # First delete table if present
            bq_client.delete_table(table)
    except:
        continue


dataset_ref = bq_client.get_dataset(raw_dataset)
for table in api_names:
    schema_file = pd.read_csv(f'./cortex-salesforce/src/table_schema/{api_names[table]}.csv')
    schema = []
    for field in schema_file.iterrows():
        field = field[1]
        if field.SourceField == f'{table}Id':
            field_name = 'Id'
        else:
            field_name = field.SourceField
        
        schema.append(bigquery.SchemaField(field_name, field.DataType, mode = 'NULLABLE'))
    table_ref = bigquery.Table(f'{project_id}.{raw_dataset}.{api_names[table]}', schema = schema)
    bq_client.create_table(table = table_ref)