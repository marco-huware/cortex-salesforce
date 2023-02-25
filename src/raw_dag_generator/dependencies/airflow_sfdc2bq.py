# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" This module provides SFDC -> BigQuery extraction Airflow bootstrapper  """

from google.cloud import bigquery

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

from simple_salesforce import Salesforce

from sfdc2bq import sfdc2bq_replicate  # pylint:disable=wrong-import-position


def extract_data_from_sfdc(
    sfdc_connection_id: str,
    api_name: str,
    bq_connection_id: str,
    project_id: str,
    dataset_name: str,
    output_table_name: str,
) -> None:

    sf_setting = {"username": "admin.salesforce@huware.com",
                "password": "Huware2021!",
                "consumer_key": "3MVG91BJr_0ZDQ4szU7OX8ztsx8M2rHe2NWpBHecWLk3StvDGOoO.MZlp5NVJS10O9NuBGI_OBHhn9mxO5N6M",
                "consumer_secret": "1D3817C44B487193505C7A96D2E4E3F4E255F1D878D6924FD2D5A0DF3A4EA9D9"
    }

    simple_sf_connection = Salesforce(**sf_setting)
    
    if bq_connection_id and bq_connection_id != "":
        # Salesforce hook made with a connection or a secret
        bq_hook = BigQueryHook(bq_connection_id)
        bq_client = bq_hook.get_client()
    else:
        # If empty, use default credentials
        bq_client = bigquery.Client()

    sfdc2bq_replicate(simple_sf_connection=simple_sf_connection,
                      api_name=api_name,
                      bq_client=bq_client,
                      project_id=project_id,
                      dataset_name=dataset_name,
                      output_table_name=output_table_name)
