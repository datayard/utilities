import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import yaml

from enum import Enum
import copy

# SpreadSheet Representation
class ss_salesforce_datadict(Enum):
    OBJECT = 0
    SCHEMA = 1
    TABLE = 2
    STAGING_FOLDER = 3
    DBT_STAGING_TABLE = 4
    DBT_STAGING_FIELD_NAME = 5
    REDSHIFT_API_NAME = 6
    UI_NAME = 7
    IS_KEY = 8
    FIELD_TYPE = 9
    ISNULL = 10
    DESCRIPTION = 11
    EXTRA_NOTES = 12
    EXAMPLE = 13

# SpreadSheet Representation
class ss_vidyard_datadict(Enum):
    OBJECT = 0
    SCHEMA = 1
    TABLE = 2
    STAGING_FOLDER = 3
    DBT_STAGING_TABLE = 4
    DBT_STAGING_FIELD_NAME = 5
    REDSHIFT_API_NAME = 6
    UI_NAME = 7
    IS_KEY = 8
    FIELD_TYPE = 9
    DESCRIPTION = 10
    EXTRA_NOTES = 11
    EXAMPLE = 12    

def read_excel_sheets_to_pandas_dataframes(target_excel_file, target_sheets):
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('bizopsgdrive_client_secret.json', scope)
    client = gspread.authorize(creds)

    target_sheets_lowercase = [x.lower() for x in target_sheets]

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    for spreadsheet in client.openall():
        if spreadsheet.title == target_excel_file:
            for sheet in client.open(spreadsheet.title).worksheets():
                
                sheetname = sheet.title.replace("_", " ")
                if sheetname in target_sheets_lowercase:
                    data = sheet.get_all_values()
                    headers = data.pop(0)
                    df = pd.DataFrame(data, columns=headers)
                    yield df                

def read_csv():
    df = pd.read_csv("SalesforceDataDictionaryAggregated.csv")
    return df

def generate_sql(target_excel_file, target_sheets):

    for df_input in read_excel_sheets_to_pandas_dataframes(target_excel_file, target_sheets):

        field_structure = "{2}.{1} as {0}"

        staging_file_name = ""
        table_name = ""
        table_alias = ""
        fields = []
        
        for row in df_input.itertuples(index=False):
            staging_file_name = row[ss_vidyard_datadict.DBT_STAGING_TABLE.value]
            table_name = "{{{{ source('{0}', '{1}') }}}}".format(
                                                            row[ss_vidyard_datadict.SCHEMA.value], row[ss_vidyard_datadict.TABLE.value]
                                                        )
            
            
            fields.append(
                            field_structure.format(
                                    row[ss_vidyard_datadict.DBT_STAGING_FIELD_NAME.value], 
                                    row[ss_vidyard_datadict.REDSHIFT_API_NAME.value], 
                                    table_alias + row[ss_vidyard_datadict.TABLE.value]
                                )
                            )

        sql = "SELECT \n\t{0}\r\n FROM \n\t{1} as {2}".format(
                                                            ",\n\t".join(fields), 
                                                            table_name, 
                                                            table_alias + row[ss_vidyard_datadict.TABLE.value]
                                                        )

        sql_file = open(".//vidyard//{0}.sql".format(staging_file_name), "wt")
        sql_file.write(sql)
        sql_file.close()

        print("{0} generated".format(staging_file_name))

def generate_yaml(target_excel_file, target_sheets):

    # YAML structure - TABLES & MODELS
    #tables:
    #   - name: account

    #models:
    #   - name: stg_salesforce_account
    #       columns:
    #            - name: accountId
    #              description: acccount id description
    #              tests:
    #               - unique
    #               - not_null    

    tables = []
    models = []

    for df_input in read_excel_sheets_to_pandas_dataframes(target_excel_file, target_sheets):

        current_table = {}
        current_model = {}

        columns = []
        model_columns = []
        redshift_name = ""
        staging_name = ""
        
        for row in df_input.itertuples(index=False):
                column_dict = {}
                column_dict["name"] = row[ss_vidyard_datadict.REDSHIFT_API_NAME.value]

                #column_dict["description"] = (str(row[ss_vidyard_datadict.DESCRIPTION.value]).replace("\n", "")).replace("\t", "")
                columns.append(column_dict)

                model_dict = {}
                model_dict["name"] = row[ss_vidyard_datadict.DBT_STAGING_FIELD_NAME.value]
                model_dict["description"] = (str(row[ss_vidyard_datadict.DESCRIPTION.value]).replace("\n", "")).replace("\t", "")
                model_columns.append(model_dict)

                if str(row[ss_vidyard_datadict.IS_KEY.value]) == "TRUE":
                    column_dict["tests"] = ["unique", "not_null"]
                    model_dict["tests"] = ["unique", "not_null"]                    

                redshift_name = row[ss_vidyard_datadict.TABLE.value]
                staging_name = row[ss_vidyard_datadict.DBT_STAGING_TABLE.value]

        current_table["name"] = redshift_name
        #current_table["columns"] = columns

        current_model["name"] = staging_name
        current_model["columns"] = model_columns

        tables.append(copy.deepcopy(current_table))
        models.append(copy.deepcopy(current_model))

    yaml_file_content = {}
    yaml_file_content["tables"] = tables
    yaml_file_content["models"] = models

    yaml_file = open(".//vidyard//vidyard.yml", "wt")
    yaml.dump(yaml_file_content, yaml_file, sort_keys=False)
    yaml_file.close()

    print("YAML file generated")

if __name__ == "__main__":
        
    target_excel_file = "Vidyard Master Data Dictionary"
    target_sheets = [
        'Organizations',
        'Teams',
        'Team Memberships',
        'Users',
        'User Groups',
        'Metrics',
        'Videos',
        'Features',
        'Active Features',
        'Players',
        'Allotment Limits',
        'Allotment Types',
        'Hubs',
        'Events',
        'Event Joins'
    ]

    generate_sql(target_excel_file, target_sheets)
    generate_yaml(target_excel_file, target_sheets)
    