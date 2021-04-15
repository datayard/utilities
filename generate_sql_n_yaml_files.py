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


def read_csv():
    df = pd.read_csv("SalesforceDataDictionaryAggregated.csv")
    return df

def generate_sql(df_input):

    if df_input is None or len(df_input) <= 0:
        return ""

    field_structure = "{2}.{1} as {0}"

    for schema in df_input["schema "].unique():
        for table in df_input["table"].unique():

            staging_file_name_account = ""
            account_table = ""
            account_table_alias = "sfdc_"
            account_fields = []
            
            for row in df_input.itertuples(index=False):
                if row[ss_salesforce_datadict.SCHEMA.value] == schema and row[ss_salesforce_datadict.TABLE.value] == table:
                        
                        staging_file_name_account = row[ss_salesforce_datadict.DBT_STAGING_TABLE.value]
                        account_table = row[ss_salesforce_datadict.SCHEMA.value] + "." + row[ss_salesforce_datadict.TABLE.value]
                        account_fields.append(field_structure.format(row[ss_salesforce_datadict.DBT_STAGING_FIELD_NAME.value], row[ss_salesforce_datadict.REDSHIFT_API_NAME.value], account_table_alias + row[ss_salesforce_datadict.TABLE.value]))

                        accounts_sql = "SELECT \n {0} \r\n FROM \n {1} as {2}".format(",\n".join(account_fields), account_table, account_table_alias + row[ss_salesforce_datadict.TABLE.value])
                        sql_file = open("{0}.sql".format(staging_file_name_account), "wt")
                        sql_file.write(accounts_sql)
                        sql_file.close()

    print("SQL file generated")

def generate_yaml(df_input):

    if df_input is None or len(df_input) <= 0:
        return ""

    # YAML structure - TABLES & MODELS

    # - name: stg_salesforce_contact
    #    columns:
    #      - name: id
    #        description: Primary key of the table
    #        tests:
    #          - unique
    #          - not_null

    #models:
    #   - name: stg_salesforce_account
    #       columns:
    #            - name: accountId
    #                tests:
    #                    - unique
    #                    - not_null    

    tables = []
    models = []

    for schema in df_input["schema "].unique():
        for table in df_input["table"].unique():

            current_table = {}
            current_model = {}

            columns = []
            model_columns = []
            name = ""
            
            for row in df_input.itertuples(index=False):
                if row[ss_salesforce_datadict.SCHEMA.value] == schema and row[ss_salesforce_datadict.TABLE.value] == table:
                    
                    column_dict = {}
                    column_dict["name"] = row[ss_salesforce_datadict.REDSHIFT_API_NAME.value]

                    column_dict["description"] = (str(row[ss_salesforce_datadict.DESCRIPTION.value]).replace("\n", "")).replace("\t", "")
                    columns.append(column_dict)

                    model_dict = {}
                    model_dict["name"] = row[ss_salesforce_datadict.DBT_STAGING_FIELD_NAME.value]
                    model_columns.append(model_dict)

                    if str(row[ss_salesforce_datadict.IS_KEY.value]) == "True":
                        column_dict["tests"] = ["unique", "not_null"]
                        model_dict["tests"] = ["unique", "not_null"]                    

                    name = row[ss_salesforce_datadict.DBT_STAGING_TABLE.value]

            current_table["name"] = name
            current_table["columns"] = columns

            current_model["name"] = name
            current_model["columns"] = model_columns

            tables.append(copy.deepcopy(current_table))
            models.append(copy.deepcopy(current_model))

    yaml_file_content = {}
    yaml_file_content["tables"] = tables
    yaml_file_content["models"] = models

    yaml_file = open("salesforce.yml", "wt")
    yaml.dump(yaml_file_content, yaml_file, sort_keys=False)
    yaml_file.close()

    print("YAML file generated")

if __name__ == "__main__":
    df = read_csv()
    generate_sql(df)
    generate_yaml(df)
    