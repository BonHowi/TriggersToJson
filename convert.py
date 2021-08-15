import pandas as pd
import sys
from tqdm import tqdm
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import storage
import unidecode
import codecs
import json

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID_input = ${{ secrets.SPREADSHEET_ID }}
SAMPLE_RANGE_NAME = 'A1:AA68'
CREDENTIALS_FILE = 'credentials\credentials.json'


def import_from_sheets():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])

    if not values_input:
        print('No data found.')
    return values_input


def main():
    print("Loading data")
    values_input = import_from_sheets()
    df = pd.DataFrame(values_input[1:], columns=values_input[0])

    print("Transforming data")
    monsters_df = df[["name", "role", "type"]]
    monsters_df["type"] = pd.to_numeric(df["type"])

    triggers = df.drop(['name', 'role', 'type', 'id'], axis=1)
    triggers = triggers.applymap(lambda s: s.lower() if type(s) == str else s)
    # triggers = triggers.applymap(lambda s: unidecode.unidecode(s) if type(s) == str else s)

    triggers_list = []
    with tqdm(total=len(triggers), file=sys.stdout) as pbar:
        for row in triggers.itertuples(index=False):
            helpt = pd.Series(row)
            helpt = helpt[~helpt.isna()]
            # Drop empty strings
            helpt = pd.Series(filter(lambda x: len(x), helpt))
            # Copy strings with spaces without keeping them
            for trigger in helpt:
                trigger_nospace = trigger.replace(' ', '')
                if trigger_nospace != trigger:
                    helpt = helpt.append(pd.Series(trigger_nospace))
            helpt = helpt.drop_duplicates()
            triggers_list.append(helpt)
            pbar.update(1)

    print("Creating trigger structure")
    triggers_def = []
    with tqdm(total=len(triggers_list), file=sys.stdout) as pbar:
        for i in triggers_list:
            triggers_def.append(list(i))
            pbar.update(1)
    triggers_def_series = pd.Series(triggers_def)
    monsters_df.insert(loc=0, column='triggers', value=triggers_def_series)

    print("Creating output")
    types = {'id': [1, 0], 'label': ["Legendary", "Rare"]}
    types_df = pd.DataFrame(data=types)
    json_types = {'types': types_df, 'commands': monsters_df}

    # convert dataframes into dictionaries
    data_dict = {
        key: json_types[key].to_dict(orient='records')
        for key in json_types.keys()
    }

    print("Saving .json output")
    with open('output\monsters.json', 'w', encoding='utf8') as fp:
        json.dump(
            data_dict,
            fp,
            indent=4,
            ensure_ascii=False,
            sort_keys=False
        )
    print(".json saved")
    
    print("Saving .txt output")
    with open('output\monsters.txt', 'w', encoding='utf8') as fp:
        json.dump(
            data_dict,
            fp,
            indent=4,
            ensure_ascii=False
        )
    print(".txt saved")


if __name__ == "__main__":
    main()
