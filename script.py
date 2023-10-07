import base64
import os.path
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
BATCH_SIZE = 10
DELAY_BETWEEN_BclearATCHES = 1  # seconds


def get_service():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json')

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

    # This line ensures 'service' is always initialized regardless of the condition.
    service = build('gmail', 'v1', credentials=creds)

    return service


def unstar_emails(service):
    page_token = None
    all_messages = []

    # Paginate through all starred emails
    while True:
        results = service.users().messages().list(
            userId='me', q='is:starred', pageToken=page_token).execute()
        messages = results.get('messages', [])
        all_messages.extend(messages)

        page_token = results.get('nextPageToken')
        if not page_token:
            break

    for i in range(0, len(all_messages), BATCH_SIZE):
        batch = service.new_batch_http_request(callback=callback)
        for message in all_messages[i:i + BATCH_SIZE]:
            batch.add(service.users().messages().modify(
                userId='me',
                id=message['id'],
                body={'removeLabelIds': ['STARRED']}
            ))
        try:
            batch.execute()
            print(f"Processed batch ending with index {i + BATCH_SIZE}")
            time.sleep(DELAY_BETWEEN_BATCHES)
        except HttpError as e:
            if e.resp.status == 429:  # Too Many Requests
                print("Rate limit exceeded. Waiting for 120 seconds before continuing.")
                time.sleep(120)
            else:
                print(f"Encountered error: {e}")

    print(f"Unstarred {len(all_messages)} emails!")


def callback(request_id, response, exception):
    if exception is not None:
        print(exception)


if __name__ == '__main__':
    service = get_service()
    unstar_emails(service)
