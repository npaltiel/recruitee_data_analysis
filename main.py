import requests
import pandas as pd
import datetime
import sqlite3

f = open('API_token', 'r')
API_Token = f.read()
f.close()
base_url = 'https://api.recruitee.com/c/95238'

today = datetime.date.today()
start_of_last_week = today - datetime.timedelta(days=today.weekday()+8)
end_of_last_week = start_of_last_week + datetime.timedelta(days=6)

start_of_last_week = start_of_last_week.strftime('%d %b %Y')
end_of_last_week = end_of_last_week.strftime('%d %b %Y')

def get_data(endpoint, params=None, pagination=0, json_data_reference=''):
    if params is None:
        params = {}

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + API_Token
    }

    if pagination:

        limit, page = pagination, 1
        lst = []

        while True:
            params['page'] = page
            params['limit'] = limit

            response = requests.get(base_url + endpoint, headers=headers, params=params)

            if response.status_code != 200:
                print("Error: ", response.status_code)
                break

            data = response.json()

            if not data[json_data_reference]:
                break

            lst.append(data[json_data_reference])

            # Increment the offset by the limit
            page += 1

        return lst

    else:
        response = requests.get(base_url + endpoint, headers=headers, params=params)

        return response.json()[json_data_reference]


# Get Admins Table
admins_df = pd.json_normalize(get_data('/admins', json_data_reference='admins'))[['id', 'first_name', 'last_name']]
admins_df['full_name'] = admins_df['first_name'] + ' ' + admins_df['last_name']

# Get Activities data
activities = get_data('/tracking/activities', params={'start_date': start_of_last_week}, pagination=500, json_data_reference='activities')

activity = []
for i in range(len(activities)):
    lst = activities[i]
    for j in range(len(lst)):
        activity.append(lst[j])

activities_df = pd.json_normalize(activity)[['event', 'created_at','message_html','candidate.id', 'candidate.name', 'admin.id', 'admin.name', 'offer.id', 'offer.title']]

# Stage_changes table
stage_changes = activities_df[activities_df['event'] == 'candidate_stage_change']
# Tags table
tags = activities_df[activities_df['event'] == 'candidate_tags']

# Get past interviews data
past_interviews = get_data('/interview/events',params={'status': 'past_due'}, pagination=1000,json_data_reference='interview_events')

interviews = []
for i in range(len(past_interviews)):
    lst = past_interviews[i]
    for j in range(len(lst)):
        interviews.append(lst[j])

interviews_df = pd.json_normalize(interviews)[['id', 'admin_ids', 'starts_at', 'candidate_id', 'offer_id', 'kind']]

ids, interview_dates = [], []
for i in range(len(interviews_df['admin_ids'])):
    admin = interviews_df['admin_ids'][i]
    date = datetime.datetime.strptime(interviews_df['starts_at'][i], "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.timedelta(hours=4)
    if len(admin) > 0:
        ids.append(admin[0])
    else:
        ids.append("")
    interview_dates.append(date)

interviews_df = interviews_df.drop(columns=['admin_ids', 'starts_at'])
interviews_df['recruiter_ids'] = ids
interviews_df['interview_date'] = interview_dates


# Get Offers
offers_df = pd.json_normalize(get_data('/offers', json_data_reference='offers'))[['id', 'title']]

# Get Candidates
candidates = get_data('/candidates', json_data_reference='candidates')
all_candidate_data_df = pd.json_normalize(candidates)
candidates_df = all_candidate_data_df[['id', 'name', 'created_at', 'positive_ratings', 'source']]
placements_df = all_candidate_data_df['placements']

conn = sqlite3.connect("C:\\Users\\nochum.paltiel\\Documents\\PycharmProjects\\Recruitee_API\\recruitee.db")
conn.close()