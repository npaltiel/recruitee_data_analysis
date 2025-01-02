from data import get_data
from add_to_db import insert_or_replace
import pandas as pd
import datetime
import sqlite3

today = datetime.date.today()
start_of_last2_week = today - datetime.timedelta(days=today.weekday() + 43)
end_of_last_week = start_of_last2_week + datetime.timedelta(days=13)

start_of_last2_week = start_of_last2_week.strftime('%d %b %Y')
end_of_last_week = end_of_last_week.strftime('%d %b %Y')

start_of_2024 = "2024-01-01T00:00:00"

# Get Admins Table
admins_df = pd.json_normalize(get_data('/admins', json_data_reference='admins'))[['id', 'first_name', 'last_name']]
admins_df.rename(columns={'id': 'admin_id'}, inplace=True)

# Get Activities data
activities = get_data('/tracking/activities', params={'start_date': start_of_last2_week},
                      pagination=500,
                      json_data_reference='activities')

activity = [item for sublist in activities for item in sublist]
activities_df = pd.json_normalize(activity)[
    ['id', 'event', 'created_at', 'message_html', 'candidate.id', 'admin.id', 'offer.id']]
activities_df.rename(
    columns={'id': 'activity_id', 'candidate.id': 'candidate_id', 'admin.id': 'admin_id', 'offer.id': 'offer_id'},
    inplace=True)

# Get past interviews data
past_interviews = get_data('/interview/events', params={'status': 'past_due'}, pagination=1000,
                           json_data_reference='interview_events')
interviews = [item for sublist in past_interviews for item in sublist]
interviews_df = pd.json_normalize(interviews)[
    ['id', 'admin_id', 'admin_ids', 'starts_at', 'candidate_id', 'offer_id', 'kind']]

# Process admin_ids
interviews_df.rename(columns={'id': 'interview_id', 'starts_at': 'interview_date', 'admin_id': 'scheduler_id'},
                     inplace=True)
interviews_df['admin_id'] = interviews_df['admin_ids'].apply(lambda x: x[0] if len(x) > 0 else None)
interviews_df.drop(columns=['admin_ids'], inplace=True)

# Get Offers
jobs_df = pd.json_normalize(get_data('/offers', json_data_reference='offers'))[['id', 'title']]
jobs_df.rename(columns={'id': 'offer_id', 'title': 'offer_title'}, inplace=True)

# Get Candidates
candidates = get_data('/candidates', params={'created_after': start_of_2024}, json_data_reference='candidates')
all_candidate_data_df = pd.json_normalize(candidates)
candidates_df = all_candidate_data_df[['id', 'name', 'created_at', 'positive_ratings', 'admin_id']].copy()
candidates_df.rename(columns={'id': 'candidate_id', 'name': 'candidate_name', 'admin_id': 'source'}, inplace=True)

# Get Placements
placements = [pl for row in all_candidate_data_df['placements'] for pl in row]
placements_df = pd.json_normalize(placements)[
    ['candidate_id', 'offer_id', 'stage_id', 'disqualified_at', 'disqualify_reason']]
placements_df.rename(columns={'disqualify_reason': 'disqualified_reason'}, inplace=True)

# Get stages
pipeline_templates = get_data('/pipeline_templates', json_data_reference='pipeline_templates')
template_ids = [template['id'] for template in pipeline_templates]

stage_id = []
stage_name = []
for id in template_ids:
    pipeline = (get_data('/pipeline_templates/' + str(id), json_data_reference='pipeline_template'))
    all_stages = pipeline['stages']
    for stage in all_stages:
        stage_id.append(stage['id'])
        stage_name.append(stage['name'])
stages_df = pd.DataFrame({'stage_id': stage_id, 'stage_name': stage_name})

# Create tables in database
conn = sqlite3.connect("C:\\Users\\nochum.paltiel\\Documents\\PycharmProjects\\recruitee_data_analysis\\recruitee2.db")
cur = conn.cursor()

# Create all tables
insert_or_replace("admins", admins_df, conn)
insert_or_replace("activities", activities_df, conn)
insert_or_replace("interviews", interviews_df, conn)
insert_or_replace("candidates", candidates_df, conn)
insert_or_replace("placements", placements_df, conn)
insert_or_replace("jobs", jobs_df, conn)
insert_or_replace("stages", stages_df, conn)

cur.close()
conn.close()
