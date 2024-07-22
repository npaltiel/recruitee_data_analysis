from data import get_data
import pandas as pd
import datetime
import sqlite3


today = datetime.date.today()
start_of_last_week = today - datetime.timedelta(days=today.weekday()+8)
end_of_last_week = start_of_last_week + datetime.timedelta(days=6)

start_of_last_week = start_of_last_week.strftime('%d %b %Y')
end_of_last_week = end_of_last_week.strftime('%d %b %Y')

# Get Admins Table
admins_df = pd.json_normalize(get_data('/admins', json_data_reference='admins'))[['id', 'first_name', 'last_name']]

# Get Activities data
activities = get_data('/tracking/activities', params={'start_date': start_of_last_week}, pagination=500, json_data_reference='activities')

activity = []
for i in range(len(activities)):
    lst = activities[i]
    for j in range(len(lst)):
        activity.append(lst[j])

activities_df = pd.json_normalize(activity)[['id', 'event', 'created_at','message_html','candidate.id', 'candidate.name', 'admin.id', 'admin.name', 'offer.id', 'offer.title']]


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

placements = []
for row in all_candidate_data_df['placements']:
    for pl in row:
        placements.append(pl)

placements_df = pd.json_normalize(placements)[['candidate_id', 'offer_id', 'stage_id']]

pipeline_templates = get_data('/pipeline_templates', json_data_reference='pipeline_templates')
template_ids = []
for template in pipeline_templates:
    template_ids.append(template['id'])

stage_ids = []
stage_names = []
for id in template_ids:
    pipeline = (get_data('/pipeline_templates/'+str(id), json_data_reference='pipeline_template'))
    all_stages = pipeline['stages']
    for stage in all_stages:
        stage_ids.append(stage['id'])
        stage_names.append(stage['name'])

stages_df = pd.DataFrame()
stages_df['stage_id'] = stage_ids
stages_df['stage_names'] = stage_names


# Create tables in database
conn = sqlite3.connect("C:\\Users\\nochum.paltiel\\Documents\\PycharmProjects\\recruitee_data_analysis\\recruitee.db")

# Create all tables
admins_df.to_sql("admins", conn)
activities_df.to_sql("activities", conn)
interviews_df.to_sql("interviews", conn)
candidates_df.to_sql("candidates", conn)
placements_df.to_sql("placements", conn)
offers_df.to_sql("jobs", conn)
stages_df.to_sql("stages", conn)

conn.close()

