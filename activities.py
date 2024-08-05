from data import get_data
import pandas as pd
import datetime
import sqlite3


today = datetime.date.today()
start_of_last_week = today - datetime.timedelta(days=today.weekday()+8)
end_of_last_week = start_of_last_week + datetime.timedelta(days=6)

start_of_last_week = start_of_last_week.strftime('%d %b %Y')
end_of_last_week = end_of_last_week.strftime('%d %b %Y')


# Get Activities data
activities = get_data('/tracking/activities', params={'start_date': start_of_last_week}, pagination=500, json_data_reference='activities')

activity = []
for i in range(len(activities)):
    lst = activities[i]
    for j in range(len(lst)):
        activity.append(lst[j])

activities_df = pd.json_normalize(activity)[['id', 'event', 'created_at','message_html','candidate.id', 'candidate.name', 'admin.id', 'admin.name', 'offer.id', 'offer.title']]


# Connect to Database
conn = sqlite3.connect("C:\\Users\\nochum.paltiel\\Documents\\PycharmProjects\\recruitee_data_analysis\\recruitee.db")

# Append Activities dataframe to our database
activities_df.to_sql("activities", conn, if_exists='append', index=False)

# Close database
conn.close()
