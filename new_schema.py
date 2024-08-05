import sqlite3

# Connect to the existing SQLite database
conn = sqlite3.connect("C:\\Users\\nochum.paltiel\\Documents\\PycharmProjects\\recruitee_data_analysis\\recruitee.db")
cur = conn.cursor()

# Begin a transaction
conn.execute('BEGIN TRANSACTION')

# 1. Rename old tables
cur.execute('ALTER TABLE admins RENAME TO old_admins')
cur.execute('ALTER TABLE activities RENAME TO old_activities')
cur.execute('ALTER TABLE interviews RENAME TO old_interviews')
cur.execute('ALTER TABLE candidates RENAME TO old_candidates')
cur.execute('ALTER TABLE placements RENAME TO old_placements')
cur.execute('ALTER TABLE jobs RENAME TO old_jobs')
cur.execute('ALTER TABLE stages RENAME TO old_stages')

# 2. Create new tables according to the new schema
cur.execute('''
    CREATE TABLE admins (
        admin_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT
    )
''')

cur.execute('''
    CREATE TABLE activities (
        activity_id INTEGER PRIMARY KEY,
        event TEXT,
        created_at TEXT,
        message_html TEXT,
        candidate_id INTEGER,
        admin_id INTEGER,
        offer_id INTEGER,
        FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
        FOREIGN KEY (admin_id) REFERENCES admins(admin_id),
        FOREIGN KEY (offer_id) REFERENCES jobs(offer_id)
    )
''')

cur.execute('''
    CREATE TABLE interviews (
        interview_id INTEGER PRIMARY KEY,
        interview_date TEXT,
        candidate_id INTEGER,
        offer_id INTEGER,
        admin_id INTEGER,
        kind TEXT,
        FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
        FOREIGN KEY (offer_id) REFERENCES jobs(offer_id),
        FOREIGN KEY (admin_id) REFERENCES admins(admin_id)
    )
''')

cur.execute('''
    CREATE TABLE candidates (
        candidate_id INTEGER PRIMARY KEY,
        candidate_name TEXT,
        created_at TEXT,
        positive_ratings INTEGER,
        source TEXT
    )
''')

cur.execute('''
    CREATE TABLE placements (
        candidate_id INTEGER,
        offer_id INTEGER,
        stage_id INTEGER,
        disqualified_at TEXT,
        disqualified_reason TEXT,
        PRIMARY KEY (candidate_id, offer_id),
        FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
        FOREIGN KEY (offer_id) REFERENCES jobs(offer_id),
        FOREIGN KEY (stage_id) REFERENCES stages(stage_id)
    )
''')

cur.execute('''
    CREATE TABLE jobs (
        offer_id INTEGER PRIMARY KEY,
        offer_title TEXT
    )
''')

cur.execute('''
    CREATE TABLE stages (
        stage_id INTEGER PRIMARY KEY,
        stage_name TEXT
    )
''')

# 3. Migrate data from old tables to new tables
cur.execute('''
    INSERT INTO admins (admin_id, first_name, last_name)
    SELECT id, first_name, last_name FROM old_admins
''')

# Insert unique records into the new activities table
cur.execute('''
    DELETE FROM old_activities
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM old_activities
        GROUP BY id
    )
''')

cur.execute('''
    INSERT INTO activities (activity_id, event, created_at, message_html, candidate_id, admin_id, offer_id)
    SELECT id, event, created_at, message_html, "candidate.id", "admin.id", "offer.id"
    FROM old_activities
''')

# Insert unique records into the new interviews table
cur.execute('''
    DELETE FROM old_interviews
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM old_interviews
        GROUP BY id
    )
''')

cur.execute('''
    INSERT INTO interviews (interview_id, interview_date, candidate_id, offer_id, admin_id, kind)
    SELECT id, interview_date, candidate_id, offer_id, recruiter_ids, kind
    FROM old_interviews
''')

cur.execute('''
    INSERT INTO candidates (candidate_id, candidate_name, created_at, positive_ratings, source)
    SELECT id, name, created_at, positive_ratings, source
    FROM old_candidates
''')

cur.execute('''
    INSERT INTO placements (candidate_id, offer_id, stage_id, disqualified_at, disqualified_reason)
    SELECT candidate_id, offer_id, stage_id, disqualified_at, disqualify_reason
    FROM old_placements
''')

cur.execute('''
    INSERT INTO jobs (offer_id, offer_title)
    SELECT id, title FROM old_jobs
''')

cur.execute('''
    INSERT INTO stages (stage_id, stage_name)
    SELECT stage_id, stage_names FROM old_stages
''')

# 4. Drop the old tables
cur.execute('DROP TABLE old_admins')
cur.execute('DROP TABLE old_activities')
cur.execute('DROP TABLE old_interviews')
cur.execute('DROP TABLE old_candidates')
cur.execute('DROP TABLE old_placements')
cur.execute('DROP TABLE old_jobs')
cur.execute('DROP TABLE old_stages')

# Commit the transaction
conn.commit()

cur.close()
# Close the connection
conn.close()
