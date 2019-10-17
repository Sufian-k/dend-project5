class SqlQueries:
    
    ################################################
    # The queries used when the insert mood truncate
    songplay_table_insert = ("""
        SELECT 
            DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
        WHERE se.page = 'NextSong' 
    """)

    user_table_insert = ("""
        SELECT 
            DISTINCT se.userId AS user_id, 
            se.firstName AS first_name, 
            se.lastName AS last_name, 
            se.gender AS gender, 
            se.level AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong'
    """)

    song_table_insert = ("""
        SELECT 
            DISTINCT ss.song_id AS song_id,
            ss.title AS title,
            ss.artist_id AS artist_id,
            ss.year AS year,
            ss.duration AS duration
        FROM staging_songs AS ss;
    """)

    artist_table_insert = ("""
        SELECT  
            DISTINCT ss.artist_id AS artist_id,
            ss.artist_name AS name,
            ss.artist_location AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude
        FROM staging_songs AS ss;
    """)

    time_table_insert = ("""
        SELECT  
            DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dow FROM start_time) AS weekday
        FROM staging_events AS se
        WHERE se.page = 'NextSong';
    """)
    
    
    ##############################################
    # The queries used when the insert mood append
    songplay_table_append = ("""
        SELECT 
            DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
        WHERE se.page = 'NextSong' AND NOT EXISTS(SELECT start_time 
                                                  FROM {} 
                                                  WHERE start_time = {}.start_time) 
    """)

    user_table_append = ("""
        SELECT 
        DISTINCT se.userId AS user_id, 
            se.firstName AS first_name, 
            se.lastName AS last_name, 
            se.gender AS gender, 
            se.level AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong' AND NOT EXISTS(SELECT user_id 
                                                  FROM {} 
                                                  WHERE se.userId = {}.user_id)
    """)

    song_table_append = ("""
        SELECT  
            DISTINCT ss.song_id AS song_id,
            ss.title AS title,
            ss.artist_id AS artist_id,
            ss.year AS year,
            ss.duration AS duration
        FROM staging_songs AS ss
        WHERE NOT EXISTS(SELECT song_id
                         FROM {}
                         WHERE ss.song_id = {}.song_id)
    """)

    artist_table_append = ("""
        SELECT  
            DISTINCT ss.artist_id AS artist_id,
            ss.artist_name AS name,
            ss.artist_location AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude
        FROM staging_songs AS ss
        WHERE NOT EXISTS(SELECT song_id
                         FROM {}
                         WHERE ss.artist_id = {}.artist_id);
    """)

    time_table_append = ("""
        SELECT  
            DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dow FROM start_time) AS weekday
        FROM staging_events AS se
        WHERE se.page = 'NextSong' AND NOT EXISTS(SELECT start_time
                                                  FROM {}
                                                  WHERE start_time = {}.start_time);
    """)
    
    #######################
    # Quality Check Queries
    
    songplay_table_check = ("""
        SELECT COUNT(*)
        FROM {}
        WHERE songplay_id IS NULL OR start_time IS NULL OR user_id IS NULL
    """)
    
    user_table_check = ("""
        SELECT COUNT(*)
        FROM {}
        WHERE user_id IS NULL
    """)
    
    song_table_check = ("""
        SELECT COUNT(*)
        FROM {}
        WHERE song_id IS NULL
    """)
    
    artist_table_check = ("""
        SELECT COUNT(*)
        FROM {}
        WHERE artist_id IS NULL
    """)
    
    time_table_check = ("""
        SELECT COUNT(*)
        FROM {}
        WHERE start_time IS NULL
    """)