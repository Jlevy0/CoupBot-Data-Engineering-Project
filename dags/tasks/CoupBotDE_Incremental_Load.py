#Coupbot Data Engineering Project: Incremental Load
#This program is meant to regularly update the PSQL server with new versions of our CoupBot-generated CSV files.
#This program is scheduled to run daily with Airflow. Refer to Incremental_Load_Dag.py in the dags folder to see how.
#Databases are updated through psycopg2. 
import os
from dotenv import load_dotenv
import logging
import psycopg2 as pg 
load_dotenv()
logging.basicConfig(filename='Incremental_Load.log', filemode = 'a', level=logging.DEBUG,
format='%(name)s - %(levelname)s - %(message)s - %(asctime)s')
def truncate_tables():
    try:
        #Attempting to connect to the local PSQL server. Credentials are held in a secure .env file.
        connection = pg.connect(
            database = os.getenv('DATABASE'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
        )

    except Exception as errorMessage:
        print('Task Failed.')
        logging.error("Exception occurred", exc_info=True)

    cursor = connection.cursor()
    #Wiping our tables to prepare for the newest version of the CSV data.
    cursor.execute("""
        TRUNCATE TABLE ROSTER CASCADE;
        TRUNCATE TABLE CHANNELS CASCADE;
        TRUNCATE TABLE MESSAGE_LOGS CASCADE;
        TRUNCATE TABLE BAN_LOGS CASCADE;
        TRUNCATE TABLE COUP_ATTEMPTS CASCADE;
        TRUNCATE TABLE SUCCESSFUL_COUPS CASCADE;

            """
        )

    connection.commit()
    return


def update_tables():
    try:
        connection = pg.connect(
            database = os.getenv('DATABASE'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
        )

    except Exception as errorMessage:
        print(errorMessage)

    cursor = connection.cursor()
    #Now we'll grab the latest data.
    try:
        cursor.execute("""

            COPY ROSTER(member_name, member_id)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_roster.csv'
            WITH (FORMAT CSV, HEADER
            );

            COPY CHANNELS (channel_name, channel_id, channel_type)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_channels.csv'
            WITH (FORMAT CSV, HEADER
            );

            COPY MESSAGE_LOGS(message_id, author_id, channel_id, message_content, message_created_at)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_message_logs.csv'
            WITH (FORMAT CSV, HEADER
            );

            COPY BAN_LOGS (banned_person_id, banned_by_id, banned_at, reason)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_ban_logs.csv'
            WITH (FORMAT CSV, HEADER
            );

            COPY COUP_ATTEMPTS (current_king_id, called_on, called_by_id)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_coup_attempts.csv'
            WITH (FORMAT CSV, HEADER
            );

            COPY SUCCESSFUL_COUPS (new_king_id, crowned_on)
            FROM '/home/pi/Desktop/CoupBot-main/coupbot_successful_coups.csv'
            WITH (FORMAT CSV, HEADER
            );

                """
            )

        logging.info("Tables successfully updated.")

    except Exception as exceptionMessage:
        print('Task Failed.')
        logging.error("Exception occurred", exc_info=True)
    #And that's the incremental load done!
    connection.commit()
    return
