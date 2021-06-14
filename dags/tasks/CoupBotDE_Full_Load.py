#Coupbot Data Engineering Project: Full Load
#This program is meant to set up the PSQL tables we'll be loading our data into.
#Databases are created through psycopg2. 
#The incremental load file will then regularly update these tables with new data.
import os
from dotenv import load_dotenv
import logging
import psycopg2 as pg 
load_dotenv()
logging.basicConfig(filename='Full_Load.log', filemode = 'a', level=logging.DEBUG,
format='%(name)s - %(levelname)s - %(message)s - %(asctime)s')

def create_tables():
    try:
        #Attempting to connect to the local PSQL server. Credentials are held in a secure .env file.
        connection = pg.connect(
            database = os.getenv('DATABASE'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
        )

    except Exception as errorMessage:
        print(errorMessage)
        logging.error("Exception occurred", exc_info=True)

    cursor = connection.cursor()
    #These tables will match the appropriate data we're holding in the CSV files CoupBot writes to.
    #Just in case you were wondering, the ID for Discord objects are all 17 digit integers, making BIGINT a necessity.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ROSTER (
                member_id BIGINT PRIMARY KEY,
                member_name VARCHAR(40)
                );

        CREATE TABLE IF NOT EXISTS CHANNELS (
                channel_id BIGINT PRIMARY KEY,
                channel_name VARCHAR(40),
                channel_type VARCHAR(15)
                );

        CREATE TABLE IF NOT EXISTS MESSAGE_LOGS (
                message_id BIGINT PRIMARY KEY,
                author_id BIGINT,
                channel_id BIGINT,
                message_content TEXT,
                message_created_at TIMESTAMP,

                CONSTRAINT fk_roster_message_logs
                    FOREIGN KEY (author_id)
                        REFERENCES roster(member_id),

                CONSTRAINT fk_channels_message_logs
                    FOREIGN KEY (channel_id)
                        REFERENCES channels(channel_id)
                );

        CREATE TABLE IF NOT EXISTS BAN_LOGS (
                banned_person_id BIGINT,
                banned_by_id BIGINT,
                banned_at TIMESTAMP,
                reason TEXT,

                CONSTRAINT fk_roster_ban_logs_banned_person
                    FOREIGN KEY (banned_person_id)
                        REFERENCES roster(member_id),

                CONSTRAINT fk_roster_ban_logs_banned_by
                    FOREIGN KEY (banned_by_id)
                        REFERENCES roster(member_id)
                );

        CREATE TABLE IF NOT EXISTS COUP_ATTEMPTS (
                current_king_id BIGINT,
                called_by_id BIGINT,
                called_on TIMESTAMP,
            
                CONSTRAINT fk_roster_coup_attempts_current_king
                    FOREIGN KEY (current_king_id)
                            REFERENCES roster(member_id),

                CONSTRAINT fk_roster_coup_attempts_called_by
                    FOREIGN KEY (called_by_id)
                            REFERENCES roster(member_id)
                );

        CREATE TABLE IF NOT EXISTS SUCCESSFUL_COUPS (
                new_king_id BIGINT,
                crowned_on TIMESTAMP,

                CONSTRAINT fk_roster_successful_coups
                    FOREIGN KEY (new_king_id)
                        REFERENCES roster(member_id)
                );
                """
        )
    #Great, we've set up our schema. Now to commit and begin pushing data into the tables.
    connection.commit()
    return

def load_data_to_tables():
    try:
        #Attempting to connect to the local PSQL server. Credentials are held in a secure .env file.
        connection = pg.connect(
            database = os.getenv('DATABASE'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
        )

    except Exception as errorMessage:
        print(errorMessage)
        logging.error("Exception occurred", exc_info=True)

    cursor = connection.cursor()

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
    #That's it for the full load. 
    #But since the data is extracted from an active Discord server, we should regularly update this database.
    #Refer to CoupBotDE_Incremental_Load.py to see how I've done that!
    connection.commit()
    return
