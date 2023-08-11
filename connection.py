import pg8000
import psycopg
import secrets
import postgresql

# Find a way to parameterize all of the queries

DB_USERNAME = "postgres"
DB_NAME = "postgres"
HOST = "192.168.68.111"
# HOST = "192.168.114.37"
PORT = 5432
SCHEMA = "cron"

db = postgresql.open(
	user=DB_USERNAME,
	password=secrets.DB_PASSWORD,
	host=HOST,
	port=PORT,
	database=DB_NAME)

db.settings["search_path"] = "$user," + SCHEMA
