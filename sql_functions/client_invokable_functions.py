import postgresql
from connection import db
from utils import log_print


def invokable__schedule_notification(schedule_name, schedule_pattern, notification_channel,
									 notification_message,notification_stop_message, target_max_listings):


	'''

	IF NOT EXISTS (SELECT * FROM count_unique_listings() AS count WHERE count > {5}) THEN
							NOTIFY {2}, '{3}';
						ELSE
							NOTIFY {2}, '{4}';
						END IF;
	'''

	s = """
		-- Schedule a job
		SELECT cron.schedule(  
					'{0}',     
					'{1}',
					
					$$
						DO ' BEGIN
							IF NOT EXISTS (SELECT * FROM count_unique_listings() AS count WHERE count > {5}) THEN
								NOTIFY {2}, ''{3}'';
							ELSE
								NOTIFY {2}, ''{4}'';
							END IF;
						END
						' LANGUAGE plpgsql;
					$$ 
		)
		""".format(
			schedule_name,
			schedule_pattern,
			notification_channel,
			notification_message,
			notification_stop_message,
			target_max_listings
		)

	# log_print(s)
	ps = db.prepare(s)
	return ps()



	# return ps(
	# 	schedule_name,
	# 	schedule_pattern,
	# 	# postgresql.string.quote_literal(
	# 		notification_channel,
	# 	# ),
	# 	postgresql.string.quote_literal(
	# 		notification_message
	# 	),
	# 	target_max_listings,
	# 	"TARGET_MAX_LISTINGS_REACHED"
	# )


def invokable__unschedule_task(schedule_name):
	ps = db.prepare("""SELECT cron.unschedule($1);""")
	return ps(schedule_name)
