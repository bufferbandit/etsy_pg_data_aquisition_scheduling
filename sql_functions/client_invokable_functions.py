import postgresql
from connection import db





def invokable__schedule_notification(schedule_name, schedule_pattern, notification_channel, notification_message):
	ps = db.prepare(
		"""
		-- Schedule a job
		SELECT cron.schedule(  $1::text,     $2::text,   CONCAT('NOTIFY', ' ', $3::text, ',', ' ', $4::text )     );
		""")
	return ps(
		schedule_name,
		schedule_pattern,
		notification_channel,
		postgresql.string.quote_literal(
			notification_message
		)
	)


def invokable__unschedule_task(schedule_name):
	ps = db.prepare("""SELECT cron.unschedule($1);""")
	return ps(schedule_name)
