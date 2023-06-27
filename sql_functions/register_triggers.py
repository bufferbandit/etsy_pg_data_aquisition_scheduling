from connection import db


def register_trigger__notify_on_table_insert(channel_name, table_name, trigger_name):
	ps = db.prepare(
		"""
		--Create trigger
		CREATE OR REPLACE TRIGGER {trigger_name}
		AFTER INSERT ON {table_name}
		FOR EACH ROW EXECUTE FUNCTION cron.notify_for_newly_inserted_row('{channel_name}');
		""".format(
			trigger_name=trigger_name,
			table_name=table_name,
			channel_name=channel_name
		)
	)
	return ps()



def register_trigger__process_inserted_listing():
	ps = db.prepare(
		"""
		--Create trigger
		CREATE OR REPLACE TRIGGER process_inserted_listing
		AFTER INSERT ON cron.listings
		FOR EACH ROW EXECUTE FUNCTION cron.process_newly_added_listing();
		"""
	)
	return ps()


def register_trigger__pool_filled():
	ps = db.prepare("""
	CREATE OR REPLACE TRIGGER check_update_count
	AFTER UPDATE ON cron.pool
	FOR EACH ROW
	WHEN (NEW.pool_update_count >= NEW.pool_update_target)
	EXECUTE FUNCTION check_if_pool_is_filled_and_unschedule();
	""")
	return ps()