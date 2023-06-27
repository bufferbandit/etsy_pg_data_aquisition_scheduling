from sql_functions.register_triggers import *
from sql_functions.register_functions import *
from sql_functions.client_invokable_functions import *
from sql_functions.create_tables import *
from sql_functions.get_tables import *
from listeners import *
from utils import *


def setup_sql(LISTINGS_POOL_UPDATE_PATTERN):
	# register functions
	register__function_encode_var_as_json_and_base64()
	register__function_notify_for_newly_inserted_row()
	register__check_if_pool_is_filled_and_unschedule()
	register__materialize_candidate_pools_and_schedule_pool_update_request_notifs(LISTINGS_POOL_UPDATE_PATTERN)
	register__get_pool_candidates()
	register__function_process_newly_inserted_listing()
	register__materialize_pools()

	# create tables
	create_table__request_batch()
	create_table__total_listings_count()
	create_table__responses()
	create_table__listing_pool_lookup()
	create_table__pool()



	# register triggers
	register_trigger__process_inserted_listing()
	register_trigger__pool_filled()

	# set insertion notification triggers
	register_trigger__notify_on_table_insert(
		trigger_name="notify_newly_added_job_run_details_row",
		table_name="job_run_details",
		channel_name="watch_job_run_details"
	)

	register_trigger__notify_on_table_insert(
		trigger_name="notify_newly_added_pool",
		table_name="pool",
		channel_name="watch_pool"
	)



	# start listening
	listen_to_channels_threaded(
		[
			"watch_get_new_listings_request",
			"watch_update_listings_pool_request",
			"watch_job_run_details",
			"watch_pool_filled",
			"watch_pool",
		],
		{
			"watch_get_new_listings_request": new_listing_request_listener,
			"watch_update_listings_pool_request": update_listing_request_listener,
			"watch_job_run_details": new_job_run_details_listener,
			"watch_pool": lambda notify: print("New pool added"),
			"watch_pool_filled": lambda notify: print("Pool filled!!!!"),
			"default": lambda notification: ...
		}
	)
	print("Listeners started")


if __name__ == "__main__":


	refresh_count_target = 7
	LISTINGS_REQUEST_INTERVAL = "10 seconds"
	LISTINGS_POOL_UPDATE_PATTERN = "5 seconds"
	# LISTINGS_REQUEST_INTERVAL = "*/5 * * * *"

	from client import client

	print(client.ping())

	print(db.prepare("SHOW search_path;")())

	setup_sql(LISTINGS_POOL_UPDATE_PATTERN)
	time.sleep(4)

	print(db.proc("version()")())

	try:
		res = invokable__schedule_notification(
			schedule_name="get_new_listings_request",
			schedule_pattern=LISTINGS_REQUEST_INTERVAL,
			notification_channel="watch_get_new_listings_request",
			notification_message="GET_NEW_ITEMS_REQUEST"
		)


		while 1: pass
	finally:
		invokable__unschedule_task("get_new_listings_request")
		print("Unscheduled")