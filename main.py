from functools import partial

from sql_functions.register_triggers import *
from sql_functions.register_functions import *
from sql_functions.client_invokable_functions import *
from sql_functions.create_tables import *
from sql_functions.get_tables import *
from listeners import *
from utils import *


def setup_sql():
	global LISTINGS_POOL_UPDATE_PATTERN,target_max_listings,max_threads,refresh_count_target, client_retries
	# register functions
	register__function_encode_var_as_json_and_base64()
	register__function_notify_for_newly_inserted_row()
	register__check_if_pool_is_filled_and_unschedule()
	register__materialize_candidate_pools_and_schedule_pool_update_request_notifs(LISTINGS_POOL_UPDATE_PATTERN)
	register__get_pool_candidates()
	register__function_process_newly_inserted_listing()
	register__materialize_pools(refresh_count_target)
	register__unschedule_all_jobs()
	register__cleanup_previous_run()
	register__count_unique_listings()
	register__get_highest_updated_count_counts()
	register__get_unique_listing_groups()

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
			"watch_get_new_listings_request": partial(new_listing_request_listener,
													  target_max_listings=target_max_listings,
													  results_in_request=results_in_request,
													  max_threads=max_threads),
			"watch_update_listings_pool_request": update_listing_request_listener,
			"watch_job_run_details": new_job_run_details_listener,
			"watch_pool": lambda notify: log_print("New pool added"),
			"watch_pool_filled": lambda notify: log_print(notify[1]),
			"default": lambda notification: ...
		}
	)
	log_print("Listeners started")


if __name__ == "__main__":

	max_threads = 10

	target_max_listings = 40000 #100000
	results_in_request = 100
	refresh_count_target = 0 # TODO: Set it to something actually useful
	client_retries = 5 # TODO: Actually use this in more places!!
	LISTINGS_REQUEST_INTERVAL = "10 seconds"
	# LISTINGS_REQUEST_INTERVAL = "*/1 * * * *"  # 1 minute
	# LISTINGS_REQUEST_INTERVAL = "*/5 * * * *"  # 5 minutes
	# LISTINGS_REQUEST_INTERVAL = "*/15 * * * *"  # 15 minutes

	# LISTINGS_POOL_UPDATE_PATTERN = "5 seconds"
	# LISTINGS_POOL_UPDATE_PATTERN = "*/10 * * * *"
	# LISTINGS_POOL_UPDATE_PATTERN = "0 */2 * * *"  # 2 hours
	LISTINGS_POOL_UPDATE_PATTERN = "*/30 * * * *"  # 30 minutes


	try:
		from client import client
		log_print(client.ping())
	except ConnectionRefusedError:
		exit("Error: RPC Server not started")

	log_print(db.prepare("SHOW search_path;")())

	setup_sql()
	time.sleep(4)

	log_print(db.proc("version()")())

	try:
		res = invokable__schedule_notification(
			schedule_name="get_new_listings_request",
			schedule_pattern=LISTINGS_REQUEST_INTERVAL,
			notification_channel="watch_get_new_listings_request",
			notification_message="GET_NEW_ITEMS_REQUEST",
			notification_stop_message="TARGET_MAX_LISTINGS_REACHED",
			target_max_listings=target_max_listings,
		)


		while 1: pass
	finally:
		invokable__unschedule_task("get_new_listings_request")
		log_print("Unscheduled")


