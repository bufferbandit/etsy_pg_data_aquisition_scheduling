from postgresql.exceptions import UniqueError
from sql_functions.get_tables import *
from sql_functions.insertions import *
from client import client

from utils import process_insertion_trigger_notification



def new_listing_request_listener(notification):
	channel, payload, pid = notification
	# print("A new listing request has been send", payload)

	# Get the latest 100 listings active
	res = client.findAllListingsActive(limit=100, sort_on="created", sort_order="desc")
	count, results = res["count"], res["results"]

	print(f"Request made: {count=}")

	# Insert the count into the total_listings_count
	insert_into_total_listings_count(count)

	# Add a new row to the request_batch table to signify a new request has been made
	sql_res = insert_into_request_batches()
	request_batch_id = sql_res[0][0]

	# insert the results
	for request_batch_insertion_id,result in enumerate(results):
		try:
			insert_into_listings(
				listing_item=result,
				request_batch_id=request_batch_id,
				request_batch_insertion_id=request_batch_insertion_id,
				updated_count=1
			)
			# print(f"Row successfully inserted {result['listing_id']}")
		except UniqueError as e:
			# print("Unique error: ", e.details["detail"])
			continue




def new_job_run_details_listener(notification):
	# channel, payload, pid = process_insertion_trigger_notification(notification)
	# print("A new job has been inserted/started: ", channel, payload)
	pass



def update_listing_request_listener(notification):
	print("Update request received", notification)
	# process a listings pool update request
	# 1. Receive the pool id
	channel, payload, pid = notification
	pool_id = int(payload)

	# 2. Get the list of ids by the pool id
	listings_for_pool_id = [listing_tuple[0] for listing_tuple in get__listings_by_pool_id(pool_id)]
	print(listings_for_pool_id)

	# 3. Send a request to update the ids all at once
	# 4. Receive the results and insert them into the listings
	# 5. Increment the pool_update_count
	insert_update_pool_update_count(pool_id)

