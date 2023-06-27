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
	# process a listings pool update request
	# 1. Receive the pool id
	channel, payload, pid = notification
	pool_id = int(payload)

	# 2. Get the list of ids by the pool id
	listings_for_pool_id = [listing_tuple[0] for listing_tuple in get__listings_by_pool_id(pool_id)]

	# 3. Send a request to update the ids all at once
	res = client.getListingsByListingIds(listing_ids=str(listings_for_pool_id)[1:-1])

	# 4. Add a new row to the request_batch table to signify a new request has been made
	sql_res = insert_into_request_batches()
	request_batch_id = sql_res[0][0]

	print("Update request received for pool: ", pool_id, " for #", str(len(listings_for_pool_id)), " listings")

	# 5. Receive the results and insert them into the listings
	for request_batch_insertion_id, updated_listing in enumerate(res["results"]):
		# Get the request times count, which represents the time the listing was requested
		get__update_count_by_listing_id_query = get__update_count_by_listing_id(updated_listing["listing_id"])
		try:
			insert_into_listings(
				listing_item=updated_listing,
				request_batch_id=request_batch_id,
				request_batch_insertion_id=request_batch_insertion_id,
				updated_count=get__update_count_by_listing_id_query[0][0] + 1
			)
			print("Item successfully updated")
		except UniqueError as e:
			print("Item has strangely enough already been updated: ", e.details["detail"])
			continue

	# 6. Increment the pool update count
	insert_update_pool_update_count(pool_id)



