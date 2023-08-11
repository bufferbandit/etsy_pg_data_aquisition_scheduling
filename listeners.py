from concurrent.futures import ThreadPoolExecutor

from jsonrpclib import ProtocolError
from postgresql.exceptions import UniqueError

from sql_functions.client_invokable_functions import invokable__unschedule_task
from sql_functions.get_tables import *
from sql_functions.insertions import *
from client import client

from utils import process_insertion_trigger_notification



def new_listing_request_listener(notification,
								 target_max_listings,
								 results_in_request,
								 max_threads):


	# pool = ThreadPoolExecutor(max_workers=max_threads)

	channel, payload, pid = notification

	# If we reached the max listings then unschedule the new listings schedule
	if payload == "TARGET_MAX_LISTINGS_REACHED":
		invokable__unschedule_task("get_new_listings_request")
		log_print("Stop command received, unscheduling get new listings")
		return



	paginated_request_id = insert_into_request_batches("paginated_request_overarching")[0][0]
	all_results = []

	tasks = []

	def wrapper(offset, limit):

		client_retries = 3
		for count in range(client_retries):
			try:
				res = client.findAllListingsActive(offset=offset, limit=limit, sort_on="created", sort_order="desc")
				break
			except ProtocolError as e:
				if "Remote end closed connection without response" in str(e):
					print("'Remote end closed connection without response' exception occurred, retrying, ", str(count))
					continue


		# unpackoo
		count, results = res["count"], res["results"]
		log_print(f"New request made: {offset=}, {limit=} {len(results)=}, etsy_count={count}")

		# append result to all results
		all_results.append(results)

		# Insert the count into the total_listings_count
		insert_into_total_listings_count(count)

		# Add a new row to the request_batch table to signify a new request has been made
		request_batch_id = insert_into_request_batches("new_singular_request")[0][0]


		# insert the results

		# insert_into_listings_batch(
		#
		# 	listing_items=results,
		# 	request_batch_id=request_batch_id,
		# 	request_batch_insertion_id=-1,
		# 	updated_count=1,
		# 	offset=offset,
		# 	paginated_request_id=paginated_request_id
		#
		# )

		insert_into_listings_threaded(results, request_batch_id, paginated_request_id=paginated_request_id)



		# for request_batch_insertion_id, result in enumerate(results):
		# 	try:
		# 		insert_into_listings(
		# 			listing_item=result,
		# 			request_batch_id=request_batch_id,
		# 			request_batch_insertion_id=request_batch_insertion_id,
		# 			updated_count=1,
		# 			offset=offset,
		# 			paginated_request_id=paginated_request_id
		# 		)
		# 		# log_print(f"Row successfully inserted {result['listing_id']}")
		# 	except UniqueError as e:
		# 		log_print("Unique error: ", e.details["detail"])
		# 		continue
		# 	except Exception as e:
		# 		log_print("Other error occured during insersion: ", e)
		# 		continue

		return results


	####

	"""
	
	When we receive a "get new items request", instead of just making 1 request (of 100 items)
	 we can also specify the number of times we want to make the new items request.
	Lets take the scenario in which we want for every new request get 10k items. In that case
	 we should create a loop that does the following:
	 
	 	- Loop through times 10k / 100 -> max / results_in_request
	 	- increment the offset every time, but instead of multiplying it, do it by step size
			
			def range(start,stop,step):
				...
				
		   start at 0
		   stop at target_max_listings
		   steps of 100 (instead of 1)
		   
	"""

	for offset in range(0, target_max_listings, results_in_request):
		# task = pool.submit(wrapper, limit=results_in_request, offset=offset)
		# tasks.append(task)

		# Wrapper to actually make the request
		wrapper(offset, results_in_request)

	thread_results = [task.result() for task in tasks]





def new_job_run_details_listener(notification):
	# channel, payload, pid = process_insertion_trigger_notification(notification)
	# channel, payload, pid = notification
	# log_print("A new job has been inserted/started: ", channel, payload)
	pass



def update_listing_request_listener(notification):
	# process a listings pool update request
	# 1. Receive the pool id
	channel, payload, pid = notification
	pool_id = int(payload)

	# 2. Get the list of ids by the pool id
	listings_for_pool_id = [listing_tuple[0] for listing_tuple in get__listings_by_pool_id(pool_id)]

	# 3. Send a request to update the ids all at once
	client_retries = 3

	for count in range(client_retries):
		try:
			res = client.getListingsByListingIds(listing_ids=str(listings_for_pool_id)[1:-1])
			break
		except ProtocolError as e:
			if "Remote end closed connection without response" in str(e):
				print("'Remote end closed connection without response' exception occurred, retrying, ", str(count))
				continue




	# 4. Add a new row to the request_batch table to signify a new request has been made
	sql_res = insert_into_request_batches("update_request")
	request_batch_id = sql_res[0][0]

	log_print("Update request received for pool: ", pool_id, " for ", str(len(listings_for_pool_id)), " listings")

	# 5. Receive the results and insert them into the listings

	insert_into_listings_threaded(res["results"], request_batch_id)

	# for request_batch_insertion_id, updated_listing in enumerate(res["results"]):
	# 	# Get the request times count, which represents the time the listing was requested
	# 	get__update_count_by_listing_id_query = get__update_count_by_listing_id(updated_listing["listing_id"])
	# 	try:
	# 		insert_into_listings(
	# 			listing_item=updated_listing,
	# 			request_batch_id=request_batch_id,
	# 			request_batch_insertion_id=request_batch_insertion_id,
	# 			updated_count=get__update_count_by_listing_id_query[0][0] + 1
	# 		)
	# 		# log_print("Item successfully updated")
	# 	except UniqueError as e:
	# 		log_print("Item has strangely enough already been updated: ", e.details["detail"])
	# 		continue

	# 6. Increment the pool update count
	insert_update_pool_update_count(pool_id)



