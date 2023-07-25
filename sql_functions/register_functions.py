from connection import db


def register__function_encode_var_as_json_and_base64():
	res = db.prepare(
		"""
		--- Encoding function
		CREATE OR REPLACE FUNCTION encode_var_as_json_and_base64(input_value anyelement)
		RETURNS text AS $$
		DECLARE
			json_text text := to_json(input_value);
			base64_text text := json_text; 
			-- This b64 encode somehow is broken and has to 
			-- be disabled for the time being encode(json_text::bytea, 'base64');
		BEGIN
			RETURN base64_text;
		END;
		$$ LANGUAGE plpgsql;
		"""
	)
	return res()


def register__function_notify_for_newly_inserted_row():
	res = db.prepare(
		"""
		--- Callback that sends notifications when trigger goes of
		CREATE OR REPLACE FUNCTION notify_for_newly_inserted_row() RETURNS TRIGGER AS $$
		DECLARE
		  	channel_name text := TG_ARGV[0];
		BEGIN
			PERFORM pg_notify(channel_name, cron.encode_var_as_json_and_base64(NEW));
		  	RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		"""
	)
	return res()




def register__get_pool_candidates():
	res = db.prepare(
	"""
		-- get candidate pools
		CREATE OR REPLACE FUNCTION get_candidate_pools() RETURNS TABLE (
			candidate_pool_id numeric,
			pool_size bigint,
			pool_candidates numeric[]
		) AS $$
		BEGIN
			RETURN QUERY
			
			WITH numbered_items AS (
					SELECT
						listing.listing_id,
						ROW_NUMBER() OVER (ORDER BY request_batch_id ASC, requested_at ASC) AS row_num,
						CEILING(ROW_NUMBER() OVER (ORDER BY request_batch_id) / 100.0) AS candidate_pool_id2
					FROM
						cron.listings listing
					LEFT JOIN
    					cron.listing_pool_lookup pool ON listing.listing_id = pool.listing_id
					WHERE 
						-- Check if the listing_id is not in an existing pool already
						-- pool.listing_id IS NULL
						-- pool.pool_id IS NULL
						NOT EXISTS (
							SELECT 1
							FROM cron.listing_pool_lookup
							WHERE listing_id = listing.listing_id
						)
				)
			SELECT
				candidate_pool_id2 as candidate_pool_id,
				COUNT(listing_id) AS pool_size,
				ARRAY_AGG(listing_id) AS pool_candidates
			
			FROM 
				numbered_items
			
			GROUP BY 
				candidate_pool_id2
				
			HAVING
				COUNT(listing_id) = 100
				
			ORDER BY
				candidate_pool_id2
			;	
					
		END;
		$$ LANGUAGE plpgsql;
	""")
	return res()


def register__materialize_pools(refresh_count_target):
	res = db.prepare(
	"""
	CREATE OR REPLACE FUNCTION materialize_pools() RETURNS INT[] AS $$
	from plpy import execute, info, prepare
	
	POOL_UPDATE_TARGET = {refresh_count_target}

	# Get the candidate pools
	candidate_pools = execute("SELECT * FROM get_candidate_pools()")

	# Create prepared statement for insertion
	insert_into_listing_pool_lookup = prepare("INSERT INTO cron.listing_pool_lookup(pool_id, listing_id) VALUES ($1, $2)", ["int", "int"])
	insert_into_pool = prepare("INSERT INTO cron.pool(pool_id, pool_update_target) VALUES ($1, $2)", ["int", "int"])

	# Get the highest previous pool id
	get_highest_pool_id_query = execute("SELECT COALESCE(MAX(pool_id), 0) + 1 FROM cron.listing_pool_lookup")
	get_highest_pool_id  = int(get_highest_pool_id_query[0]["?column?"])
	
	materialized_pools_ids = []

	# Loop through candidate pools
	for count, row in enumerate(candidate_pools, start=0):

		# Generate a new id
		new_pool_id = get_highest_pool_id + count
		
		# Create the pool
		execute(insert_into_pool, [new_pool_id, POOL_UPDATE_TARGET])
		
		# Add the new pool id to the list of newly created pools
		materialized_pools_ids.append(new_pool_id)

		# Get the pool candidates
		pool_candidate_ids_list = row["pool_candidates"]

		# Loop through the pool candidate lists
		for candidate_id in pool_candidate_ids_list:
			candidate_id = int(candidate_id) 
			execute(insert_into_listing_pool_lookup, [new_pool_id, candidate_id])
			
	return materialized_pools_ids
	$$ LANGUAGE plpython3u;
	""".format(refresh_count_target=refresh_count_target))
	return res()


def register__function_process_newly_inserted_listing():
	res = db.prepare(
		"""
		--- Callback that sends notifications when trigger goes of
		CREATE OR REPLACE FUNCTION process_newly_added_listing() RETURNS TRIGGER AS $$
			BEGIN
				-- If a new listing comes in then run the pool materialization
				PERFORM materialize_candidate_pools_and_schedule_pool_update_request_notifs();
				return NEW;
			END;
		$$ LANGUAGE plpgsql;
		"""
	)
	return res()


def register__materialize_candidate_pools_and_schedule_pool_update_request_notifs(pattern):
	ps = db.prepare(
		"""
		CREATE OR REPLACE FUNCTION materialize_candidate_pools_and_schedule_pool_update_request_notifs() RETURNS INT[] AS $$
			from plpy import execute,prepare, info

			materialized_pools_query_res = execute("SELECT * FROM materialize_pools()")
			materialized_pools_ids = list(materialized_pools_query_res)[0]["materialize_pools"]
			
			for materialized_pool_id in materialized_pools_ids:
				# Schedule pool update request notifications to be sent to client
				ps = prepare(
					'''
					SELECT cron.schedule(
						 'send-update-requests-schedule-for-pool-%s',
						 '{pattern}',
						 CONCAT('NOTIFY watch_update_listings_pool_request', ', ' , $1  )
					);
					''' % str(materialized_pool_id), ["text"])
				res = execute(ps,[str("'"+str(materialized_pool_id)+"'")])
			return materialized_pools_ids
			$$ LANGUAGE plpython3u;
		""".format(pattern=pattern))
	return ps()



def register__check_if_pool_is_filled_and_unschedule():
	ps = db.prepare("""
		CREATE OR REPLACE FUNCTION check_if_pool_is_filled_and_unschedule() RETURNS TRIGGER AS $$
		
		from plpy import execute, info, prepare
		
		pool_update_count = int(TD['new']['pool_update_count'])
		pool_update_target = int(TD['new']['pool_update_target'])
		pool_id = int(TD['new']['pool_id'])
		pool_schedule_name = 'send-update-requests-schedule-for-pool-' + str(pool_id)
		
		# You'd think that this will get executed after insertion so the number 
		#  should be what they are but apparently not
		if pool_update_count >= pool_update_target:
			ps = prepare("SELECT cron.unschedule($1)", ["text"])
			execute(ps, [pool_schedule_name])
			execute("SELECT pg_notify('watch_pool_filled', 'pool finished target reached: %s')" % str(pool_id))
		$$
		LANGUAGE plpython3u;
	""")
	return ps()


def register__unschedule_all_jobs():
	ps = db.prepare("""
	-- call unschedule_all_jobs()
	CREATE OR REPLACE PROCEDURE unschedule_all_jobs() AS
	$$
	DECLARE
	  job_row RECORD;
	BEGIN
	  -- unschedule all jobs
	  FOR job_row IN SELECT jobname FROM cron.job LOOP
		-- Generate and execute the unschedule function dynamically
		EXECUTE format('SELECT cron.unschedule(%L)', job_row.jobname);
	  END LOOP;
	END $$ LANGUAGE plpgsql;
	""")
	return ps()

def register__cleanup_previous_run():
	ps = db.prepare("""
	-- call cleanup_previous_run()
	CREATE OR REPLACE PROCEDURE cleanup_previous_run() AS
	$$
	BEGIN
	  -- delete the pool tab	le
	  DELETE FROM cron.pool;
	
	  -- delete the listing pool lookup table
	  DELETE FROM cron.listing_pool_lookup; 
	  
	  -- delete run history table
	  DELETE FROM cron.job_run_details;
	
	  -- unschedule all jobs
	  CALL unschedule_all_jobs();
	END $$ LANGUAGE plpgsql;
	""")
	return ps()


def register__count_unique_listings():
	ps = db.prepare("""
	CREATE OR REPLACE FUNCTION count_unique_listings() RETURNS INT AS $$
	DECLARE
    	unique_listing_count INT;
	BEGIN
		SELECT COUNT(DISTINCT listing_id) INTO unique_listing_count
		FROM cron.listings;
	RETURN unique_listing_count;
	END $$ LANGUAGE plpgsql;
	""")
	return ps()


def register__get_highest_updated_count_counts():
	ps = db.prepare("""
	CREATE OR REPLACE FUNCTION get_highest_updated_count_counts()
		RETURNS TABLE (
			highest_updated_count INT,
			count_of_highest_updated_count INT
		)
		AS $$
		BEGIN
			RETURN QUERY
				SELECT 
					highest_updated_count2::INT AS highest_updated_count, 
					COUNT(*)::INT AS count_of_highest_updated_count
				FROM (
					SELECT listing_id, MAX(updated_count) AS highest_updated_count2
					FROM cron.listings
					GROUP BY listing_id
				) AS subquery
				GROUP BY highest_updated_count2
				ORDER BY highest_updated_count2 DESC;
		END;
	$$ LANGUAGE plpgsql;	
	""")
	return ps()


def register__get_unique_listing_groups():
	ps = db.prepare("""
	CREATE OR REPLACE FUNCTION get_unique_listing_groups()
		RETURNS TABLE(count INT, unique_count_groups INT) AS $$
	BEGIN
		RETURN QUERY
		SELECT count2::INT as count, COUNT(*)::INT AS unique_count_groups
		FROM (
			SELECT listing_id, updated_count, COUNT(*) AS count2
			FROM cron.listings
			GROUP BY listing_id, updated_count
		) AS subquery
		GROUP BY count
		ORDER BY unique_count_groups DESC;
	END;
	$$ LANGUAGE plpgsql;
	""")
	return ps()