from connection import db



def get__jobs():
	res = db.prepare("SELECT * FROM cron.job")
	return list(res.rows())

def get__job_run_details():
	res = db.prepare("SELECT * FROM cron.job_run_details")
	return list(res.rows())

def get__listings_by_pool_id(pool_id):
	ps = db.prepare("select listing_id from cron.listing_pool_lookup where pool_id=$1::int")
	return ps(pool_id)

def get__update_count_by_listing_id(listing_id):
	ps = db.prepare("select MAX(updated_count) from cron.listings where listing_id=$1::int")
	return ps(listing_id)
