from connection import db


def create_table__request_batch():
	# todo, also add a request type column to here (new, update)
	res = db.prepare("""
	CREATE TABLE IF NOT EXISTS request_batches (
		id SERIAL PRIMARY KEY,
		requested_at TIMESTAMPTZ DEFAULT NOW()
	);
	""")
	return res()

def create_table__total_listings_count():
	res = db.prepare("""
	CREATE TABLE IF NOT EXISTS cron.total_listings_count (
	  count INT,
	  at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	""")
	return res()


def create_table__responses():
	res = db.prepare(
	"""
	CREATE TABLE IF NOT EXISTS cron.listings (
	  listing_id numeric,
	  user_id numeric,
	  shop_id numeric,
	  title text,
	  description text,
	  state text, -- CHECK (state IN ('active', 'inactive', 'sold_out', 'draft', 'expired')),
	  creation_timestamp numeric,
	  created_timestamp numeric,
	  ending_timestamp numeric,
	  original_creation_timestamp numeric,
	  last_modified_timestamp numeric,
	  updated_timestamp numeric,
	  state_timestamp numeric,
	  quantity numeric,
	  shop_section_id numeric,
	  featured_rank numeric,
	  url text,
	  num_favorers numeric,
	  non_taxable boolean,
	  is_taxable boolean,
	  is_customizable boolean,
	  is_personalizable boolean,
	  personalization_is_required boolean,
	  personalization_char_count_max numeric,
	  personalization_instructions text,
	  listing_type text,  --CHECK (listing_type IN ('physical', 'download', 'both')),
	  tags text[],
	  materials text[],
	  shipping_profile_id numeric,
	  return_policy_id numeric,
	  processing_min numeric,
	  processing_max numeric,
	  who_made text,
	  when_made text,
	  is_supply boolean,
	  item_weight numeric,
	  item_weight_unit text,
	  item_length numeric,
	  item_width numeric,
	  item_height numeric,
	  item_dimensions_unit text,
	  is_private boolean,
	  style text[],
	  file_data text,
	  has_variations boolean,
	  should_auto_renew boolean,
	  language text,
	  price json,
	  taxonomy_id numeric,
	  production_partners json,
	  skus json,
	  views numeric,
	  updated_count numeric,
	  request_batch_id integer REFERENCES request_batches (id),
	  request_batch_insertion_id numeric,
	  requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,


	  ---------
	  PRIMARY KEY (listing_id, updated_count)
	);
	"""
	)
	return res()



def create_table__listing_pool_lookup():
	res = db.prepare(
	"""
	CREATE TABLE IF NOT EXISTS cron.listing_pool_lookup (
	  pool_id INTEGER,
	  listing_id INTEGER
	);
	""")
	return res()


def create_table__pool():
	res = db.prepare(
	"""
	CREATE TABLE IF NOT EXISTS cron.pool (
	  pool_id INTEGER,
	  pool_update_target INTEGER,
	  pool_update_count INTEGER DEFAULT 0,
	  CHECK (pool_update_count <= pool_update_target)
	);
	""")
	return res()