import json
from random import randint
from connection import db



def insert_into_total_listings_count(count):
	ps = db.prepare("INSERT INTO cron.total_listings_count VALUES ($1::FLOAT8)")
	return ps(count)


def insert_into_request_batches():
	ps = db.prepare("""INSERT INTO cron.request_batches DEFAULT VALUES RETURNING id;""")
	return ps()

def insert_into_listings(listing_item, request_batch_id, request_batch_insertion_id, updated_count=1):
	ps = db.prepare("""
	INSERT INTO listings VALUES (
		$1::numeric ,  
		$2::numeric ,  
		$3::numeric ,  
		$4::text ,  
		$5::text ,  
		$6::text ,  
		$7::numeric ,  
		$8::numeric ,  
		$9::numeric ,  
		$10::numeric , 
		$11::numeric , 
		$12::numeric , 
		$13::numeric , 
		$14::numeric , 
		$15::numeric , 
		$16::numeric , 
		$17::text , 
		$18::numeric , 
		$19::boolean , 
		$20::boolean , 
		$21::boolean , 
		$22::boolean , 
		$23::boolean , 
		$24::numeric , 
		$25::text , 
		$26::text , 
		$27::text[] , 
		$28::text[] , 
		$29::numeric , 
		$30::numeric , 
		$31::numeric , 
		$32::numeric , 
		$33::text , 
		$34::text , 
		$35::boolean , 
		$36::numeric , 
		$37::text , 
		$38::numeric , 
		$39::numeric , 
		$40::numeric , 
		$41::text , 
		$42::boolean, 
		$43::text[] , 
		$44::text , 
		$45::boolean , 
		$46::boolean , 
		$47::text , 
		$48::json , 
		$49::numeric, 
		$50::json, 
		$51::json, 
		$52::numeric ,
		$53::numeric,
		$54::numeric,
		$55::numeric,
		default
	);
	""")
	return ps(
			listing_item["listing_id"],
			listing_item["user_id"],
			listing_item["shop_id"],
			listing_item["title"],
			listing_item["description"],
			listing_item["state"],
			listing_item["creation_timestamp"],
			listing_item["created_timestamp"],
			listing_item["ending_timestamp"],
			listing_item["original_creation_timestamp"],
			listing_item["last_modified_timestamp"],
			listing_item["updated_timestamp"],
			listing_item["state_timestamp"],
			listing_item["quantity"],
			listing_item["shop_section_id"],
			listing_item["featured_rank"],
			listing_item["url"],
			listing_item["num_favorers"],
			listing_item["non_taxable"],
			listing_item["is_taxable"],
			listing_item["is_customizable"],
			listing_item["is_personalizable"],
			listing_item["personalization_is_required"],
			listing_item["personalization_char_count_max"],
			listing_item["personalization_instructions"],
			listing_item["listing_type"],
			listing_item["tags"],
			listing_item["materials"],
			listing_item["shipping_profile_id"],
			listing_item["return_policy_id"],
			listing_item["processing_min"],
			listing_item["processing_max"],
			listing_item["who_made"],
			listing_item["when_made"],
			listing_item["is_supply"],
			listing_item["item_weight"],
			listing_item["item_weight_unit"],
			listing_item["item_length"],
			listing_item["item_width"],
			listing_item["item_height"],
			listing_item["item_dimensions_unit"],
			listing_item["is_private"],
			listing_item["style"],
			listing_item["file_data"],
			listing_item["has_variations"],
			listing_item["should_auto_renew"],
			listing_item["language"],
			str(json.dumps(listing_item["price"])),
			listing_item["taxonomy_id"],
			str(json.dumps(listing_item["production_partners"])),
			str(json.dumps(listing_item["skus"])),
			listing_item["views"],
			updated_count,
			request_batch_id,
			request_batch_insertion_id
	)


def insert_update_pool_update_count(pool_id):
	ps = db.prepare(
		"""
		UPDATE cron.pool
		SET pool_update_count = pool_update_count + 1
		WHERE pool_id = $1::int;
		""")
	return ps(pool_id)