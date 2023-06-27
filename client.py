import os

import secrets


rpc_address_host = "localhost"
rpc_address_port = 1337



rpc = True


if rpc:
	from lib.etsy_v3_oauth2_client.etsy_rpc_client_service import EtsyRPCBaseClientJSON
	client = EtsyRPCBaseClientJSON(
			secrets.API_TOKEN,
			secrets.ETSY_EMAIL,
			secrets.ETSY_PASSWORD,
			rpc_address_host,
			rpc_address_port,
			"json",
			launching_client_connect_timeout=20
	)

else:
	from lib.etsy_v3_oauth2_client.etsy_selenium_client import EtsyOAuth2ClientSelenium
	client = EtsyOAuth2ClientSelenium(
		api_token=secrets.API_TOKEN,
		email=secrets.ETSY_EMAIL,
		password=secrets.ETSY_PASSWORD,
		host="localhost",
		port=5000,
		auto_close_browser=True,
		auto_refresh_token=True,
		reference_file_path=os.path.join(
			os.path.dirname(__file__), "./","lib","etsy_v3_oauth2_client", "api_reference.json"),
)
