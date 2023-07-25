import base64
import builtins
import json
import threading
import time
from datetime import datetime

from connection import db


def start_listening_commands(channel_names):
	if channel_names:
		# Loop through the channels
		# If channel is not a list make a list with the channel  name as the only element
		for channel in (channel_names if isinstance(channel_names, list) else [channel_names]):
			# Execute the listen command
			db.listen(channel)


def listen_to_channels(channel_names, process_notification_callback):
	# Send listening commands
	start_listening_commands(channel_names)

	for notification in db.iternotifies():
		channel, payload, pid = notification

		# If the process_notification is a dict, use it to look up the callback
		if not isinstance(process_notification_callback, dict):
			process_notification_callback(notification)
		else:
			if channel in process_notification_callback:
				process_notification_callback[channel.strip()](notification)
			else:
				process_notification_callback["default"](notification)


def listen_to_channels_threaded(channel_name=None, process_notification_callback=lambda notify: None,
								autostart_thread=True):
	# Create a new thread
	thread = threading.Thread(target=listen_to_channels, args=(channel_name, process_notification_callback))
	# Autostart it if specified
	if autostart_thread:
		thread.start()
	# Return the thread
	return thread


def process_insertion_trigger_notification(notification):
	channel, payload, pid = notification
	b64_decoded_bytes = base64.b64decode(payload)
	b64_decoded_str = b64_decoded_bytes.decode("utf-8")
	b64_decoded_json = json.loads(b64_decoded_str)
	return channel, b64_decoded_json,pid



def log_print(*args, **kwargs):
	now = datetime.now()
	builtins.print(now, " | ", *args, **kwargs)