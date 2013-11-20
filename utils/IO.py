from collections import defaultdict

WIFI_SCANS_FILE_PATH = '/big_data/data/wifi_scans_by_user/user_'
LOCATIONS_FILE_PATH = '/big_data/data/location_scans_by_user/user_'

def get_locations_from_file_for_user(user_id):
	locations_file = open(LOCATIONS_FILE_PATH + str(user_id), 'r').read().split("\n")
	locations = []
	for line in locations_file:
		# Location file columns:
		# user_id (hashed), timestamp, latitude, longitude, accuracy, provider
		split_line = (line.split(" "))
		if len(split_line) < 2:
			continue

		#discard locations not in Europe (mainly to address Cisco bug)
		if float(split_line[3]) < -21.445312:
			continue

		if float(split_line[4]) < 31.0:
			locations.append(
				(int(split_line[1]), float(split_line[2]), float(split_line[3]), float(split_line[4]), split_line[5]))

	return locations

def get_wifi_scans_from_file_for_user(user_id):
	wifi_file = open(WIFI_SCANS_FILE_PATH + str(user_id), 'r').read().split("\n")
	wifi_scans = defaultdict(list)
	for line in wifi_file:
		# WiFi scan file columns:
		# user_id (hashed), timestamp, ssid (hashed), bssid (hashed), RSSI
		split_line = (line.split(" "))
		if len(split_line) < 2:
			continue
		# Use tuples for the bisect function to work correctly
		timestamp = int(split_line[1])
		wifi_scans[timestamp].append((int(split_line[2]), int(split_line[3]), int(split_line[4])))

	return wifi_scans