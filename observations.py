import bisect
from collections import defaultdict
from multiprocessing import Pool

import time
import sys
from pymongo import MongoClient
from utils.location_outlier_remover import get_filtered_locations
from utils.IO import get_locations_from_file_for_user, get_wifi_scans_from_file_for_user

TIME_INTERVAL = int(sys.argv[1])

client = MongoClient(max_pool_size=8)

def find_near_wifi_scans(location_timestamp, wifi_scans_timestamps):
	#start_time = get_time_difference(0)
	upper_range = int(location_timestamp) + TIME_INTERVAL
	lower_range = int(location_timestamp) - TIME_INTERVAL
	interval = [lower_range, upper_range]
	left = bisect.bisect_left(wifi_scans_timestamps, interval[0])
	right = bisect.bisect_right(wifi_scans_timestamps, interval[1])
	found_scans_timestamps = wifi_scans_timestamps[left:right]
	#print "End bisect: " + str(get_time_difference(start_time))
	return found_scans_timestamps

def get_nearest_scan(found_scans_timestamp, location_timestamp):
	found_scans_timestamp.sort(key=lambda x: abs(location_timestamp - x))
	closest_scan_timestamp = found_scans_timestamp[0]
	return closest_scan_timestamp

def scan_observed(scan_timestamp, ap_scan, location, previous_observations, user_id):
	"""
	Returns True if another observation have been associated with the scan
	It also replaces that observation if the new one is closer in time to the
	respective wifi scan
	"""

	location_timestamp = location[0]
	latitude = location[1]
	longitude = location[2]

	distance_to_closest_scan = abs(location_timestamp - scan_timestamp)

	for observation in previous_observations:
		observed_wifi_scan_time = observation[3]
		observation_location_scan_time = observation[4]

		if scan_timestamp != observed_wifi_scan_time:
			continue

		if abs(observation_location_scan_time - location_timestamp) >= TIME_INTERVAL:
			continue

		if distance_to_closest_scan < abs(observed_wifi_scan_time - observation_location_scan_time):
			scan_signal_strength = ap_scan[2]
			loc_index = previous_observations.index(observation)
			previous_observations[loc_index] = [latitude, longitude, scan_signal_strength, scan_timestamp,
														location_timestamp, user_id]
		return True

	return False


def map_location_to_access_point(location, wifi_scans, bssid_mapped_locations, wifi_scans_timestamps, user_id):
	location_timestamp = location[0]

	found_scans_timestamps = find_near_wifi_scans(location_timestamp, wifi_scans_timestamps)

	if len(found_scans_timestamps) < 1:
		return

	#Use just one scan, the one nearest in time to the location scan
	closest_scan_timestamp = get_nearest_scan(found_scans_timestamps, location_timestamp)
	closest_scans = wifi_scans[closest_scan_timestamp]
	latitude = location[1]
	longitude = location[2]
	#Iterate through each scanned access point to grow its list of observations
	for ap_scan in closest_scans:
		bssid = ap_scan[1]
		scan_timestamp = closest_scan_timestamp

		if not scan_observed(scan_timestamp, ap_scan, location, bssid_mapped_locations[bssid], user_id):
			scan_signal_strength = ap_scan[2]
			bssid_mapped_locations[bssid].append([latitude, longitude, scan_signal_strength, scan_timestamp, location_timestamp, user_id])

def map_locations_to_access_points_for_the_user(user_id):
	print "Started for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	bssid_mapped_locations = defaultdict(list)

	user_locations = get_locations_from_file_for_user(user_id)
	user_locations = get_filtered_locations(user_locations)

	user_wifi_scans = get_wifi_scans_from_file_for_user(user_id)
	print "Ended data load up from file for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	print "Wifi scans: " + str(len(user_wifi_scans))

	wifi_timestamps = user_wifi_scans.keys()
	wifi_timestamps = sorted(wifi_timestamps)

	print "Ended sorting for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	for location in user_locations:
		map_location_to_access_point(location, user_wifi_scans, bssid_mapped_locations, wifi_timestamps, user_id)
	print "Ended for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())

	print "Started mongo dump for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	while len(bssid_mapped_locations) > 0:
		write_observations_to_mongo(bssid_mapped_locations.popitem())
	print "Ended mongo dump for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())

def map_locations_to_access_points_for_user_locations(param_tuple):
	user_id = param_tuple[0]
	user_locations = param_tuple[1]
	print "Started for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	bssid_mapped_locations = defaultdict(list)
	user_wifi_scans = get_wifi_scans_from_file_for_user(user_id)
	print "Ended data load up from file for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	print "Wifi scans: " + str(len(user_wifi_scans))
	wifi_timestamps = user_wifi_scans.keys()
	wifi_timestamps = sorted(wifi_timestamps)
	print "Ended sorting for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	for location in user_locations:
		map_location_to_access_point(location, user_wifi_scans, bssid_mapped_locations, wifi_timestamps, user_id)
	print "Ended for user id: " + str(user_id) + " at " + time.strftime("%H:%M:%S", time.localtime())
	while len(bssid_mapped_locations) > 0:
		write_observations_to_mongo(bssid_mapped_locations.popitem())

def merge_results(results):
	bssid_dict = {}
	for result in results:
		for bssid in result:
			if bssid not in bssid_dict:
				bssid_dict[bssid] = []
			bssid_dict[bssid].extend(result[bssid])
	return bssid_dict

def chunks(l, n):
	return [l[i:i+n] for i in range(0, len(l), n)]

def map_locations_to_access_points_multiprocessed_for_the_user(user_id):
	user_locations = get_locations_from_file_for_user(user_id)
	user_locations = get_filtered_locations(user_locations)
	n = 4
	chunked_list = chunks(user_locations, int(len(user_locations)/n))
	chunked_list_user = []
	for location_list in chunked_list:
		chunked_list_user.append((user_id, location_list))
	p = Pool(processes=n)
	p.map(map_locations_to_access_points_for_user_locations, chunked_list_user)

def write_observations_to_mongo(bssid_tuple):
	bssid = bssid_tuple[0]
	possible_locations = bssid_tuple[1]

	db = client['radudb']
	bssid_collection = db['observations']

	for observation in possible_locations:
		bssid_data = {}
		bssid_data["BSSID"] = bssid
		bssid_data["latitude"] = observation[0]
		bssid_data["longitude"] = observation[1]
		bssid_data["RSSI"] = observation[2]
		bssid_data["location_timestamp"] = observation[4]
		bssid_data["user_id"] = observation[5]
		bssid_data["time_delta"] = TIME_INTERVAL

		bssid_collection.insert(bssid_data)

if __name__ == "__main__":
	print "Start: " + time.strftime("%H:%M:%S", time.localtime())

	user_ids = range(1, 132, 1)
	# user_ids.remove(108)
	# user_ids.remove(20)
	# user_ids.remove(44)
	# user_ids.remove(34)
	# big_data_user_ids = [34]

	print "Computing possible location for each BSSID: " + time.strftime("%H:%M:%S", time.localtime())
	p = Pool(processes=20)
	p.map(map_locations_to_access_points_for_the_user, user_ids)

	# print "Processing user with big data: " + time.strftime("%H:%M:%S", time.localtime())
	# for user_id in big_data_user_ids:
	# 	big_data_results = map_locations_to_access_points_multiprocessed_for_the_user(user_id)


	print "End: " + time.strftime("%H:%M:%S", time.localtime())
