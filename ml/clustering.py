from haversine import haversine
from ml import gaussian


def prepare_for_gaussian(location_list):
	locations = []
	for location in location_list:
		locations.append([location[0], location[1]])
	return locations

def get_clusters(bssid_tuple):
	bssid_dict = {}
	bssid = bssid_tuple[0]
	possible_locations = bssid_tuple[1]
	if len(possible_locations) < 5:
		return bssid_dict

	locations = prepare_for_gaussian(possible_locations)
	labels = gaussian.gaussian(locations)
	for i in range(0, len(labels), 1):
		locations[i].append(labels[i])
		locations[i].append(possible_locations[i][3])
		locations[i].append(possible_locations[i][4])
	bssid_dict[bssid] = locations
	return bssid_dict

def remove_singular_points(locations):
	items_to_remove = []
	for location in locations:
		i = 0
		for copy_location in locations:
			if haversine((copy_location[0], copy_location[1]), (location[0], location[1])) < 0.05:
				i += 1
				if i > 3:
					break
		if i < 4:
			items_to_remove.append(location)
	for item_to_remove in items_to_remove:
		locations.remove(item_to_remove)
	return locations

def remove_outliers(bssid_tuple):
	bssid_dict = {}
	bssid = bssid_tuple[0]
	possible_locations = bssid_tuple[1]
	bssid_dict[bssid] = remove_singular_points(possible_locations)
	return bssid_dict
