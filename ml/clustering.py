from multiprocessing import Pool
from haversine import haversine
from pymongo import MongoClient
import gaussian

client = MongoClient(max_pool_size=30)

def get_clusters(bssid):
	observations = client["radudb"]["observations"].find({"BSSID": bssid})
	if observations.count() < 5:
		return
	locations = []
	for observation in observations:
		locations.append([observation["latitude"], observation["longitude"]])

	labels = gaussian.gaussian(locations)
	observations.rewind()
	bssid_file = open("observations/" + str(bssid), "w")
	observation_lines = ""
	for i, observation in enumerate(observations):
		observation['cluster_id'] = int(labels[i])
		observation.pop('_id')
		observation_lines += " ".join([str(obs_value) for obs_value in observation.values()]) + "\n"
	bssid_file.write(observation_lines)
	bssid_file.close()

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

if __name__ == "__main__":
	bssids =  client["radudb"]["observations"].distinct("BSSID")

	#print bssids
	p = Pool(processes=20)
	p.map(get_clusters, bssids)
