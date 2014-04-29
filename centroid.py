from math import radians
from multiprocessing import Pool
import os
from pymongo import MongoClient
from utils.geo import llaToECEF, ECEFTolla
from utils.median import getmedian, get_MAD_for_points_with_median

client = MongoClient(max_pool_size=30)

def get_ecef_locations(locations):
	ecef_locations = []
	for location in locations:
		lat = radians(location[2])
		lon = radians(location[5])
		ecef_location = llaToECEF(lat, lon, radians(0))
		ecef_locations.append(ecef_location)
	return ecef_locations

def get_median_and_deviation_for_locations(locations):

	ecef_locations = get_ecef_locations(locations)
	median = getmedian(ecef_locations)
	median_absolute_deviation = get_MAD_for_points_with_median(ecef_locations, median)
	wgs84_median = ECEFTolla(median[0], median[1], median[2])

	return (wgs84_median[0], wgs84_median[1]), median_absolute_deviation


def compute_centroid(bssid):
	observations = [[int(line.split(" ")[0]), int(line.split(" ")[1]), float(line.split(" ")[2]), int(line.split(" ")[3]), int(line.split(" ")[4]), float(line.split(" ")[5]), int(line.split(" ")[6]), int(line.split(" ")[7])] for line in open("observations/" + str(bssid), "r").read().split("\n") if len(line) > 2]
	clusters = set([observation[3] for observation in observations])
	for cluster in clusters:
		clustered_observations = [observation for observation in observations if observation[3] == cluster]
		median, deviation = get_median_and_deviation_for_locations(clustered_observations)
		write_centroid_to_mongo(bssid, median, deviation, cluster, len(clustered_observations))

def write_centroid_to_mongo(bssid, median, deviation, cluster_id, observations_length):
	collection = client["radudb"]["estimations"]
	collection.insert({"BSSID": bssid, "deviation": deviation, "observation_count": observations_length, "cluster_id": cluster_id, "estimation": {"latitude": median[0], "longitude": median[1]}})

if __name__ == "__main__":
	bssids = os.listdir("observations")
	p = Pool(processes=20)
	p.map(compute_centroid, bssids)
