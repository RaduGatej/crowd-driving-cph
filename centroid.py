from math import radians
from utils.geo import llaToECEF, ECEFTolla
from utils.median import getmedian, get_MAD_for_points_with_median


def get_ecef_locations(locations):
	ecef_locations = []
	for location in locations:
		lat = radians(location[0])
		lon = radians(location[1])
		ecef_location = llaToECEF(lat, lon, radians(0))
		ecef_locations.append(ecef_location)
	return ecef_locations

def get_median_and_deviation_for_locations(locations):

	ecef_locations = get_ecef_locations(locations)
	median = getmedian(ecef_locations)
	median_absolute_deviation = get_MAD_for_points_with_median(ecef_locations, median)
	wgs84_median = ECEFTolla(median[0], median[1], median[2])

	return (wgs84_median[0], wgs84_median[1]), median_absolute_deviation


def compute_centroid(bssid_tuple):
	bssid = bssid_tuple[0]
	observations = bssid_tuple[1]

	median, deviation = get_median_and_deviation_for_locations(observations)

	return (bssid, median, deviation, len(observations))
