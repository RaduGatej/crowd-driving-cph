from haversine import haversine

from utils.IO import get_locations_from_file_for_user


SPEED_LIMIT = 300

def get_speed(starting_location, final_location):
	time_difference_hours = float(final_location[0] - starting_location[0])/3600
	if time_difference_hours == 0:
		return 0.0
	distance = haversine((final_location[1], final_location[2]), (starting_location[1], starting_location[2]))
	speed = float(distance)/time_difference_hours

	return speed

def get_filtered_locations(locations):
	locations.sort(key=lambda x: x[0])
	outliers = []
	last_valid_location_index = -1
	for i in range(1, len(locations), 1):
		if last_valid_location_index < 0:
			speed = get_speed(locations[i-1], locations[i])
			if speed > SPEED_LIMIT:
				last_valid_location_index = i - 1
				outliers.append(locations[i])
		else:
			speed = get_speed(locations[last_valid_location_index], locations[i])
			if speed > SPEED_LIMIT:
				outliers.append(locations[i])
			else:
				last_valid_location_index = -1

	for outlier in outliers:
		locations.remove(outlier)

	return locations

