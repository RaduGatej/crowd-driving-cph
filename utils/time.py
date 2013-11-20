from datetime import datetime
import time

def get_time_difference(start_time):
	dt = datetime.now()
	sec_since_epoch = time.mktime(dt.timetuple()) + dt.microsecond/1000000.0
	millis_since_epoch = sec_since_epoch * 1000
	return millis_since_epoch - start_time
