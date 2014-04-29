from multiprocessing import Pool
from pymongo import MongoClient
import time
from ml.clustering import get_clusters
from observations import map_locations_to_access_points_for_the_user

client = MongoClient(max_pool_size=30)


user_ids = range(1, 132, 1)
print "Computing possible location for each BSSID: " + time.strftime("%H:%M:%S", time.localtime())
p = Pool(processes=20)
p.map(map_locations_to_access_points_for_the_user, user_ids)
print "End: " + time.strftime("%H:%M:%S", time.localtime())

print "Computing clusters for each BSSID: " + time.strftime("%H:%M:%S", time.localtime())
bssids = client["radu_db"]["observations"].distinct("bssid")
p = Pool(processes=20)
p.map(get_clusters, bssids)
print "End: " + time.strftime("%H:%M:%S", time.localtime())
