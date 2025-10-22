from time import time, ctime, sleep

time1 = ctime(time())
sleep(60)
time2 = ctime(time())


dbutils.notebook.exit("task1 completed")