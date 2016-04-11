from elemental_api import *


import os
import datetime
import time

def not_modif_time(filename=''):
    now = time.time()
    if os.path.isfile(filename):
        lmodif = os.stat(filename).st_ctime
        if (lmodif + 3) < now:
            return True
        else:
            return False

def lock(lockfile=''):
    if not os.path.isfile(lockfile):
        today = time.time()
        lock_file = open(lockfile, 'wt')
        lock_file.write(str(today))
        lock_file.close()
        return True
    else:
        return False


def unlock(lockfile=''):
    os.unlink(lockfile)

scan_path = '/disk_3/hotgo_master/test/'
path_in = '/data/mnt/hotgo_master/test/'
path_out = '/data/mnt/stg01/output/'
#path_proc = '/data/mnt/hotgo_master/.processed/'
path_lock = '/tmp/elemento_scan.lock'

if lock(path_lock) == False:
        print "Already running..."
        exit()

for file in os.listdir(scan_path):
    filename = os.path.splitext(file)[0]
    print filename
    if os.path.isfile(scan_path + file):
        if file.endswith(".mp4") or file.endswith(".mxf"):
            if not_modif_time(scan_path + file):
#                cmd = "mv " + '"' + path_in + file + '"' + ' "' + path_proc + '"'
#                print cmd
#                os.popen(cmd)
                elmJob = ElementalJob()
		elmJob.server = Elemental('10.4.0.91')
		elmJob.file_input = file
		elmJob.presetId = '78'
		elmJob.output_path = path_out + 'test/' + filename + '/mp4/350/'
		elmJob.output_basename = filename
		elmjob.start()
#                job = elmJob(elm)
#                job.input_filename = file
#                job.hls_preset_id  = 1
#                job.input_path     = path_proc
#                job.basename       = filename
#                job.system_path    = True
#                job.output_path    = filename + '/hls/'
#                job.name           = filename
#                job.start()
#                print job.status()
#                print job.files()

unlock(path_lock)






#job = CreateJob(x, '/data/mnt/hotgo_master/.processed/002902.mxf', '/mnt/output/', '002902', '78')


