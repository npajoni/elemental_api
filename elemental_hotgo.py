from elemental_api import *


import os
import datetime
import time

def not_modif_time(filename=''):
    now = time.time()
    if os.path.isfile(filename):
        lmodif = os.stat(filename).st_ctime
        if (lmodif + 300) < now:
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


path_in = '/data/mnt/hotgo_master/.processed/'
path_out = '/data/mnt/stg01/output/'
#path_proc = '/data/mnt/hotgo_master/.processed/'
path_lock = '/tmp/elemento_scan.lock'

if lock(path_lock) == False:
        print "Already running..."
        exit()

for file in os.listdir(path_in):
    filename = os.path.splitext(file)[0]
    print filename
    if os.path.isfile(path_in + file):
        if file.endswith(".mp4") or file.endswith(".mxf"):
            if not_modif_time(path_in + file):
#                cmd = "mv " + '"' + path_in + file + '"' + ' "' + path_proc + '"'
#                print cmd
#                os.popen(cmd)
                elm = Elemental('10.4.0.91')
		input = file
		preset = '78'
		output_path = path_out + filename + '/mp4/350/'
		output_filename = filename
		CreateJob(elm, input, output_path, output_filename, preset) 
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


