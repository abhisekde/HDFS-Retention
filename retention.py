import shell
import json
from datetime import datetime
import sys
import os
from Metadata import Metadata

def __tree_hdfs(path):
    cmd = "hadoop fs -ls {}".format(path)
    txt, err = shell.execute(cmd)
    dirs = []
    if err:
        print("Failed to read HDFS path {}".format(path))
        shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "WARN", "Failed to read HDFS path {}".format(err))
    # Parse `ls` output
    lines = txt.decode("utf-8").split("\n") 

    for line in lines:
        # Igonore file count and other meta informations
        if line.find("/") == -1:
            continue

        _path = _path = line[line.find("/"):]

        # Check if the current path belongs to configured pond
        pond_path =  _path[len(hdfs):]
        if pond_path.startswith("/"):
            pond_path = pond_path[1:]
        
        if pond_path.find("/") != -1:
            pond_end = pond_path.find("/")
        else:
            pond_end = len(pond_path)
 
        pond_name = pond_path[:pond_end]
        config_ponds = retentions.keys()
        if pond_name not in config_ponds:
            continue

        # drwx------
        if line.startswith("d"):
            print("Working on path: {}". format(_path)) 
            #shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Working on path: {}". format(_path))
            yield from __tree_hdfs(_path)

        # -rwx------
        elif line.startswith("-"):
            print("Working on file: {}".format(_path))
            _date = ' '.join(line[:line.find("/")].split(' ')[-3:-1])
            if _path.split('/')[-2] == 'data':
                _name = _path.split('/')[-3]
            else:
                _name = _path.split('/')[-2]   
            _pond = _path.split('/')[2]
            _last_used = datetime.strptime(_date, "%Y-%m-%d %H:%M")
            #shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Working on file: {}". format(_path))
            yield Metadata(_name, _pond, _last_used, path)

        # Any other text or errors
        else: 
            continue 

def __validate(item, retentions):
    rv = None
    print("Validating file: {._name}".format(item))
    #shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Validating dataset: {._name}". format(item))
    if item._age_days > int(retentions[item._storage]):  
        rv =  item

        # Ignore files marked as exception
        ignore = settings["exceptions"] # dict(pond, dname)
        for pond in ignore.keys():
            dnames = ignore[pond]
            for ds in dnames:
                if pond+ds == item._storage + item._name:
                    #shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Ignoring dataset: " + item._name)
                    rv = None
                    break
    else:
        rv = None # New partition present
    return rv

# GLOBAL
# Load settings
assert (len(sys.argv) == 2), "usage: " + sys.argv[0] + " /path/to/config.json"
 
settings = shell.load(sys.argv[1])
retentions = settings["retentions"]
tracker = settings["tracker"]
hdfs = settings["root"]

# MAIN
cmd = "kinit -k -t /home/centos/hdfs.keytab hdfs"
txt, err = shell.execute(cmd)
if err:
    print("Failed to read HDFS path {}".format(path))
    shell.log(os.path.basename(sys.argv[0]), "kinit", "FAIL", "Failed to initiate kerberos {}".format(err))
    exit(1)

try:
    if not os.path.exists(tracker):
        with open(tracker, "w") as tk:
            tn = datetime.now().strftime("%d/%m/%Y")
            tk.write(tn)
            shell.log(os.path.basename(sys.argv[0]), tracker, "INFO", "New tracker created at " + tn)
    else:
        with open(tracker, "r") as tk:
            tn = datetime.now()
            ts = tk.read().replace("\n", "")
            tt = datetime.strptime(ts, "%d/%m/%Y")
            dt = (tn - tt).days
        if dt > 7:
            shell.log(os.path.basename(sys.argv[0]), tracker, "WARN", "Tracker is too stale. Last executed on: " + ts + ". Please validate application state, remove tracker file and re-start the process.")
            exit(1)
        else:
            shell.log(os.path.basename(sys.argv[0]), tracker, "INFO", "Tracker updated as " + tn.strftime("%d/%m/%Y"))
            with open(tracker, "w") as tk:
                tk.write(tn.strftime("%d/%m/%Y"))
            
except Exception as e:
    shell.log(os.path.basename(sys.argv[0]), tracker, "FAIL", e.__str__() + ". Please validate application state, remove tracker file and re-start the process.")
    exit(1)

shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Starting clean up of HDFS.")

# Slack message data structures
slack_msg = {"blocks": None }
blocks = []
block = {
    "type": "section",
    "text": {
        "type": "mrkdwn",
        "text": ""
    }
}

datasets = []
for file in __tree_hdfs(hdfs):
    dataset = __validate(file, retentions)
    if dataset != None:
        datasets.append(dataset)
shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "HDFS parsing finished.")

ds_set = set(datasets)
map_ds = set()
for item in ds_set:
    if map_ds.__contains__(item._path):
        continue
    block["text"]["text"] = ""
    blocks.clear()
    slack_msg["blocks"] = None

    txt = "Dataset *" + item._name +"* in *"+item._storage+"* has expired its retention.\n`"+item._path+"`\n"
    cmd = "hadoop fs -rm -r -f -skipTrash {._path}".format(item)
    shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Cleaning " + item._path)

    map_ds.add(item._path)
    #out_txt, err = shell.execute(cmd)
    #if err:
    #    print("Failed to remove HDFS path {}".format(item._path))  
    #    txt = txt + " *Failed* to remove dataset from HDFS."   
    #    shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "FAIL", "Failed to clean " + item._path)        
    #else:    
    #    print("Removed HDFS path {}".format(item._path))
    #    txt = txt + " Removed dataset from HDFS."
    #    shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Removed dataset " + item._name)
    #
    #block["text"]["text"] = txt
    #blocks.append(block)
    #slack_msg["blocks"] = blocks
    # Post to slack, if there are expired datasets
    #print("Notification:", slack_msg)
    #slack = shell.http(
    #        settings["slack"],
    #        'POST',
    #        json.dumps(slack_msg)
    #    )
    #shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Sending notification to slack channel.")
    #
    #if not slack:
    #    print('Failed to call slack channel.')
    #    shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "FAIL", "Failed to notify slack channel.")

shell.log(os.path.basename(sys.argv[0]), os.path.basename(sys.argv[1]), "INFO", "Clean up finished.")

