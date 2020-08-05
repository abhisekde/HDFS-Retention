import os
import datetime
import json
import sys
import subprocess 
import requests
from requests.auth import HTTPBasicAuth

def execute(cmd):
    '''
    Execute a shell command and return output, errors
    Expects a command as string, eg: `ls -ltr /home`
    Returns a touple (output, errors)
    '''
    try:
        w_list = cmd.split(' ')
        c_list = [w for w in w_list if w != '']
        proc = subprocess.Popen(c_list, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        stdout, stderr = proc.communicate()
        return stdout, stderr
    except Exception as e:
        return "", e.__str__()

def load(path_):
    '''
    Loads a JSON settings file
    Expects a file path
    Returns a Dictionary of settings with `_OK_` flag defining if the file was loaded correctly
    '''
    config = {'_OK_': False}

    assert os.path.exists(path_), 'Can not load settings file, please verify file path and access permissions'

    try:
        f = open(path_, "r+")
        t = f.read()
        tx = t.replace('\n', '')
        f.close()
        config = json.loads(tx)
        config['_OK_'] = True 
    except Exception as ex:
        print(ex.__str__())
        config = {'_OK_': False}
    return config

def http(url, type_ = 'GET', data_ = None, user = None, pwd = None):
    '''
    Make HTTP calls
    Expects: 
        URL, 
        type_ = type of request defaults to GET, 
        data_ = dictionary data for post defaults to none, 
        user = authentication username defaults to none, 
        pwd = authentication password, defaults to none
    Returns reponse 
    '''
    if user and pwd:
        auth_ = HTTPBasicAuth(user, pwd)
    else:
        auth_ = None

    try:
        if type_ == 'GET' and auth_:
            response = requests.get(url=url, auth=auth_)
        elif type_ == 'GET':
            response = requests.get(url=url)
        elif type_ == 'POST' and auth_:
            response = requests.post(url=url, data=data_, auth=auth_)
        elif type_ == 'POST':
            response = requests.post(url=url, data=data_)
    except:
        response = None
    
    return response

def log(app, context, level, txt, file_path="/home/centos/retention/logs", timestamp_sfx=True):
    assert (level in ("INFO", "WARN", "FAIL")), "Log level can only be one among INFO, WARN, FAIL"
    log_entry = app + "|" + datetime.datetime.now().__str__() + "|" + context + "|" + level + "|" + txt
    log_entry.replace('\'', '')

    if timestamp_sfx == True:
        timestamp_sfx = "_" + datetime.datetime.now().day.__str__() + datetime.datetime.now().month.__str__() + datetime.datetime.now().year.__str__() 
    else:
        timestamp_sfx = ""
    execute("mkdir -p {}".format(file_path))
    with open(file_path+ os.sep + app + timestamp_sfx + ".log", "a") as log_w:
        log_w.write(log_entry + "\n")
        print(log_entry)

