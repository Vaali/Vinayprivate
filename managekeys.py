from multiprocessing import Manager
import random
from datetime import datetime, date, timedelta
import time
import loggingmodule


class ManageKeys():
    manager = Manager()
    blocked_keys = manager.list()
    curr_keys = manager.list()
    proj_keys = []
    logger_managedkeys = loggingmodule.initialize_logger1('Managedkeyslog','errorkeys.log')
    def __init__(self):
        self.proj_keys = ["AIzaSyB34POCUa53BcFsdPURNsvm0i6AX4kqjWo","AIzaSyBA-UrozRMFbVqrBxivh5IqXzt1H9jwYSY","AIzaSyAfeaQZyCpnxmBpwIfa-DbZ1Ny9pw_rFvI","AIzaSyDz04gJUsb_9sX6CLsxiaS-AeX_toUOnhM","AIzaSyC-AJNub7xhMGFcSTcJ7IXOrQZuqfZOW00","AIzaSyCAxLZzH-AvClkqRJ5JM4WR-odnmdpFH2o","AIzaSyBXs075Y10IAhH4rlqeHYBmVuEzOeLz4xo","AIzaSyCgp8XEQfhDMFM9BoFHr8H2BSrAbBfb5U0"] #vinay
        self.proj_keys += ["AIzaSyCBjTtgWV16zl9ivezXUm7Gr5ac6QnHDgI","AIzaSyBZCO5-gQRcmYlvuZZCLJyVqqKxTzKLgiM","AIzaSyCW0fEzUcOQtewKeGcUc8XPXnN2j1EAKZY","AIzaSyDZoLt2Q0fkEkkiqepp60WPmkS69NTX370","AIzaSyDfESLhLqMa6qqvzigCGy5F36YURuW_Eus","AIzaSyCiFWuQWfXhsBKzXPZ5hQYy0Du_SMIal94","AIzaSyDud6MWfd1l5BPb53x9GGqCAUQoDYmUIGE","AIzaSyDZk4Kwal9BB9JxQbbP5WYvLvEOSAiV8Ao","AIzaSyD47DzEbad6eEPk29gkITOnYrgZUATXf_I","AIzaSyC9coydvCvnkysL6g-FIAyqg89LzUtqq-o"]#kin
        self.proj_keys += ['AIzaSyBX-WCpgMHu_9OGpfkdQJD3SMsJTcDCscE','AIzaSyAMGC-oG66RxddL1BrYupDPKGlUV16Fy0I']
        self.proj_keys +=['AIzaSyAUeAM6MhxAO-QoByqZTmkYkTbvcheyxAU','AIzaSyBplSw0EhZiACVBkMdqvrLkenQ_PTau9v0','AIzaSyBDVAu4lVhNGfH06875DatHXcz3u-1gKCI']
        self.proj_keys +=['AIzaSyBt8RP9znGKvWVlHvn8GWdNcLvVRduB4Ak','AIzaSyCl3UV_9k0aLBPI1viEkFoX1wqBPaV26NQ','AIzaSyALgk-GhC4LvlV9WvB8528vyX8ZK29grVc']
        self.proj_keys +=['AIzaSyCFKavfiehRz1aD7cgugi3wWy-4_e6unWw','AIzaSyC3WtfwN8Jzff32AZln7rDmWI9vsJvj01s']


    def getkey(self):
        if(len(self.curr_keys)==0):
            return ""
        return self.curr_keys[random.randint(0,len(self.curr_keys)-1)]
    

    def wait_for_keys(self):
        tomorrow = datetime.today() + timedelta(1)
        midnight = datetime(year=tomorrow.year, month=tomorrow.month,day=tomorrow.day, hour=0, minute=0, second=0)
        timetomidnight = (midnight - datetime.now()).seconds
        time.sleep(3600*4+timetomidnight)

    def reset_projkeys(self):
        self.curr_keys = self.manager.list()
        self.blocked_keys = self.manager.list()
        for key in self.proj_keys:
            self.curr_keys.append(key)
            self.logger_managedkeys.error('adding key')

    def removekey(self,key):
        if(key in self.curr_keys):
		    self.curr_keys.remove(key)

    def add_blockedkey(self,key):
        if(key not in self.blocked_keys):
		    self.blocked_keys.append(key)

    def keys_exhausted(self):
        if(len(self.blocked_keys) == len(self.proj_keys)):
		    #logger_crawl.error("sleeping")
		    self.logger_managedkeys.error('sleeping')
		    self.wait_for_keys()
		    self.reset_projkeys()
		    self.logger_managedkeys.error("error "+str(len(self.blocked_keys))+" "+str(len(self.proj_keys)))

    def get_blocked_keys(self):
        return self.blocked_keys
    
    def get_curr_keys(self):
        return self.curr_keys