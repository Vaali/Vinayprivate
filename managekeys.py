from multiprocessing import Manager,Lock
import random
from datetime import datetime, date, timedelta
import time
import loggingmodule


class ManageKeys():
    manager = Manager()
    lock = Lock()
    blocked_keys = manager.dict()
    curr_keys = manager.list()
    proj_keys = []
    logger_managedkeys = loggingmodule.initialize_logger1('Managedkeyslog','errorkeys.log')
    def __init__(self,mode =1):
        if(mode ==0):
            self.logger_managedkeys.error("Adding for recrawling")
            self.proj_keys = ["AIzaSyB34POCUa53BcFsdPURNsvm0i6AX4kqjWo","AIzaSyBA-UrozRMFbVqrBxivh5IqXzt1H9jwYSY","AIzaSyAfeaQZyCpnxmBpwIfa-DbZ1Ny9pw_rFvI","AIzaSyDz04gJUsb_9sX6CLsxiaS-AeX_toUOnhM","AIzaSyC-AJNub7xhMGFcSTcJ7IXOrQZuqfZOW00","AIzaSyCAxLZzH-AvClkqRJ5JM4WR-odnmdpFH2o","AIzaSyBXs075Y10IAhH4rlqeHYBmVuEzOeLz4xo","AIzaSyCgp8XEQfhDMFM9BoFHr8H2BSrAbBfb5U0"] #vinay
        elif(mode ==1):
            self.proj_keys += ['AIzaSyBX-WCpgMHu_9OGpfkdQJD3SMsJTcDCscE','AIzaSyAMGC-oG66RxddL1BrYupDPKGlUV16Fy0I']
            self.proj_keys +=['AIzaSyAUeAM6MhxAO-QoByqZTmkYkTbvcheyxAU','AIzaSyBplSw0EhZiACVBkMdqvrLkenQ_PTau9v0','AIzaSyBDVAu4lVhNGfH06875DatHXcz3u-1gKCI']
            self.proj_keys += ["AIzaSyCBjTtgWV16zl9ivezXUm7Gr5ac6QnHDgI","AIzaSyBZCO5-gQRcmYlvuZZCLJyVqqKxTzKLgiM","AIzaSyCW0fEzUcOQtewKeGcUc8XPXnN2j1EAKZY","AIzaSyDZoLt2Q0fkEkkiqepp60WPmkS69NTX370","AIzaSyDfESLhLqMa6qqvzigCGy5F36YURuW_Eus","AIzaSyCiFWuQWfXhsBKzXPZ5hQYy0Du_SMIal94","AIzaSyDud6MWfd1l5BPb53x9GGqCAUQoDYmUIGE","AIzaSyDZk4Kwal9BB9JxQbbP5WYvLvEOSAiV8Ao","AIzaSyD47DzEbad6eEPk29gkITOnYrgZUATXf_I","AIzaSyC9coydvCvnkysL6g-FIAyqg89LzUtqq-o"]#kin
            self.proj_keys +=['AIzaSyBt8RP9znGKvWVlHvn8GWdNcLvVRduB4Ak','AIzaSyCl3UV_9k0aLBPI1viEkFoX1wqBPaV26NQ','AIzaSyALgk-GhC4LvlV9WvB8528vyX8ZK29grVc']
            self.proj_keys +=['AIzaSyCFKavfiehRz1aD7cgugi3wWy-4_e6unWw','AIzaSyC3WtfwN8Jzff32AZln7rDmWI9vsJvj01s']
            self.proj_keys +=['AIzaSyBTTnptHRDud60a-h3buMcPwzKhdrGZ1S0','AIzaSyCdJFCmD9S8qhZbrO0Hic6CYI8Hj3IWaYA']
            self.proj_keys +=['AIzaSyBRY49S17Gcfm48v_wkiZtbfhlai0gatf0','AIzaSyDQNVWezLNutolxzzwgUofOsuzrOEr5sfQ','AIzaSyBMEfrcUeK5cM9DVlFP-Zi0Vz3yuGPX7kc']
            self.proj_keys +=['AIzaSyBGG-eJTTFsbu1p6SoLfDvIidqW0naxn48','AIzaSyAYgFfT81uTeMTllLqi9owADcSIPkNcAvc','AIzaSyDZMKWuYIn5oULShiyaG09jyPi56PmhS14']
            self.proj_keys +=['AIzaSyCqroxejE1UUAxCaato6kX3VuueJdnXAK0','AIzaSyDVjf8h0Fizy_4SkOo02oaLFTeoCCFDPfY','AIzaSyBviZW5gsZrqKtv5FCwGNEEVfFYijuYPoI']
            self.proj_keys +=['AIzaSyBiWtCGFzPF6LdWe9fNdavCCy0_m6L71m0','AIzaSyCMzkS3GVf8u_yIIW__GotuW4ySjAbU5Ew','AIzaSyBk3OaJuf1liOJ9BL4eK8JEbdR-tOJSj6w']
            self.proj_keys +=['AIzaSyCM_39KOKHMDKxJID2QgjQdQDO1-s1Prt8','AIzaSyDkpQP4YQgAkjXBGYra7wn1CoQ2WZyFfT8','AIzaSyBKpPjgUtl0wO6A4YK_j2MOwD1yzsgl3FU']
            self.proj_keys +=['AIzaSyDoqoyYdiFFPon7g_WwGixSos8iALtD3RQ','AIzaSyA1b-yU8GbYKSILgXLAlT3juc1C8K06Cl0','AIzaSyABVbf1BPN8EGD3wYAZj6vsdSHqYGrksZE']
            self.proj_keys +=['AIzaSyD7nDGJYcMHyUVpduKbWwvnHVZf1_5rknY','AIzaSyDhtPYWTOkewbpEgElSkVba9wYeD5Mu00w','AIzaSyATHwlSmYr5DXce3rY_BQg2kNlr_V5qvpI']
            self.proj_keys +=['AIzaSyDJJuOmsK6pvuv_xIyFTF1Kpr2vyY0kWKM','AIzaSyC1IthMyKv9BkaWEwkq5UkxhRJ7ly_83Xk','AIzaSyD62jxJgLyvN-QbfgIZ18GT41UckiMTLjc']
            self.proj_keys +=['AIzaSyAvVhIFv9wZqaV2VHOsolUFTBT-n5TYo8Q','AIzaSyC6cmuXT7eoHcJbROKnSnEBFo_RGbUEzCA','AIzaSyAtbdGsPBfwlbyPRyc8YNNXeR1eBZHZ0tE']
            self.proj_keys +=['AIzaSyAqsXTQOYgI0SGevfRAfTEHndvoCPxww7E','AIzaSyBT5q85eGjeGq2oLmqv1bC3BsiwEP6Qy0Q','AIzaSyDxVtwdd-haNfcyH0-nDC_DYkq-S2Xzm9o']
            self.proj_keys +=['AIzaSyA7WqbUwpqHrEGvBlAgXNNrQtOgoyA7IP4','AIzaSyCms9Fn_Q1rv7NiW7FgBGKOdp7IAzch3b4','AIzaSyApp-IVzlwtW6GrhLu-IBq8KB-Ov4czDo4']
            self.proj_keys +=['AIzaSyCO4m_lwJpXyeT78c6s5XQV2KmOm3LAd9g','AIzaSyAJjo0VYRPgvX4GGYwpRcWoQSXzSW3j9gM','AIzaSyDV9VsHzcZeQEPqKFQEIhiPVrQdFN9SuyM']



    def getkey(self):
        self.lock.acquire()
        if(len(self.curr_keys)==0):
            self.lock.release()
            return ""
        retKey = self.curr_keys[random.randint(0,len(self.curr_keys)-1)]
        self.lock.release()
        return retKey
    

    def wait_for_keys(self):
        tomorrow = datetime.today() + timedelta(1)
        midnight = datetime(year=tomorrow.year, month=tomorrow.month,day=tomorrow.day, hour=0, minute=0, second=0)
        timetomidnight = (midnight - datetime.now()).seconds
        time.sleep((3600*4+timetomidnight)%(24*3600))

    def reset_projkeys(self):
        self.curr_keys = self.manager.list()
        self.blocked_keys = self.manager.dict()
        for key in self.proj_keys:
            self.curr_keys.append(key)
            self.logger_managedkeys.error('adding key')

    def removekey(self,key):
        self.lock.acquire()
        if(key in self.curr_keys):
		    self.curr_keys.remove(key)
        self.lock.release()

    def add_blockedkey(self,key):
        #self.lock.acquire()
        if(key not in self.blocked_keys):
		    self.blocked_keys[key] = 1
        #self.lock.release()

    def keys_exhausted(self):
        #self.logger_managedkeys.error("error "+str(len(self.blocked_keys))+" "+str(len(self.proj_keys)))
        if(len(self.blocked_keys) == len(self.proj_keys)):
		    #logger_crawl.error("sleeping")
		    self.logger_managedkeys.error('sleeping')
		    self.wait_for_keys()
		    self.reset_projkeys()
		    

    def get_blocked_keys(self):
        return self.blocked_keys
    
    def get_curr_keys(self):
        return self.curr_keys