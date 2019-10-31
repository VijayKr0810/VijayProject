import os
import sys
import time
import subprocess
from datetime import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))+"\\"

if not os.path.isdir(dir_path+"\\logs\\"):
    print "No"
    os.mkdir(dir_path+"\\logs\\")
else:
    print "Yes"
    
LOG_FILE_PATH = dir_path+"\\logs\\"+"meter_sim_pmon.log"
CONFIG_PATH = dir_path+"config.cfg"
MAX_FILE_SIZE = 1*1024*1024

class pmon_class():
    def __init__(self):
        fun_name = "pmon_class()"
        self.log_file_name = LOG_FILE_PATH
        self.file_ptr = None
        self.dbg_msg = ['[Inform]', '[Warning]', '[Severe]', '[Fatal]', '[Report]']
        self.INFORM = self.dbg_msg[0]
        self.WARNING = self.dbg_msg[1]
        self.SEVERE = self.dbg_msg[2]
        self.FATAL = self.dbg_msg[3]
        self.REPORT = self.dbg_msg[4]

        self.process_list = []

        
        self.cfg_ptr = open(CONFIG_PATH, "r")
        for line in self.cfg_ptr:
            if("IMEI" in line):
                imei = int(((line.split('='))[1])[:-1])
            elif("NUM_INST" in line):
                PROCESS_TOTAL = int(((line.split('='))[1])[:-1])
            elif("SIM_EXE_NAME" in line):
                self.exe_name = (((line.split('='))[1])[:-1])

        dbg_buf = "%-25s : Starting imei : %d, Num of Inst : %d" % (fun_name, imei, PROCESS_TOTAL)
        self.dbg_log(self.INFORM, dbg_buf)

        for idx in range(PROCESS_TOTAL):
            try:
                PROCESS_ARGS = []
                imei_str = str(imei+idx)
                met_ser_str = str((idx+imei)%100000)
                PROCESS_ARGS = [dir_path+self.exe_name, imei_str,met_ser_str]
                dbg_buf = "%-25s : Filling Procs id : %d , proc Name : %s" % (fun_name, idx+1, PROCESS_ARGS)
                self.dbg_log(self.INFORM, dbg_buf)

                self.process_list.append(subprocess.Popen(PROCESS_ARGS))
                time.sleep(3)
                
            except (KeyboardInterrupt, SystemExit):
                dbg_buf = "%-25s : Keyboard Interrupt in  Pmon" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                time.sleep(2)
                
                for i in range(PROCESS_TOTAL):
                    if not len(self.process_list[i]):
                        imei_str = str(imei+i)
                        met_ser_str = str((i+imei)%100000)
                        PROCESS_ARGS = [dir_path+self.exe_name, imei_str,met_ser_str]
                        dbg_buf = "%-25s : Killing proc Name : %s" % (fun_name, PROCESS_ARGS)
                        self.dbg_log(self.INFORM, dbg_buf)
                        self.process_list[i].kill()
                        time.sleep(0.5)
                break

        dbg_buf = "%-25s : All proc are in monitoring"%(fun_name)
        self.dbg_log(self.INFORM, dbg_buf)

        while True:
            try:
                for i in range(PROCESS_TOTAL):
                    if not (self.process_list[i]) == None:
                        imei_str = str(imei+i)
                        met_ser_str = str((i+imei)%100000)
                        PROCESS_ARGS = [dir_path+self.exe_name, imei_str,met_ser_str]       
                        p = self.process_list[i]

                        dbg_buf = "%-25s : %s is running"%(fun_name,PROCESS_ARGS)
                        #self.dbg_log(self.INFORM, dbg_buf)

                        if p.poll() != None: 
                            dbg_buf = "%-25s : Reopening proc Name : %s" % (fun_name, PROCESS_ARGS)
                            self.dbg_log(self.INFORM, dbg_buf)
                            time.sleep(2)
                            self.process_list[i] = subprocess.Popen(PROCESS_ARGS)
                            time.sleep(3)

                        time.sleep(1)

            except (KeyboardInterrupt, SystemExit):
                dbg_buf = "%-25s : Keyboard Interrupt in  Pmon" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                time.sleep(2)
                for i in range(PROCESS_TOTAL):
                    imei_str = str(imei+i)
                    met_ser_str = str((i+imei)%100000)
                    PROCESS_ARGS = [dir_path+self.exe_name, imei_str,met_ser_str]
                    dbg_buf = "%-25s : Killing proc Name : %s" % (fun_name, PROCESS_ARGS)
                    self.dbg_log(self.INFORM, dbg_buf)
                    self.process_list[i].kill()
                    time.sleep(0.5)
                break
        time.sleep(2)
        sys.exit(0)


    def write_into_file(self, dbg_buff):
        if not self.file_ptr:
            self.file_ptr = open(self.log_file_name, 'w')
            self.file_ptr.write(dbg_buff)
        else:
            self.file_ptr = open(self.log_file_name, 'a')
            self.file_ptr.write(dbg_buff)

        statinfo = os.stat(self.log_file_name)
        if(statinfo.st_size > MAX_FILE_SIZE):
            self.file_ptr.close()
            os.remove(self.log_file_name)
            self.file_ptr = open(self.log_file_name, 'w')

    def dbg_log(self, mode, dbg_str):
        dbg_buff = '%s : %s : %s\n' % ((datetime.now().strftime('%d_%b_%Y %H:%M:%S'), mode, dbg_str))
        print (dbg_buff)
        self.write_into_file(dbg_buff)

try:
    pmon_class()

except (KeyboardInterrupt, SystemExit):
    dbg_buff = "Keyboard interrupt Returning from system"
    print dbg_buff
    time.sleep(2)
    sys.exit(0)

#   ----------------End Of File----------------------------------------------
