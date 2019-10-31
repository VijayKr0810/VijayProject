import os
import sys
import time
import serial
import socket
import struct
import random
import thread
import threading
import mysql.connector
from datetime import date
from datetime import datetime
from datetime import datetime, timedelta

#   ------------------------------------------------------------------------
dir_path = os.path.dirname(os.path.realpath(__file__))+"\\"
LOG_FILE_PATH = dir_path+'\\logs\\'+"meter_sim"
CONFIG_PATH = dir_path+"config.cfg"
MAX_FILE_SIZE = 1*1024*1024

fcstab = [
    0x0000, 0x1189, 0x2312, 0x329b, 0x4624, 0x57ad, 0x6536, 0x74bf,
    0x8c48, 0x9dc1, 0xaf5a, 0xbed3, 0xca6c, 0xdbe5, 0xe97e, 0xf8f7,
    0x1081, 0x0108, 0x3393, 0x221a, 0x56a5, 0x472c, 0x75b7, 0x643e,
    0x9cc9, 0x8d40, 0xbfdb, 0xae52, 0xdaed, 0xcb64, 0xf9ff, 0xe876,
    0x2102, 0x308b, 0x0210, 0x1399, 0x6726, 0x76af, 0x4434, 0x55bd,
    0xad4a, 0xbcc3, 0x8e58, 0x9fd1, 0xeb6e, 0xfae7, 0xc87c, 0xd9f5,
    0x3183, 0x200a, 0x1291, 0x0318, 0x77a7, 0x662e, 0x54b5, 0x453c,
    0xbdcb, 0xac42, 0x9ed9, 0x8f50, 0xfbef, 0xea66, 0xd8fd, 0xc974,
    0x4204, 0x538d, 0x6116, 0x709f, 0x0420, 0x15a9, 0x2732, 0x36bb,
    0xce4c, 0xdfc5, 0xed5e, 0xfcd7, 0x8868, 0x99e1, 0xab7a, 0xbaf3,
    0x5285, 0x430c, 0x7197, 0x601e, 0x14a1, 0x0528, 0x37b3, 0x263a,
    0xdecd, 0xcf44, 0xfddf, 0xec56, 0x98e9, 0x8960, 0xbbfb, 0xaa72,
    0x6306, 0x728f, 0x4014, 0x519d, 0x2522, 0x34ab, 0x0630, 0x17b9,
    0xef4e, 0xfec7, 0xcc5c, 0xddd5, 0xa96a, 0xb8e3, 0x8a78, 0x9bf1,
    0x7387, 0x620e, 0x5095, 0x411c, 0x35a3, 0x242a, 0x16b1, 0x0738,
    0xffcf, 0xee46, 0xdcdd, 0xcd54, 0xb9eb, 0xa862, 0x9af9, 0x8b70,
    0x8408, 0x9581, 0xa71a, 0xb693, 0xc22c, 0xd3a5, 0xe13e, 0xf0b7,
    0x0840, 0x19c9, 0x2b52, 0x3adb, 0x4e64, 0x5fed, 0x6d76, 0x7cff,
    0x9489, 0x8500, 0xb79b, 0xa612, 0xd2ad, 0xc324, 0xf1bf, 0xe036,
    0x18c1, 0x0948, 0x3bd3, 0x2a5a, 0x5ee5, 0x4f6c, 0x7df7, 0x6c7e,
    0xa50a, 0xb483, 0x8618, 0x9791, 0xe32e, 0xf2a7, 0xc03c, 0xd1b5,
    0x2942, 0x38cb, 0x0a50, 0x1bd9, 0x6f66, 0x7eef, 0x4c74, 0x5dfd,
    0xb58b, 0xa402, 0x9699, 0x8710, 0xf3af, 0xe226, 0xd0bd, 0xc134,
    0x39c3, 0x284a, 0x1ad1, 0x0b58, 0x7fe7, 0x6e6e, 0x5cf5, 0x4d7c,
    0xc60c, 0xd785, 0xe51e, 0xf497, 0x8028, 0x91a1, 0xa33a, 0xb2b3,
    0x4a44, 0x5bcd, 0x6956, 0x78df, 0x0c60, 0x1de9, 0x2f72, 0x3efb,
    0xd68d, 0xc704, 0xf59f, 0xe416, 0x90a9, 0x8120, 0xb3bb, 0xa232,
    0x5ac5, 0x4b4c, 0x79d7, 0x685e, 0x1ce1, 0x0d68, 0x3ff3, 0x2e7a,
    0xe70e, 0xf687, 0xc41c, 0xd595, 0xa12a, 0xb0a3, 0x8238, 0x93b1,
    0x6b46, 0x7acf, 0x4854, 0x59dd, 0x2d62, 0x3ceb, 0x0e70, 0x1ff9,
    0xf78f, 0xe606, 0xd49d, 0xc514, 0xb1ab, 0xa022, 0x92b9, 0x8330,
    0x7bc7, 0x6a4e, 0x58d5, 0x495c, 0x3de3, 0x2c6a, 0x1ef1, 0x0f78
]

all_bill_val_obis = []
all_bill_val_obis.append([0,0,0,1,2,255])
all_bill_val_obis.append([1,0,13,0,0,255])

all_bill_val_obis.append([1,0,1,8,0,255])
for idx in range(1, 9):
    all_bill_val_obis.append([1,0,1,8,idx,255])


all_bill_val_obis.append([1,0,5,8,0,255])
all_bill_val_obis.append([1,0,8,8,0,255])

all_bill_val_obis.append([1,0,9,8,0,255])
for idx in range(1, 9):
    all_bill_val_obis.append([1,0,9,8,idx,255])


#all_bill_val_obis.append([1,0,1,6,0,255])
for idx in range(0, 9):
    all_bill_val_obis.append([1,0,1,6,idx,255])
    all_bill_val_obis.append([1,0,1,6,idx,255])


#all_bill_val_obis.append([1,0,9,6,0,255])
for idx in range(0, 9):
    all_bill_val_obis.append([1,0,9,6,idx,255])
    all_bill_val_obis.append([1,0,9,6,idx,255])

mid_night_time_obis_det = [0,0,1,0,0,255]
mid_night_kwh_imp_obis_det = [1,0,1,8,0,255]
mid_night_kwh_exp_obis_det = [1,0,2,8,0,255]
mid_night_kvah_imp_obis_det = [1,0,9,8,0,255]
mid_night_kvah_exp_obis_det = [1,0,10,8,0,255]
mid_night_rect_en_hi_obis_det = [1,0,94,91,1,255]
mid_night_rect_en_low_obis_det = [1,0,94,91,2,255]
mid_night_kvarh_q1_obis_det = [1,0,5,8,0,255]
mid_night_kvarh_q2_obis_det = [1,0,6,8,0,255]
mid_night_kvarh_q3_obis_det = [1,0,7,8,0,255]
mid_night_kvarh_q4_obis_det = [1,0,8,8,0,255]

all_mid_night_val_obis = []
all_mid_night_val_obis.append(mid_night_time_obis_det)
all_mid_night_val_obis.append(mid_night_kwh_imp_obis_det)
all_mid_night_val_obis.append(mid_night_kwh_exp_obis_det)
all_mid_night_val_obis.append(mid_night_kvah_imp_obis_det)
all_mid_night_val_obis.append(mid_night_kvah_exp_obis_det)
all_mid_night_val_obis.append(mid_night_rect_en_hi_obis_det)
all_mid_night_val_obis.append(mid_night_rect_en_low_obis_det)
all_mid_night_val_obis.append(mid_night_kvarh_q1_obis_det)
all_mid_night_val_obis.append(mid_night_kvarh_q2_obis_det)
all_mid_night_val_obis.append(mid_night_kvarh_q3_obis_det)
all_mid_night_val_obis.append(mid_night_kvarh_q4_obis_det)


event_date_time_obis_det = [0,0,1,0,0,255]
event_curr_ir_obis_det = [1,0,31,7,0,255]
event_curr_iy_obis_det = [1,0,51,7,0,255]
event_curr_ib_obis_det = [1,0,71,7,0,255]
event_v1_obis_det = [1,0,32,7,0,255]
event_v2_obis_det = [1,0,52,7,0,255]
event_v3_obis_det = [1,0,72,7,0,255]
event_pf_r_obis_det = [1,0,33,7,0,255]
event_pf_y_obis_det = [1,0,53,7,0,255]
event_pf_b_obis_det = [1,0,73,7,0,255]
event_kwh_obis_det = [1,0,1,8,0,255]
event_code_obis_det = [0,0,96,11,0,255]

all_event_val_obis_det = []
all_event_val_obis_det.append(event_date_time_obis_det)
all_event_val_obis_det.append(event_curr_ir_obis_det)
all_event_val_obis_det.append(event_curr_iy_obis_det)
all_event_val_obis_det.append(event_curr_ib_obis_det)
all_event_val_obis_det.append(event_v1_obis_det)
all_event_val_obis_det.append(event_v2_obis_det)
all_event_val_obis_det.append(event_v3_obis_det)
all_event_val_obis_det.append(event_pf_r_obis_det)
all_event_val_obis_det.append(event_pf_y_obis_det)
all_event_val_obis_det.append(event_pf_b_obis_det)
all_event_val_obis_det.append(event_kwh_obis_det)
all_event_val_obis_det.append(event_code_obis_det)


inst_rtcdt_obis_det = [0,0,1,0,0,255]
inst_curr_ir_obis_det = [1,0,31,7,0,255]
inst_curr_iy_obis_det = [1,0,51,7,0,255]
inst_curr_ib_obis_det = [1,0,71,7,0,255]
inst_v1_obis_det = [1,0,32,7,0,255]
inst_v2_obis_det = [1,0,52,7,0,255]
inst_v3_obis_det = [1,0,72,7,0,255]
inst_pfr_obis_det = [1,0,33,7,0,255]
inst_pfy_obis_det = [1,0,53,7,0,255]
inst_pfb_obis_det = [1,0,73,7,0,255]
inst_pf_avg_obis_det = [1,0,13,7,0,255]
inst_freq_obis_det = [1,0,14,7,0,255]
inst_kva_obis_det = [1,0,9,7,0,255]
inst_kw_obis_det = [1,0,1,7,0,255]
inst_kvar_obis_det = [1,0,3,7,0,255]
inst_bill_cnt_obis_det = [0,0,0,1,0,255]
inst_bill_date_obis_det = [0,0,0,1,2,255]
inst_kwh_obis_det = [1,0,1,8,0,255]
inst_kvarh_lag_obis_det = [1,0,5,8,0,255]
inst_kvarh_lead_obis_det = [1,0,8,8,0,255]
inst_kvah_obis_det = [1,0,9,8,0,255]
inst_pw_fail_cnt_obis_det = [0,0,96,7,0,255]
inst_tamper_cnt_obis_det = [0,0,94,91,0,255]
inst_programing_cnt_obis_det = [0,0,96,2,0,255]
inst_pw_fail_dur_obis_det = [0,0,94,91,8,255]


all_inst_val_obis_det = []
all_inst_val_obis_det.append(inst_rtcdt_obis_det)
all_inst_val_obis_det.append(inst_curr_ir_obis_det)
all_inst_val_obis_det.append(inst_curr_iy_obis_det)
all_inst_val_obis_det.append(inst_curr_ib_obis_det)
all_inst_val_obis_det.append(inst_v1_obis_det)
all_inst_val_obis_det.append(inst_v2_obis_det)
all_inst_val_obis_det.append(inst_v3_obis_det)
all_inst_val_obis_det.append(inst_pfr_obis_det)
all_inst_val_obis_det.append(inst_pfy_obis_det)
all_inst_val_obis_det.append(inst_pfb_obis_det)
all_inst_val_obis_det.append(inst_pf_avg_obis_det)
all_inst_val_obis_det.append(inst_freq_obis_det)
all_inst_val_obis_det.append(inst_kva_obis_det)
all_inst_val_obis_det.append(inst_kw_obis_det)
all_inst_val_obis_det.append(inst_kvar_obis_det)
all_inst_val_obis_det.append(inst_bill_cnt_obis_det)
all_inst_val_obis_det.append(inst_bill_date_obis_det)
all_inst_val_obis_det.append(inst_kwh_obis_det)
all_inst_val_obis_det.append(inst_kvarh_lag_obis_det)
all_inst_val_obis_det.append(inst_kvarh_lead_obis_det)
all_inst_val_obis_det.append(inst_kvah_obis_det)
all_inst_val_obis_det.append(inst_pw_fail_cnt_obis_det)
all_inst_val_obis_det.append(inst_tamper_cnt_obis_det)
all_inst_val_obis_det.append(inst_programing_cnt_obis_det)
all_inst_val_obis_det.append(inst_pw_fail_dur_obis_det)


ls_blk_date_det = [0,0,1,0,0,255]
ls_curr_ir_obis_det = [1,0,31,27,0,255]
ls_curr_iy_obis_det = [1,0,51,27,0,255]
ls_curr_ib_obis_det = [1,0,71,27,0,255]
ls_v1_obis_det = [1,0,32,27,0,255]
ls_v2_obis_det = [1,0,52,27,0,255]
ls_v3_obis_det = [1,0,72,27,0,255]
ls_freq_obis_det = [1,0,14,27,0,255]
ls_kwh_e_obis_det = [1,0,2,29,0,255]
ls_kwh_i_obis_det = [1,0,1,29,0,255]
ls_kvarh_lg_obis_det = [1,0,5,29,0,255]
ls_kvarh_ld_obis_det = [1,0,8,29,0,255]
ls_kvah_e_obis_det = [1,0,10,29,0,255]
ls_kvah_i_obis_det = [1,0,9,29,0,255]

all_ls_val_obis_det = []
all_ls_val_obis_det.append(ls_blk_date_det)
all_ls_val_obis_det.append(ls_curr_ir_obis_det)
all_ls_val_obis_det.append(ls_curr_iy_obis_det)
all_ls_val_obis_det.append(ls_curr_ib_obis_det)
all_ls_val_obis_det.append(ls_v1_obis_det)
all_ls_val_obis_det.append(ls_v2_obis_det)
all_ls_val_obis_det.append(ls_v3_obis_det)
all_ls_val_obis_det.append(ls_freq_obis_det)
all_ls_val_obis_det.append(ls_kwh_e_obis_det)
all_ls_val_obis_det.append(ls_kwh_i_obis_det)
all_ls_val_obis_det.append(ls_kvarh_lg_obis_det)
all_ls_val_obis_det.append(ls_kvarh_ld_obis_det)
all_ls_val_obis_det.append(ls_kvah_e_obis_det)
all_ls_val_obis_det.append(ls_kvah_i_obis_det)

#   -----------------------------------------------------------------------

class main_class():
    
    def __init__(self, imei, ser_num):
        fun_name = "main()"

        self.seq_num = 1
        self.met_loc = "cms_bangalore"

        self.imei = imei
        self.g_conn = None
        self.log_file_name = LOG_FILE_PATH + "_" + self.imei + ".log"

        self.modem_sock_fd = None
        self.sock_fd = None
        self.ser_fd = None
        self.file_ptr = None
        
        self.dbg_msg = ['[Inform]', '[Warning]', '[Severe]', '[Fatal]', '[Report]']
        self.INFORM = self.dbg_msg[0]
        self.WARNING = self.dbg_msg[1]
        self.SEVERE = self.dbg_msg[2]
        self.FATAL = self.dbg_msg[3]
        self.REPORT = self.dbg_msg[4]

        self.src_addr = 3
        self.live_app_flag = True

        self.local_byte_arr = bytearray()
        self.dest_addr = 0
        self.recv_seq_num = 0
        self.send_seq_num = 0
        self.ctrl_field = 0

        self.KEPP_ALIVE_FLAG = 0x0F
        self.START_END_FLAG = 0x7E
        self.FRAME_FORMAT_TYPE = 0xA000
        self.POLL_FINAL_BIT = 0x10

        self.METER_PASSWORD = 'lnt1'
        self.send_get_next_block_flag = 0
        #Frame types
        self.CTRL_SNRM_FRAME = 0x83
        self.CTRL_DISC_FRAME = 0x43
        self.I_FRAME = 0x00 # last bit should be zero
        self.CTRL_RR_FRAME = 0x01
        self.CTRL_RNR_FRAME = 0x05
        self.AARQ_FRAME = 0x60
        self.AARE_FRAME = 0x61
        self.GET_REQUEST_NORMAL = 0xC001
        self.GET_REQUEST_NEXT_DATA_BLOCK = 0xC002

        #FCS related
        self.PPPINITFCS16 = 0xFFFF  
        self.PPPGOODFCS16 = 0xF0B8

        self.get_next_req_flag = 0
        self.send_next_blk_cnt = 1
        self.send_next_bill_cnt = 1

        self.last_ls_qry = 0
        self.last_mid_night_qry = 0
        self.last_billing_qry = 0


        self.g_send_seq = 0
        self.g_recv_seq = 0
        
        self.config_det = {
            "host_ip":"",
            "host_port":"",
            "modem_ip":"",
            "modem_port":"",
            "conn_type":"",
            "com_port":"",
            "baudrate":"",
            "mdas_db_port":"",
            "mdas_db_name":"",
            "mdas_db_ip":"",
            "mdas_db_user":""
            }

        # Name plate related
        self.meter_ser_num = "MET_SIM_%d"%((ser_num))
        
        self.meter_manf_name = "CMS L&T"
        self.meter_fw_ver = "CMS19.00"
        self.meter_type = 2
        self.meter_ct_ratio = 1.0
        self.meter_pt_ratio = 1.0

        self.meter_max_bill_cnt = 1

        self.np_meter_ser_num_obis = [0, 0, 96, 1, 0, 255]
        self.np_meter_manf_obis = [0, 0, 96, 1, 1, 255]
        self.np_meter_fw_obis = [1, 0, 0, 2, 0, 255]
        self.np_meter_type_obis = [0, 0, 94, 91, 9, 255]
        self.np_meter_ct_ratio_obis = [1, 0, 0, 4, 2, 255]
        self.np_meter_pt_ratio_obis = [1, 0, 0, 4, 3, 255]

        self.sent_inst_flag = 0
        self.sent_hc_flag = 0

        self.inst_scale_obis_code = [1, 0, 94, 91, 3, 255]
        self.inst_val_obis_code = [1, 0, 94, 91, 0, 255]

        self.int_per_blk_obis_code = [1, 0, 0, 8, 4, 255]
        self.ls_scaler_obis_code = [1, 0, 94, 91, 4, 255]
        self.ls_scaler_val_obis_code = [1, 0, 94, 91, 4, 255]
        self.ls_block_val_obis_code = [1, 0, 99, 1, 0, 255]

        self.event_val_obis_code = [0, 0, 99, 98, 0, 255]
        self.event_scaler_obis_code = [1, 0, 94, 91, 7, 255]
        self.event_scaler_val_code = [1, 0, 94, 91, 7, 255]

        self.max_num_event = 6
        self.event_param_0_obis_code = [0, 0, 99, 98, 0, 255]
        self.event_param_1_obis_code = [0, 0, 99, 98, 1, 255]
        self.event_param_2_obis_code = [0, 0, 99, 98, 2, 255]
        self.event_param_3_obis_code = [0, 0, 99, 98, 3, 255]
        self.event_param_4_obis_code = [0, 0, 99, 98, 4, 255]
        self.event_param_5_obis_code = [0, 0, 99, 98, 5, 255]
        self.event_param_6_obis_code = [0, 0, 99, 98, 6, 255]

        self.all_event_param_obis_code = []

        self.all_event_param_obis_code.append(self.event_param_0_obis_code)
        self.all_event_param_obis_code.append(self.event_param_1_obis_code)
        self.all_event_param_obis_code.append(self.event_param_2_obis_code)
        self.all_event_param_obis_code.append(self.event_param_3_obis_code)
        self.all_event_param_obis_code.append(self.event_param_4_obis_code)
        self.all_event_param_obis_code.append(self.event_param_5_obis_code)
        self.all_event_param_obis_code.append(self.event_param_6_obis_code)

        self.max_num_mid_night_data = 30
        self.mid_night_val_obis_code = [1, 0, 99, 2, 0, 255]
        self.mid_night_scaler_obis_code = [1, 0, 94, 91, 5, 255]
        self.mid_night_scaler_val_obis_code = [1, 0, 94, 91, 5, 255]

        self.bill_entry_order_obis_code = [1,0,98,1,0,255]
        self.bill_val_obis_code = [1,0,98,1,0,255]
        self.bill_scaler_obis_code = [1,0,94,91,6,255]
        self.bill_scaler_val_obis_code = [1,0,94,91,6,255]


        if not self.read_basic_cfg():
            dbg_buf = "%-25s : Read basic config failed" % (fun_name)
            self.dbg_log(self.REPORT, dbg_buf)
            sys.exit(0)
        
        if not self.mdas_db_init():
            dbg_buf = "%-25s : Database init failed" % (fun_name)
            self.dbg_log(self.REPORT, dbg_buf)
            sys.exit(0)
            
        if "TCP/IP" in self.config_det["conn_type"]:
            if not self.modem_sock_init():
                dbg_buf = "%-25s : Modem Socket Initilization failed" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                sys.exit(0)
                
            if not self.sock_init():
                dbg_buf = "%-25s : Mdas Socket Initilization failed" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                sys.exit(0)

            self.store_eng_inst_init_val()

            dbg_buf = "%-25s : Recvd imei : %s, MeterSerNum : %s"%(fun_name, self.imei, self.meter_ser_num)
            self.dbg_log(self.INFORM, dbg_buf)

            self.mdas_thred = threading.Thread(target=self.proc_mdas_thread)
            self.mdas_thred.start()

            self.modem_thred = threading.Thread(target=self.proc_modem_thread)
            self.modem_thred.start()

        elif "SERIAL" in self.config_det["conn_type"]:
            if not self.serial_init():
                dbg_buf = "%-25s : Serial Initilization failed" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                sys.exit(0)
            self.serial_thread()

        try:
            while self.live_app_flag == True:
                time.sleep(1)
                
                if self.g_conn == None:
                    if not self.mdas_db_init():
                        dbg_buf = "%-25s : Database Re-init failed" % (fun_name)
                        self.dbg_log(self.REPORT, dbg_buf)
                        
            time.sleep(2)
            print "````````````````````````````````````````````````"
            sys.exit(0)

        except KeyboardInterrupt:
            self.live_app_flag = False
            time.sleep(2)
            sys.exit(0)

    def proc_modem_thread(self):
        fun_name = "proc_modem_thread()"

        time.sleep(2)
        self.modem_sock_fd.send(self.imei)
        
        time.sleep(2)
        self.send_modem_startup_msg()

        time.sleep(2)
        self.send_power_on_msg()
        
        while self.live_app_flag == True:
            curr_time = datetime.now()

            if((curr_time.minute%5)==0):
                if self.sent_hc_flag == 0:
                    dbg_buf = "%-25s : Sending HC Message" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)
                    self.send_modem_startup_msg()
                    self.sent_hc_flag = 1
            else:
                self.sent_hc_flag = 0

            #if((int(time.time()) - curr_time_in_sec) > 3*60):
                #dbg_buf = "%-25s : Sending HC Message" % (fun_name)
                #self.dbg_log(self.INFORM, dbg_buf)
                #self.send_modem_startup_msg()
                #curr_time_in_sec = int(round(time.time()))
                
            if((curr_time.minute%15)==0):
                if self.sent_inst_flag == 0:
                    self.sent_inst_flag = 1
                    #dbg_buf = "%-25s : Sending Inst period value Message" % (fun_name)
                    #self.dbg_log(self.INFORM, dbg_buf)
                    #self.proc_inst_val_type()
                    #self.send_period_inst_val_msg()
                    #for idx in range(0, len(self.all_event_param_obis_code)):
                        #self.send_event_val(idx)
                        #self.send_period_event_data()
            else:
                self.sent_inst_flag = 0

            self.check_modem_cmd()

        time.sleep(2)
        dbg_buf = "%-25s : Modem thread error, returning from here"% (fun_name)
        self.dbg_log(self.INFORM, dbg_buf)
        
            
    def check_modem_cmd(self):
        fun_name = "check_modem_cmd()"

        curr_time = int(time.time())
        try:
            while self.live_app_flag == True:
                try:
                    read_byte = ""
                    diff_time = int(time.time()) - curr_time
                    if (diff_time > 3):
                        dbg_buf = "%-25s : Read Time out, Reff Time is 3 sec."% (fun_name)
                        #self.dbg_log(self.INFORM, dbg_buf)
                        break
                    
                    read_byte = self.modem_sock_fd.recv(256)
                    if(read_byte):
                        #dbg_buf = "%-25s : Recv command form mdas server to Modem , Byte : %d"% (fun_name, len(read_byte))
                        #self.dbg_log(self.INFORM, dbg_buf)
                        #self.print_recv_data(read_byte)
                        self.proc_modem_cmd(read_byte)
                        break
                
                except socket.timeout:
                    continue
                
                except Exception as e:
                    dbg_buf = "%-25s : Tcp Cmd soket read Error : %s" % (fun_name, e)
                    self.dbg_log(self.INFORM, dbg_buf)
                    self.live_app_flag = False
                    time.sleep(2)

        except (KeyboardInterrupt):
            dbg_buf = "%-25s : Keyboard Interrupt in  Tcp cmd sock read" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
                
            self.live_app_flag = False
            time.sleep(2)
            if not self.modem_sock_fd == None:
                self.modem_sock_fd.close()

    def proc_modem_cmd(self, cmd_msg):
        fun_name = "proc_modem_cmd()"
        try:
            self.byte_arr = bytearray()

            hex_arr =  cmd_msg.encode('hex')
  
            if not len(hex_arr)%2 == 0:
                hex_arr = '0' + hex_arr

            for idx in range(0,len(hex_arr),2):
                self.byte_arr.append((int(hex_arr[idx],16) << 4) | int(hex_arr[idx+1], 16))

            if (self.byte_arr[0] == self.KEPP_ALIVE_FLAG) or (self.byte_arr[len(self.byte_arr)-1] == self.KEPP_ALIVE_FLAG):
                data = '0f'
                self.modem_sock_fd.send(data.decode('hex'))
                dbg_buf = "%-25s : Recvd Keep Alive cmd, sending back resp " % (fun_name)
                self.dbg_log(self.INFORM, dbg_buf)
                return

            dbg_buf = "%-25s : Recv Command : %s"% (fun_name, cmd_msg)
            self.dbg_log(self.INFORM, dbg_buf)  

            if "AT_COMMAND" in cmd_msg:
                ack_data = "ACK/%s/%s/%s/%s"%(self.imei, "AT_COMMAND", "SUCCESS", (cmd_msg.split('>')[1]).split('<')[0])
                
                self.modem_sock_fd.send(ack_data)
                dbg_buf = "%-25s : Sending ACK : %s for command : %s"% (fun_name, ack_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)
                
                resp_data = "RESPONSE/%s/%s/%s/%s/%s/%s"%(self.imei, "AT_COMMAND", "SUCCESS", "", "", (cmd_msg.split('>')[1]).split('<')[0])
                self.modem_sock_fd.send(resp_data)
                
                dbg_buf = "%-25s : Sending Response : %s for command : %s"% (fun_name, resp_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)

            if "GET_HC_MESSAGE" in cmd_msg:
                ack_data = "ACK/%s/%s/%s/%s"%(self.imei, "GET_HC_MESSAGE", "SUCCESS", (cmd_msg.split('>')[1]).split('<')[0])
                
                self.modem_sock_fd.send(ack_data)
                dbg_buf = "%-25s : Sending ACK : %s for command : %s"% (fun_name, ack_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)

                self.send_modem_startup_msg()

                resp_data = "RESPONSE/%s/%s/%s/%s/%s/%s"%(self.imei, "GET_HC_MESSAGE", "SUCCESS", "", "", (cmd_msg.split('>')[1]).split('<')[0])
                self.modem_sock_fd.send(resp_data)
                
                dbg_buf = "%-25s : Sending Response : %s for command : %s"% (fun_name, resp_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)

            if "RESET" in cmd_msg:
                ack_data = "ACK/%s/%s/%s/%s"%(self.imei, "RESET", "SUCCESS", (cmd_msg.split('>')[1]).split('<')[0])
                
                self.modem_sock_fd.send(ack_data)
                dbg_buf = "%-25s : Sending ACK : %s for command : %s"% (fun_name, ack_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)
                
                resp_data = "RESPONSE/%s/%s/%s/%s/%s/%s"%(self.imei, "RESET", "SUCCESS", "", "", (cmd_msg.split('>')[1]).split('<')[0])
                self.modem_sock_fd.send(resp_data)
                
                dbg_buf = "%-25s : Sending Response : %s for command : %s"% (fun_name, resp_data, cmd_msg)
                self.dbg_log(self.INFORM, dbg_buf)

                self.live_app_flag = False

        except Exception as e:
            dbg_buf = "%-25s : Command socket resp error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)


    def serial_thread(self):
        fun_name = "serial_thread()"

        if not self.proc_msg():
            dbg_buf = "%-25s : Problems in Serial thread  need to close" % (fun_name)
            self.dbg_log(self.REPORT, dbg_buf)

            self.live_app_flag = False
            time.sleep(2)
            
            if not self.sock_fd == None:
                self.sock_fd.close()
            
            if not self.ser_fd == None:
                if self.ser_fd.is_open:
                    self.ser_fd.close()
                
            time.sleep(2)
            
            sys.exit(0)
            
    def proc_mdas_thread(self):
        fun_name = "proc_mdas_thread"
        try:
            dbg_buf = "%-25s : Sending 1st time imei num on mdas port," % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
            
            self.sock_fd.send(self.imei)
            time.sleep(2)
            
            fun_ret = self.proc_msg()
            if not fun_ret:
                dbg_buf = "%-25s : Problems in main function need to close" % (fun_name)
                self.dbg_log(self.REPORT, dbg_buf)

                self.live_app_flag = False
                time.sleep(2)
                
                if not self.sock_fd == None:
                    self.sock_fd.close()
                
                if not self.ser_fd == None:
                    if self.ser_fd.is_open:
                        self.ser_fd.close()
                    
                time.sleep(2)
                
                sys.exit(0)
                
        except (KeyboardInterrupt, SystemExit, Exception) as e:
                dbg_buf = "%-25s : KeyboardInterrupt or Problems in mdas_thread need to close in  read , Error : %s" % (fun_name, e)
                self.dbg_log(self.INFORM, dbg_buf)
                self.live_app_flag = False
            
                time.sleep(2)
                sys.exit(0)

    def write_into_file(self, dbg_buf):
        if not self.file_ptr:
            self.file_ptr = open(self.log_file_name, 'w')
            self.file_ptr.write(dbg_buf)
        else:
            self.file_ptr = open(self.log_file_name, 'a')
            self.file_ptr.write(dbg_buf)

        statinfo = os.stat(self.log_file_name)
        if(statinfo.st_size > MAX_FILE_SIZE):
            self.file_ptr.close()
            os.remove(self.log_file_name)
            self.file_ptr = open(self.log_file_name, 'w')


    def dbg_log(self, mode, dbg_str):
        dbg_buf = '%s : %s : %s\n' % ((datetime.now().strftime('%d_%b_%Y %H:%M:%S'), mode, dbg_str))
        print (dbg_buf)
        self.write_into_file(dbg_buf)
        

    def read_qry(self):
        fun_name = "read_qry()"
        idx = 0
        loc_read_byte = []

        curr_time = int(time.time())
        while self.live_app_flag == True:
            try:
                read_byte = ""
                diff_time = int(time.time()) - curr_time
                if (diff_time >= 2):
                    dbg_buf = "%-25s : Read Time out, Reff Time is 3 sec."% (fun_name)
                    #self.dbg_log(self.INFORM, dbg_buf)
                    break
                    
                if self.config_det["conn_type"] == "TCP/IP":
                    read_byte = self.sock_fd.recv(128)
                    
                elif self.config_det["conn_type"] == "SERIAL":
                    read_byte = self.ser_fd.read()

                if(read_byte):
                    #print hex(ord(read_byte))[2:]
                    loc_read_byte.append(read_byte)
                    self.recv_tcp_data = self.recv_tcp_data + read_byte
                    idx = idx + len(read_byte)
                    """
                    if hex(ord(read_byte))[2:] == '7e' and idx == 0 :
                        start_flag_recv = 1
                        idx = idx + 1
                    elif hex(ord(read_byte))[2:] == '7e':
                        idx = idx + 1
                        if idx>2:
                            reff = hex(ord(loc_read_byte[2]))[2:]
                            if int(reff, 16) + 2 == (len(loc_read_byte)):
                                break
                    else:
                        idx = idx + 1
                    """
                
            except (KeyboardInterrupt, SystemExit):
                dbg_buf = "%-25s : Keyboard Interrupt in  read" % (fun_name)
                self.dbg_log(self.INFORM, dbg_buf)
                self.live_app_flag = False
                time.sleep(2)
                break
            
            except socket.timeout:
                continue
            
        return idx


    def proc_msg(self):
        fun_name = "proc_msg()"

        while self.live_app_flag == True:
            time.sleep(0.1)
            try:
                self.recv_tcp_data = ""
                read_ret = self.read_qry()
                if(read_ret == 1):
                    if hex(ord(self.recv_tcp_data))[2:] == 'f':
                        dbg_buf = "%-25s : -->>>Recvd Keep Alive cmd, sending back resp" % (fun_name)
                        self.dbg_log(self.INFORM, dbg_buf)
                        data = '0f'
                        self.sock_fd.send(data.decode('hex'))
                        continue
                        
                if(read_ret > 8):
                    dbg_buf = "%-25s : Msg recv on socket Numbyte : %d" % (fun_name, len(self.recv_tcp_data))
                    self.dbg_log(self.INFORM, dbg_buf)
                    
                    self.print_recv_data(self.recv_tcp_data)
                    
                    self.proc_recv_tcp_msg(self.recv_tcp_data)
                    
            except (KeyboardInterrupt, SystemExit, Exception ) as e:
                dbg_buf = "%-25s : Keyboard Interrupt in Error : %s" % (fun_name, e)
                self.dbg_log(self.INFORM, dbg_buf)
                break
            
            except socket.timeout:
                continue
            
        dbg_buf = "%-25s : live_app_flag false Need to close every things" % (fun_name)
        self.dbg_log(self.INFORM, dbg_buf)

        if not self.sock_fd == None:
            self.sock_fd.close()
            
        if not self.ser_fd == None:
            if self.ser_fd.is_open:
                self.ser_fd.close()
        
        return False

    def proc_recv_tcp_msg(self, msg):
        fun_name = "proc_recv_tcp_msg()"

        try:
            self.local_byte_arr1 = bytearray()
            self.local_byte_arr = bytearray()
            hex_arr =  (hex(int(msg.encode('hex'), 16))[2:])[:-1]

            #print "len of total hex arr :",len(hex_arr)

            if not len(hex_arr)%2 == 0:
                hex_arr = '0'+hex_arr

            #print "len of total hex arr :",len(hex_arr)
            
            for idx in range(0,len(hex_arr),2):
                self.local_byte_arr1.append((int(hex_arr[idx],16) << 4) | int(hex_arr[idx+1], 16))

            if self.local_byte_arr1[0] == self.KEPP_ALIVE_FLAG:
                data = '0f'
                self.sock_fd.send(data.decode('hex'))
                dbg_buf = "%-25s : 1st byte Recvd Keep Alive cmd, sending back resp " % (fun_name)
                self.dbg_log(self.INFORM, dbg_buf)

                if self.local_byte_arr1[1] == self.START_END_FLAG:
                    #print "qry len : ",self.local_byte_arr1[3]
                    #self.local_byte_arr = self.local_byte_arr1[1:]
                    for idx in range (0, self.local_byte_arr1[3]+2):
                        self.local_byte_arr.append(self.local_byte_arr1[1+idx])
            
            elif self.local_byte_arr1[len(self.local_byte_arr1)-1] == self.KEPP_ALIVE_FLAG:
                data = '0f'
                self.sock_fd.send(data.decode('hex'))
                dbg_buf = "%-25s : Last Byte Recvd Keep Alive cmd, sending back resp " % (fun_name)
                self.dbg_log(self.INFORM, dbg_buf)

                if self.local_byte_arr1[0] == self.START_END_FLAG:
                    #self.local_byte_arr = self.local_byte_arr1[0:-1]
                    for idx in range (0, self.local_byte_arr1[2]+2):
                        self.local_byte_arr.append(self.local_byte_arr1[idx])
            else:
                self.local_byte_arr = self.local_byte_arr1

            #print "--------------------->>>>>>>>>>",len(self.local_byte_arr),len(self.local_byte_arr1)
            if len(self.local_byte_arr) < 9:
                dbg_buf = "%-25s : Invalid qry recv len less than 9."%(fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                return False
                
            if not self.validate_qry(self.local_byte_arr):
                dbg_buf = "%-25s : Validation failed Invalid qry recv ."%(fun_name)
                self.dbg_log(self.REPORT, dbg_buf)
                return False
            
            if(self.local_byte_arr[3] > 16):
                dbg_buf = "%-25s : Invalid Address recv : %X" % (
                    fun_name, self.local_byte_arr[3])
                self.dbg_log(self.INFORM, dbg_buf)

                return False

            self.src_addr = self.local_byte_arr[3]
            
            self.dest_addr = self.local_byte_arr[4]

            self.ctrl_field = self.local_byte_arr[5]
            
            if(self.ctrl_field == (self.CTRL_SNRM_FRAME | self.POLL_FINAL_BIT)):
                self.recv_seq_num = 0
                self.send_seq_num = 0
                self.send_snrm_resp()
                
            elif(self.ctrl_field == (self.CTRL_DISC_FRAME | self.POLL_FINAL_BIT)):
                self.send_disc_resp()

            if((self.ctrl_field & 0X01) == 0):
                
                self.send_seq_num = (self.ctrl_field >> 1) & 0X01
                self.send_seq_num = (self.send_seq_num << 1) | (self.ctrl_field >> 2) & 0X01
                self.send_seq_num = (self.send_seq_num << 1) | (self.ctrl_field >> 3) & 0X01

                self.recv_seq_num = self.ctrl_field >> 5
                self.recv_seq_num = (self.recv_seq_num << 1) | (self.ctrl_field >> 6) & 0X01
                self.recv_seq_num = (self.recv_seq_num << 1) | (self.ctrl_field >> 7) & 0X01

                #self.recv_seq_num = ((self.send_seq_num + 1) % 8)
                self.send_seq_num = ((self.recv_seq_num + 1) % 8)

                if self.local_byte_arr[12] == 0X02:
                    type_resp = ""
                    self.send_get_next_block_flag = 1

                    if self.last_ls_qry == 1:
                        type_resp = "Load survey Block data"
                        self.send_blk_data_resp()

                    if self.last_mid_night_qry == 1:
                        type_resp = "Mid Night data"
                        self.send_mid_night_val(1)

                    if self.last_billing_qry == 1:
                        type_resp = "Billing data"
                        self.proc_billing_value_qry(1)
                        
                    dbg_buf = "%-25s : Recv GetNextReqst. Sent Resp for %s" % (fun_name,type_resp)
                    self.dbg_log(self.INFORM, dbg_buf)
                    return True
                else:
                    self.send_get_next_block_flag = 0
                    
                    
                self.intf_class = 0
                self.intf_class = (self.local_byte_arr[14] << 8) | self.local_byte_arr[15]

                self.attribute = 0
                self.attribute = (self.local_byte_arr[len(self.local_byte_arr) - 4] << 8) | self.local_byte_arr[len(self.local_byte_arr) - 5]

                dbg_buf = "%-25s : I frame Qry IntfClass : %X attribute : %X" % (fun_name, self.intf_class, self.attribute)
                self.dbg_log(self.INFORM, dbg_buf)

                if(self.local_byte_arr[11] == 0X60):
                    dbg_buf = "%-25s : AARQ frame Qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.meter_pass_len = self.local_byte_arr[40]

                    self.meter_pass = ""

                    for idx in range(0, self.meter_pass_len):
                        hex_val = self.local_byte_arr[41 + idx]
                        self.meter_pass = self.meter_pass + "%X"%(hex_val)

                    dbg_buf = "%-25s : AARQ frame Qry, Pass Len : %d MeterPassLen : %d MeterPass : %s" % (
                        fun_name, self.meter_pass_len, len(self.meter_pass), self.meter_pass)
                    self.dbg_log(self.INFORM, dbg_buf)
                    
                    if(self.meter_pass.decode('hex') == self.METER_PASSWORD):
                        self.send_aare_frame(0)
                    else:
                        self.send_aare_frame(1)

                elif((self.intf_class == 0X0008) and (self.attribute == 0X0002)):
                    dbg_buf = "%-25s : I frame date time qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.send_date_time_resp()

                elif((self.intf_class == 0X0001) and (self.attribute == 0X0002)):
                    dbg_buf = "%-25s : I frame Name plate Info qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])
                        
                    if self.int_per_blk_obis_code == self.recv_obis:
                        self.proc_int_period_blk_resp(self.recv_obis)
                    else:
                        self.send_name_plate_resp(self.recv_obis)

                elif((self.intf_class == 0X0007) and (self.attribute == 0X0003)):
                    dbg_buf = "%-25s : I frame  val Obis By Group qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])
                    
                    self.inst_obis_by_grp(self.recv_obis)

                elif((self.intf_class == 0X0007) and (self.attribute == 0X0002)):
                    dbg_buf = "%-25s : I frame  scalar Val By Group qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])
                    
                    self.inst_val_by_grp(self.recv_obis)

                elif((self.intf_class == 0X0007) and (self.attribute == 0X0001)):
                    dbg_buf = "%-25s : I frame Load survey data value qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])
                    
                    self.proc_ls_data_block(self.recv_obis)

                elif((self.intf_class == 0X0007) and (self.attribute == 0X0005)):
                    dbg_buf = "%-25s : I frame Event entry data value qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])

                    if self.bill_entry_order_obis_code == self.recv_obis:
                        self.send_bill_entry_resp()
                    else:
                        self.proc_event_rel_qry(self.recv_obis)
                    
                elif((self.intf_class == 0X0007) and (self.attribute == 0X0007)):
                    dbg_buf = "%-25s : I frame Num Of Event value qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])

                    if self.bill_entry_order_obis_code == self.recv_obis:
                        self.send_num_bill_entry_resp()
                    else:
                        self.proc_num_event_qry(self.recv_obis)

                elif((self.intf_class == 0X0007) and (self.attribute == 0X0000)):
                    dbg_buf = "%-25s : I frame Event value qry" % (fun_name)
                    self.dbg_log(self.INFORM, dbg_buf)

                    self.recv_obis = []
                    for idx in range (0,6):
                        self.recv_obis.append(self.local_byte_arr[16 + idx])

                    if self.bill_entry_order_obis_code == self.recv_obis:
                        self.proc_billing_value_qry(0)
                    else:
                        self.proc_event_value_qry(self.recv_obis)

        except Exception as e:
            dbg_buf = "%-25s : Error in proc msg : %s" % (fun_name, e)
            self.dbg_log(self.INFORM, dbg_buf)

    def validate_qry(self, msg):
        
        fun_name = "validate_qry()"

        qry_len = len(msg)

        if((self.local_byte_arr[0] != self.START_END_FLAG) or
           (self.local_byte_arr[len(msg)-1] != self.START_END_FLAG)):
            dbg_buf = "%-25s : InValid start end flag recv" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
            return False
        
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.local_byte_arr[1:6],5)
        cal_check_sum ^= 0xFFFF
                        
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
  
        if((self.local_byte_arr[6] != high_fcs) or (self.local_byte_arr[7] != low_fcs)):
            dbg_buf = "%-25s : Header checksum validation failed high_fcs %X low_fcs %X " % (fun_name, high_fcs, low_fcs)
            self.dbg_log(self.INFORM, dbg_buf)
            return False

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.local_byte_arr[1:qry_len-3],(qry_len-4))
        cal_check_sum ^= 0xFFFF
                        
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
                                 
        if((self.local_byte_arr[qry_len-3] != high_fcs) or (self.local_byte_arr[qry_len-2] != low_fcs)):
            dbg_buf = "%-25s : app checksum validation failed high_fcs : %X low_fcs %X " % (fun_name, high_fcs, low_fcs)
            self.dbg_log(self.INFORM, dbg_buf)
            return False
        
        return True

    def send_resp(self, msg):

        if "TCP/IP" in self.config_det["conn_type"]:
            self.sock_fd.send(msg)
        elif "SERIAL" in self.config_det["conn_type"]:
            self.ser_fd.write(msg)

    def serial_init(self):
        fun_name = "serial_init()"

        try:
            self.comm_port = self.config_det["com_port"]
            dbg_buf = "%-25s : Selected Comm Port : %s" % (fun_name,self.comm_port)
            self.dbg_log(self.INFORM, dbg_buf)

            self.ser_fd = serial.Serial()
            self.ser_fd.port = self.comm_port
            self.ser_fd.baudrate = int(self.config_det["baudrate"])
            self.ser_fd.timeout = 0.1

            self.ser_fd.open()

            dbg_buf = "%-25s : Port : %s has opened succesfully" % (fun_name,self.comm_port)
            self.dbg_log(self.INFORM, dbg_buf)

            return True
            
        except Exception as e:
            dbg_buf = "%-25s : Error in init serial : %s" % (fun_name,e)
            self.dbg_log(self.INFORM, dbg_buf)
            return False

    def print_recv_data(self, msg):
        frame_len = 16
        fame_cnt = 1
        idx = 0

        dbg_buf = ""

        for data in msg:
            idx = idx + 1
            loc_data = ""
            loc_data = hex(ord(data))[2:] + " "
            dbg_buf = dbg_buf + loc_data
            if (idx == (fame_cnt * frame_len)):
                self.dbg_log(self.INFORM, dbg_buf)
                
                fame_cnt = fame_cnt + 1
                dbg_buf = ""
                
        self.dbg_log(self.INFORM, dbg_buf)

    def modem_sock_init(self):
        fun_name = "modem_sock_init()"
        try:
            self.modem_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.modem_sock_fd.settimeout(1.0)
            self.server_address = (self.config_det["host_ip"], self.config_det["modem_port"])
            
            #print "%-25s : Trying to connect to server Host %s"%(fun_name,self.config_det["host_ip"])

            self.modem_sock_fd.connect(self.server_address)

            dbg_buf =  "%-25s : connected  to Host : %s port : %s" % (
                fun_name, self.config_det["host_ip"], self.config_det["modem_port"])
            self.dbg_log(self.INFORM, dbg_buf)
            
            return True
        
        except Exception as e:
            dbg_buf = "%-25s : Modem Create Socket error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False
        
    def sock_init(self):
        fun_name = "sock_init()"
        try:
            self.sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock_fd.settimeout(1.0)
            
            self.server_address = (self.config_det["host_ip"], self.config_det["host_port"])
            
            #print "%-25s : Trying to connect to server Host %s"%(fun_name,self.config_det["host_ip"])

            self.sock_fd.connect(self.server_address)

            dbg_buf =  "%-25s : connected  to Host : %s port : %s" % (
                fun_name, self.config_det["host_ip"], self.config_det["host_port"])
            
            self.dbg_log(self.INFORM, dbg_buf)


            return True
        
        except Exception as e:
            dbg_buf = "%-25s : Create Socket error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False
        
    def read_basic_cfg(self):
        fun_name = "read_basic_cfg()"

        try:
            self.cfg_ptr = open(CONFIG_PATH, "r") 
            for line in self.cfg_ptr:
                if("HOST_IP" in line):
                    self.config_det["host_ip"] = ((line.split('='))[1])[:-1]
                elif("HOST_PORT" in line):
                    self.config_det["host_port"] = int(((line.split('='))[1])[:-1])
                elif("MODEM_PORT" in line):
                    self.config_det["modem_port"] = int(((line.split('='))[1])[:-1])
                elif("BAUDRATE" in line):
                    self.config_det["baudrate"] = int(((line.split('='))[1])[:-1])
                elif("CONN_TYPE" in line):
                    self.config_det["conn_type"] = (((line.split('='))[1])[:-1])
                elif("COM_PORT" in line):
                    self.config_det["com_port"] = (((line.split('='))[1])[:-1])
                elif("MDAS_DB_PORT" in line):
                    self.config_det["mdas_db_port"] = int((((line.split('='))[1])[:-1]))
                elif("MDAS_DB_NAME" in line):
                    self.config_det["mdas_db_name"] = (((line.split('='))[1])[:-1])
                elif("MDAS_DB_IP" in line):
                    self.config_det["mdas_db_ip"] = (((line.split('='))[1])[:-1])
                elif("MDAS_DB_USER" in line):
                    self.config_det["mdas_db_user"] = (((line.split('='))[1])[:-1])
                #

            self.cfg_ptr.close()
            
            dbg_buf = "%-25s : Basic Config read success" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
            
            return True
        
        except Exception as e:
            dbg_buf = "%-25s : Read Config file error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False
        
    def mdas_db_init(self):
        fun_name = "mdas_db_init()"

        try:
            self.g_conn = mysql.connector.connect(
                host=self.config_det["mdas_db_ip"],
                user=self.config_det["mdas_db_user"],
                passwd="softel")

            self.g_curs = self.g_conn.cursor()

            dbg_buf = "%-25s : Mysql Init done" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)

            self.create_all_table()
            return True

        except Exception as e:
	    self.g_conn = None
            dbg_buf = "%-25s : Mysql init error : %s\n" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False

    def create_all_table(self):
        fun_name = "create_all_table()"

        try:
            table_name = "BILLING_"+self.imei

            sql_qry = "CREATE TABLE IF NOT EXISTS CMS_METER_SIMULATOR.%s(\
                `UNIQUE_ID` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                `UPDATE_TIME` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
                `BILLING_DATE` datetime DEFAULT NULL,`BILLING_PROFILE` varchar(45) DEFAULT NULL,\
                `AVG_PF` float DEFAULT NULL,`CUM_ENG_KWH` float DEFAULT NULL,\
                `CUM_ENG_KWH_TZ1` float DEFAULT NULL,`CUM_ENG_KWH_TZ2` float DEFAULT NULL,\
                `CUM_ENG_KWH_TZ3` float DEFAULT NULL,`CUM_ENG_KWH_TZ4` float DEFAULT NULL,\
                `CUM_ENG_KWH_TZ5` float DEFAULT NULL,`CUM_ENG_KWH_TZ6` float DEFAULT NULL,\
                `CUM_ENG_KWH_TZ7` float DEFAULT NULL,`CUM_ENG_KWH_TZ8` float DEFAULT NULL,\
                `CUM_ENG_KVARH_LAG` float DEFAULT NULL,`CUM_ENG_KVARH_LEAD` float DEFAULT NULL,\
                `CUM_ENG_KVAH` float DEFAULT NULL,`CUM_ENG_KVAH_TZ1` float DEFAULT NULL,\
                `CUM_ENG_KVAH_TZ2` float DEFAULT NULL,`CUM_ENG_KVAH_TZ3` float DEFAULT NULL,\
                `CUM_ENG_KVAH_TZ4` float DEFAULT NULL,`CUM_ENG_KVAH_TZ5` float DEFAULT NULL,\
                `CUM_ENG_KVAH_TZ6` float DEFAULT NULL,`CUM_ENG_KVAH_TZ7` float DEFAULT NULL,\
                `CUM_ENG_KVAH_TZ8` float DEFAULT NULL,`ENERGY_ACT_TOTAL` float DEFAULT NULL,\
                `DEMAND_ACT_TOTAL_TS` datetime DEFAULT NULL,`ENERGY_APP_TOTAL` float DEFAULT NULL,\
                `DEMAND_APP_TOTAL_TS` datetime DEFAULT NULL,`DEMAND_APP_TOTAL` float DEFAULT NULL,\
                `DEMAND_ACT_TOTAL` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_1` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_1` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_2` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_2` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_3` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_3` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_4` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_4` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_5` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_5` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_6` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_6` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_7` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_7` float DEFAULT NULL,`DEMAND_ACT_TOTAL_TS_8` datetime DEFAULT NULL,\
                `ENERGY_APP_TOTAL_8` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_1` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_1` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_2` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_2` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_3` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_3` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_4` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_4` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_5` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_5` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_6` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_6` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_7` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_7` float DEFAULT NULL,`DEMAND_APP_TOTAL_TS_8` datetime DEFAULT NULL,\
                `DEMAND_APP_TOTAL_8` float DEFAULT NULL,\
                PRIMARY KEY (`UNIQUE_ID`)\
                ) ENGINE=InnoDB AUTO_INCREMENT=176 DEFAULT CHARSET=latin1"%(table_name)

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Billing table created : %s" % (fun_name, table_name)
            self.dbg_log(self.INFORM, dbg_buf)

            table_name = "EVENT_"+self.imei

            sql_qry = "CREATE TABLE IF NOT EXISTS CMS_METER_SIMULATOR.%s (\
            `UNIQUE_ID` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `UPDATE_TIME` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
            `EVENT_TIME` datetime DEFAULT NULL,\
            `EVENT_CODE` int(11) DEFAULT NULL,\
            `VOLT_1` float DEFAULT NULL,\
            `VOLT_2` float DEFAULT NULL,\
            `VOLT_3` float DEFAULT NULL,\
            `CURR_1` float DEFAULT NULL,\
            `CURR_2` float DEFAULT NULL,\
            `CURR_3` float DEFAULT NULL,\
            `PF_1` float DEFAULT NULL,\
            `PF_2` float DEFAULT NULL,\
            `PF_3` float DEFAULT NULL,\
            `ENG_KWH` float DEFAULT NULL,\
            PRIMARY KEY (`UNIQUE_ID`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1"%(table_name)

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Event table created : %s" % (fun_name, table_name)
            self.dbg_log(self.INFORM, dbg_buf)

            table_name = "INST_"+self.imei
            sql_qry = "CREATE TABLE IF NOT EXISTS CMS_METER_SIMULATOR.%s (\
            `UPDATE_TIME` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
            `INST_DATE` datetime DEFAULT NULL,\
            `CURR_1` float DEFAULT NULL,\
            `CURR_2` float DEFAULT NULL,\
            `CURR_3` float DEFAULT NULL,\
            `VOLT_1` float DEFAULT NULL,\
            `VOLT_2` float DEFAULT NULL,\
            `VOLT_3` float DEFAULT NULL,\
            `PF_1` float DEFAULT NULL,\
            `PF_2` float DEFAULT NULL,\
            `PF_3` float DEFAULT NULL,\
            `AVG_PF` float DEFAULT NULL,\
            `FREQ` float DEFAULT NULL,\
            `APP_POWER` float DEFAULT NULL,\
            `ACT_POWER` float DEFAULT NULL,\
            `REACT_POWER` float DEFAULT NULL,\
            `CUMENGY_KWH` float DEFAULT NULL,\
            `CUMENGY_KVARH_LAG` float DEFAULT NULL,\
            `CUMENGY_KVARH_LEAD` float DEFAULT NULL,\
            `CUMENGY_KVAH` float DEFAULT NULL,\
            `NUM_POWER_FAIL` float DEFAULT NULL,\
            `NUM_POWER_FAIL_DUR` float DEFAULT NULL,\
            `CUM_TAMPER_COUNT` float DEFAULT NULL,\
            `CUM_BILLING_COUNT` float DEFAULT NULL,\
            `CUM_PGM_COUNT` float DEFAULT NULL,\
            `BILLING_DATE` datetime DEFAULT NULL\
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1"%(table_name)

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Instantanious table created : %s" % (fun_name, table_name)
            self.dbg_log(self.INFORM, dbg_buf)
        
            table_name = "LOAD_SURVEY_"+self.imei
            sql_qry = "CREATE TABLE IF NOT EXISTS CMS_METER_SIMULATOR.%s (\
            `UNIQUE_ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\
            `UPDATE_TIME` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
            `LS_DATE` datetime DEFAULT NULL,\
            `VOLT_1` float DEFAULT NULL,\
            `VOLT_2` float DEFAULT NULL,\
            `VOLT_3` float DEFAULT NULL,\
            `CURR_1` float DEFAULT NULL,\
            `CURR_2` float DEFAULT NULL,\
            `CURR_3` float DEFAULT NULL,\
            `FREQ` float DEFAULT NULL,\
            `ENGY_KWH_EXP` float DEFAULT NULL,\
            `ENGY_KWH_IMP` float DEFAULT NULL,\
            `ENGY_KVARH_LAG` float DEFAULT NULL,\
            `ENGY_KVARH_LEAD` float DEFAULT NULL,\
            `ENGY_KVAH_EXP` float DEFAULT NULL,\
            `ENGY_KVAH_IMP` float DEFAULT NULL,\
            PRIMARY KEY (`UNIQUE_ID`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1"%(table_name)

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Load Survey table created : %s" % (fun_name, table_name)
            self.dbg_log(self.INFORM, dbg_buf)

        except Exception as e:
            dbg_buf = "%-25s : Create table Error : %s\n" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False

    def proc_mid_night_val(self, recv_obis, send_init_flag):

        self.mid_night_curr_date = time.time()
        self.mid_night_curr_date = self.mid_night_curr_date - ((self.max_num_mid_night_data-1)*24*60*60)
        self.send_mid_night_val(send_init_flag)
        
    def proc_event_value_qry(self, recv_obis):
        fun_name = "proc_event_value_qry()"
        self.send_byte_arr = bytearray()

        if recv_obis in self.all_event_param_obis_code:
            for idx in range(0, len(self.all_event_param_obis_code)):
                if recv_obis == self.all_event_param_obis_code[idx]:
                    self.send_event_val(idx)
                    break
        else:
            return False
        
        dbg_buf = "%-25s : Sending Event val response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)
        
    def proc_num_event_qry(self, recv_obis):
        fun_name = "proc_num_event_qry()"
        self.send_byte_arr = bytearray()


        for idx in range(0, len(self.all_event_param_obis_code)):
            if recv_obis == self.all_event_param_obis_code[idx]:
                self.send_num_event()
                break
            
        if idx == len(self.all_event_param_obis_code):
            dbg_buf = "%-25s : Obis code not matched for num of event qry"%(fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
            return False

        dbg_buf = "%-25s : Sending Num Event response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)
        
    def proc_event_rel_qry(self, recv_obis):
        fun_name = "proc_event_rel_qry()"
        self.send_byte_arr = bytearray()

        for idx in range(0, len(self.all_event_param_obis_code)):
            if recv_obis == self.all_event_param_obis_code[idx]:
                self.send_event_entry_order()
                break
            
        if idx == len(self.all_event_param_obis_code):
            dbg_buf = "%-25s : Obis code not matched for order of event entry"%(fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
            return False

        dbg_buf = "%-25s : Sending  Event order entry response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def proc_ls_data_block(self, recv_obis):
        fun_name = "proc_ls_data_block()"
        
        self.send_next_blk_cnt = 1
        
        self.st_date_time = bytearray()
        self.end_date_time = bytearray()

        self.st_date_time.append(self.local_byte_arr[47])
        self.st_date_time.append(self.local_byte_arr[48])
        self.st_date_time.append(self.local_byte_arr[49])
        self.st_date_time.append(self.local_byte_arr[50])
        self.st_date_time.append(self.local_byte_arr[52])
        self.st_date_time.append(self.local_byte_arr[53])

        self.end_date_time.append(self.local_byte_arr[61])
        self.end_date_time.append(self.local_byte_arr[62])
        self.end_date_time.append(self.local_byte_arr[63])
        self.end_date_time.append(self.local_byte_arr[64])
        self.end_date_time.append(self.local_byte_arr[66])
        self.end_date_time.append(self.local_byte_arr[67])

        try:
            fmt = '%Y-%m-%d %H:%M'
            st_time_str = "%04d-%02d-%02d %02d:%02d"%((
                (self.st_date_time[0]<<8) | self.st_date_time[1]), self.st_date_time[2],
                self.st_date_time[3], self.st_date_time[4], self.st_date_time[5])
        
            end_time_str = "%04d-%02d-%02d %02d:%02d"%((
                (self.end_date_time[0]<<8) | self.end_date_time[1]), self.end_date_time[2], self.end_date_time[3],
                                                       self.end_date_time[4], self.end_date_time[5])
        
            st_date = datetime.strptime(st_time_str, fmt)
            end_date = datetime.strptime(end_time_str, fmt)

            d1_ts = time.mktime(st_date.timetuple())
            d2_ts = time.mktime(end_date.timetuple())
            
        except Exception as e:
            dbg_buf = "%-25s : Error to form date : %s"%(fun_name, e)
            self.dbg_log(self.INFORM, dbg_buf)
            return False
            
        if(int(d2_ts - d1_ts)<0):
            dbg_buf = "%-25s : Mismatched to form date , sending error response : %s"%(fun_name, e)
            self.dbg_log(self.INFORM, dbg_buf)
            self.send_error_blk_data_resp()
            return False

        if(self.st_date_time[5]%15==0) or (self.end_date_time[5]%15==0):
            self.num_ls_block = 1 + int(d2_ts - d1_ts) / 900
        else:
            self.num_ls_block = int(d2_ts - d1_ts) / 900

        dbg_buf = "%-25s : recvd Block data qry, St dt : %s end dt : %s NumBlock : %d"%(
            fun_name, st_time_str, end_time_str, self.num_ls_block)
        
        self.dbg_log(self.INFORM, dbg_buf)
        
        self.send_blk_data_resp()
        

    def proc_int_period_blk_resp(self, recv_obis):
        fun_name = "proc_int_period_blk_resp()"

        self.send_int_period_blk_resp()
        
        dbg_buf = "%-25s : Sending Int blk period response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)
        
        
    def inst_val_by_grp(self, recv_obis):
        fun_name = "proc_all_val_by_grp()"
        dbg_buf = ""

        if(recv_obis == self.inst_scale_obis_code):
            self.proc_inst_scal_val_type()
            dbg_buf = "%-25s : Sending Inst scaler by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.inst_val_obis_code):
            self.proc_inst_val_type()
            self.update_inst_val_table()
            dbg_buf = "%-25s : Sending Inst val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.ls_scaler_val_obis_code):
            self.proc_block_scaler_val_type()
            dbg_buf = "%-25s : Sending block scaler val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.event_scaler_val_code):
            self.proc_event_scaler_val_type()
            dbg_buf = "%-25s : Sending Event scaler val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.mid_night_scaler_val_obis_code):
            self.proc_mid_night_scaler_val_type()
            dbg_buf = "%-25s : Sending Mid Night scaler val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.mid_night_val_obis_code):
            self.proc_mid_night_val(recv_obis, 0)
            dbg_buf = "%-25s : Sending Mid Night data value by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))
            
        elif(recv_obis == self.bill_scaler_val_obis_code):
            self.proc_bill_scaler_val()
            dbg_buf = "%-25s : Sending Billing val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif recv_obis in self.all_event_param_obis_code:
            for idx in range(0, len(self.all_event_param_obis_code)):
                if recv_obis == self.all_event_param_obis_code[idx]:
                    self.send_event_val(idx)
                    dbg_buf = "%-25s : Sending Event val by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))
                    break
        else:
            dbg_buf = "%-25s : No obis code matched with recv obis code : %s"%(fun_name, recv_obis)
            self.dbg_log(self.INFORM, dbg_buf)
            return False
            
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def inst_obis_by_grp(self, recv_obis):
        fun_name = "proc_all_scalar_obis_by_grp()"
        dbg_buf = ""

        if(recv_obis == self.inst_scale_obis_code):
            self.proc_inst_scal_obis()
            dbg_buf = "%-25s : Sending Inst scaler obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.inst_val_obis_code):
            self.proc_inst_val_obis()
            dbg_buf = "%-25s : Sending Inst val obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.ls_block_val_obis_code):
            self.proc_block_val_obis()
            dbg_buf = "%-25s : Sending block val obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.ls_scaler_obis_code):
            self.proc_block_scaler_obis()
            dbg_buf = "%-25s : Sending block scaler obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.event_val_obis_code):
            self.proc_event_val_obis()
            dbg_buf = "%-25s : Sending event val obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.event_scaler_obis_code):
            self.proc_event_scaler_obis()
            dbg_buf = "%-25s : Sending event scaler obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.mid_night_val_obis_code):
            self.proc_mid_night_val_obis()
            dbg_buf = "%-25s : Sending Midnight val obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))

        elif(recv_obis == self.mid_night_scaler_obis_code):
            self.proc_mid_night_scaler_obis()
            dbg_buf = "%-25s : Sending Midnight scaler obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))
            
        elif(recv_obis == self.bill_val_obis_code):
            self.proc_bill_val_obis()
            dbg_buf = "%-25s : Sending Billing Val obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))
            
        elif(recv_obis == self.bill_scaler_obis_code):
            self.proc_bill_scaler_obis()
            dbg_buf = "%-25s : Sending Billing scaler obis by group info response, Len : %d"%(fun_name, len(self.send_byte_arr))
        else:
            dbg_buf = "%-25s : No obis code matched with recv obis code : %s"%(fun_name, recv_obis)
            self.dbg_log(self.INFORM, dbg_buf)
            return False
            
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)


    def send_name_plate_resp(self, recv_obis):
        fun_name = "send_name_plate_resp()"

        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)

        cntrl_addr = 0xA000 | len(self.send_byte_arr)&0XFF
        self.send_byte_arr.append((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr.append(cntrl_addr & 0XFF)

        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0

        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)

        if(recv_obis == self.np_meter_ser_num_obis):
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X0C)
            for data in self.meter_ser_num:
                loc_data = hex(ord(data))[2:]
                self.send_byte_arr.append(int(loc_data,16))
            if (len(self.meter_ser_num) < 12):
                for idx in range (len(self.meter_ser_num), 12):
                    self.send_byte_arr.append(0X20)
                    idx = idx + 0

        elif(recv_obis == self.np_meter_manf_obis):
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(len(self.meter_manf_name))
            for data in self.meter_manf_name:
                loc_data = hex(ord(data))[2:]
                self.send_byte_arr.append(int(loc_data,16))

        elif(recv_obis == self.np_meter_fw_obis):
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(len(self.meter_fw_ver))
            for data in self.meter_fw_ver:
                loc_data = hex(ord(data))[2:]
                self.send_byte_arr.append(int(loc_data,16))

        elif(recv_obis == self.np_meter_type_obis):
            self.send_byte_arr.append(0X11)
            self.send_byte_arr.append(self.meter_type)

        elif(recv_obis == self.np_meter_ct_ratio_obis):
            self.send_byte_arr.append(0X17)
            loc_byte_arr = bytearray(struct.pack("f", self.meter_ct_ratio))
            self.send_byte_arr.append(loc_byte_arr[3])
            self.send_byte_arr.append(loc_byte_arr[2])
            self.send_byte_arr.append(loc_byte_arr[1])
            self.send_byte_arr.append(loc_byte_arr[0])

        elif(recv_obis == self.np_meter_pt_ratio_obis):
            self.send_byte_arr.append(0X17)
            loc_byte_arr = bytearray(struct.pack("f", self.meter_pt_ratio))
            self.send_byte_arr.append(loc_byte_arr[3])
            self.send_byte_arr.append(loc_byte_arr[2])
            self.send_byte_arr.append(loc_byte_arr[1])
            self.send_byte_arr.append(loc_byte_arr[0])


        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending Name plate info response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)


    def send_date_time_resp(self):
        fun_name = "send_date_time_resp()"

        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)
       

        cntrl_addr = 0xA000 | (0x1E)
        self.send_byte_arr.append((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr.append(cntrl_addr & 0XFF)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0

        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append(self.ctrl_field)

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X09)
        self.send_byte_arr.append(0X0C)

        curr_time = datetime.now()

        year = curr_time.year
        month = curr_time.month
        day = curr_time.day
        hour = curr_time.hour
        minute = curr_time.minute
        sec = curr_time.second

        self.send_byte_arr.append((year >> 8) & 0XFF)
        self.send_byte_arr.append((year) & 0XFF)
        self.send_byte_arr.append((month) & 0XFF)
        self.send_byte_arr.append((day) & 0XFF)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append((hour) & 0XFF)
        self.send_byte_arr.append((minute) & 0XFF)
        self.send_byte_arr.append((sec) & 0XFF)
        self.send_byte_arr.append(0XFF)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X4A)
        self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending date time response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def send_aare_frame(self, flag):
        fun_name = "send_aare_frame()"
        
        try:
            self.send_byte_arr = bytearray()

            self.send_byte_arr.append(self.START_END_FLAG)

            cntrl_addr = 0xA000 | (0x3A)
            self.send_byte_arr.append((cntrl_addr >> 8)& 0XFF)
            self.send_byte_arr.append(cntrl_addr & 0XFF)
            
            self.send_byte_arr.append(self.dest_addr)
            self.send_byte_arr.append(self.src_addr)

            self.ctrl_field = 0

            self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
            self.ctrl_field = self.ctrl_field | (1 << 4)
            self.ctrl_field = self.ctrl_field | (self.send_seq_num)

            self.send_byte_arr.append(self.ctrl_field)
            
            loc_len = len(self.send_byte_arr)

            cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
            cal_check_sum ^= 0xFFFF
                            
            high_fcs = (cal_check_sum) & 0xFF
            low_fcs = (cal_check_sum >> 8) & 0xFF
            
            self.send_byte_arr.append(high_fcs)
            self.send_byte_arr.append(low_fcs)

            self.send_byte_arr.append(0XE6)
            self.send_byte_arr.append(0XE7)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X61)
            self.send_byte_arr.append(0X29)
            self.send_byte_arr.append(0XA1)
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)
            self.send_byte_arr.append(0X07)
            self.send_byte_arr.append(0X60)
            self.send_byte_arr.append(0X85)
            self.send_byte_arr.append(0X74)
            self.send_byte_arr.append(0X05)
            self.send_byte_arr.append(0X08)
            self.send_byte_arr.append(0X01)
            self.send_byte_arr.append(0X01)
            self.send_byte_arr.append(0XA2)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X01)
            self.send_byte_arr.append(flag)
            self.send_byte_arr.append(0XA3)
            self.send_byte_arr.append(0X05)
            self.send_byte_arr.append(0XA1)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X01)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0XBE)
            self.send_byte_arr.append(0X10)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X0E)
            self.send_byte_arr.append(0X08)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X06)
            self.send_byte_arr.append(0X5F)
            self.send_byte_arr.append(0X1F)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X10)
            self.send_byte_arr.append(0X1C)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X07)

            loc_len = len(self.send_byte_arr)

            cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len],(loc_len-1))
            cal_check_sum ^= 0xFFFF
                            
            high_fcs = (cal_check_sum) & 0xFF
            low_fcs = (cal_check_sum >> 8) & 0xFF
            
            self.send_byte_arr.append(high_fcs)
            self.send_byte_arr.append(low_fcs)

            self.send_byte_arr.append(self.START_END_FLAG)

            dbg_buf = "%-25s : Sending Aarq response, Len : %d"%(fun_name, len(self.send_byte_arr))
            self.dbg_log(self.INFORM, dbg_buf)

            self.print_data(self.send_byte_arr)
            
            self.send_resp(self.send_byte_arr)

        except Exception as e:
            dbg_buf = "%-25s : Sending Aarq Error  : %s"%(fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)

    def send_disc_resp(self):
        fun_name = "send_disc_resp()"

        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)

        cntrl_addr = 0xA000 | (0x1E)
        self.send_byte_arr.append((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr.append(cntrl_addr & 0XFF)
                
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)
        self.send_byte_arr.append(0X73)

        cal_check_sum = 0
        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1: loc_len], loc_len - 1)
        cal_check_sum ^= 0xFFFF
                                
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
            
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)
            
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X12)
        self.send_byte_arr.append(0X05)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X06)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X07)
        self.send_byte_arr.append(0X04)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X08)
        self.send_byte_arr.append(0X04)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)

        cal_check_sum = 0
        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1: loc_len],loc_len - 1)
        cal_check_sum ^= 0xFFFF
                                                
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)
        
        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending Disc frame response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)
        
        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)


    def send_snrm_resp(self):
        fun_name = "send_snrm_resp()"

        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)

        cntrl_addr = 0xA000 | (0x1E)
        self.send_byte_arr.append((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr.append(cntrl_addr & 0XFF)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)
        self.send_byte_arr.append(0X73)

        cal_check_sum = 0
        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1: loc_len], (loc_len - 1))
        cal_check_sum ^= 0xFFFF
                                                
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)
        
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X12)
        self.send_byte_arr.append(0X05)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X06)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X80)
        self.send_byte_arr.append(0X07)
        self.send_byte_arr.append(0X04)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X08)
        self.send_byte_arr.append(0X04)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)

        cal_check_sum = 0
        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1: loc_len], (loc_len - 1))
        cal_check_sum ^= 0xFFFF
                                                
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)
        
        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending Snrm frame response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)
        
        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def pppfcs16(self, fcs, msg, msg_len):
        idx = 0
        
        for idx in range(0, msg_len):
            fcs = (fcs >> 8) ^ (fcstab[(fcs ^ msg[idx] & 0xFF) & 0xFF])
            
        return fcs
    
    def print_data(self, msg):
        frame_len = 16
        fame_cnt = 1
        idx = 0

        dbg_buf = ""

        for data in msg:
            idx = idx + 1
            loc_data = ""
            loc_data = "%02X "%(data)
            dbg_buf = dbg_buf + loc_data
            if (idx == (fame_cnt * frame_len)):
                self.dbg_log(self.INFORM, dbg_buf)
                
                fame_cnt = fame_cnt + 1
                dbg_buf = ""
            
        if(len(dbg_buf)):    
            self.dbg_log(self.INFORM, dbg_buf)


    def proc_billing_value_qry(self, init_bill_val):
        fun_name = "proc_billing_value_qry()"

        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        if init_bill_val == 0:
            self.last_billing_qry = 1
            self.last_ls_qry = 0
            self.last_mid_night_qry = 0
            self.send_byte_arr.append(0X02)
        elif init_bill_val == 1:
            self.send_byte_arr.append(0X01)
            self.last_billing_qry = 0

        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)

        value = self.send_next_bill_cnt
        ba = bytearray(struct.pack("i", value))
        for idx in range (0, 4):
            self.send_byte_arr.append(ba[3 - idx])

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(1)

        #bill_val = meter_max_bill_cnt+1
        #for bill_idx in range(0, self.meter_max_bill_cnt+1):
        if True:
            self.bill_data_list = []
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(len(all_bill_val_obis))

            curr_time = datetime.now()

            year = curr_time.year
            month = curr_time.month
            day =  curr_time.day
            hour = curr_time.hour
            minute =  curr_time.minute
            sec = 0

            for idx in range (0, len(all_bill_val_obis)):
                if idx == 0:
                    if init_bill_val == 0:
                        self.fill_date_time(year, month, day, hour, minute, sec)
                    elif init_bill_val == 1:
                        self.fill_date_time(year, month, 1, 0, 0, 0)

                    dbg_buf = "%-25s : Init_BillVal : %d , Filled Date : %02d_%02d_%04d %02d:%02d" % (fun_name,init_bill_val,day,month,year,hour,minute)
                    self.dbg_log(self.INFORM, dbg_buf)

                    curr_time = datetime.now() - timedelta(init_bill_val)
                    year = curr_time.year
                    month = curr_time.month
                    day =  curr_time.day
                    hour = curr_time.hour
                    minute =  curr_time.minute
                    sec = 0

                elif idx == 1:
                    value = random.uniform(9000, 10000)
                    self.fill_loc_data(value)
                elif idx == 23 or idx == 41:
                    self.fill_date_time(year, month, day, 0, 0, sec)
                elif idx == 25 or idx == 43:
                    self.fill_date_time(year, month, day, 0, 0, sec)   
                elif idx == 27 or idx == 45:
                    self.fill_date_time(year, month, day, 3, 0, sec)
                elif idx == 29 or idx == 47:
                    self.fill_date_time(year, month, day, 6, 0, sec)
                elif idx == 31 or idx == 49:
                    self.fill_date_time(year, month, day, 9, 0, sec)
                elif idx == 33 or idx == 51:
                    self.fill_date_time(year, month, day, 12, 0, sec)
                elif idx == 35 or idx == 53:
                    self.fill_date_time(year, month, day, 15, 0, sec)
                elif idx == 37 or idx == 55:
                    self.fill_date_time(year, month, day, 18, 0, sec)
                elif idx == 39 or idx == 57:
                    self.fill_date_time(year, month, day, 21, 0, sec)
                else:
                    value = random.uniform(20000, 25000)
                    self.fill_loc_data(value)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending  Billing value response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.send_resp(self.send_byte_arr)
        self.print_data(self.send_byte_arr)

        self.update_bill_data_table(str(init_bill_val))
        self.send_next_bill_cnt = self.send_next_bill_cnt + 1
    
    def fill_loc_data(self, value):
        self.bill_data_list.append(float(float(value)/10000))

        self.send_byte_arr.append(0X05)
        ba = bytearray(struct.pack("i", value))
        for idx in range (0, 4):
            self.send_byte_arr.append(ba[3 - idx])

    def fill_date_time(self, year, month, day, hour, minute, sec):
        value = "%04d-%02d-%02d %02d:%02d:%02d"%(year,month,day,hour,minute,sec) 
        self.bill_data_list.append(value)

        self.send_byte_arr.append(0X09)
        self.send_byte_arr.append(0X0C)
        self.send_byte_arr.append((year >> 8) & 0XFF)
        self.send_byte_arr.append((year) & 0XFF)
        self.send_byte_arr.append((month) & 0XFF)
        self.send_byte_arr.append((day) & 0XFF)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append((hour) & 0XFF)
        self.send_byte_arr.append((minute) & 0XFF)
        self.send_byte_arr.append((sec) & 0XFF)
        self.send_byte_arr.append(0XFF)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X4A)
        self.send_byte_arr.append(0X00)

    def proc_bill_scaler_val(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_bill_val_obis)-1)

        for idx in range(1, len(all_bill_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0Xfc)
            self.send_byte_arr.append(0X16)
            self.send_byte_arr.append(0X1f)
            idx = idx + 0

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def proc_bill_val_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_bill_val_obis))

        for idx in range(0, len(all_bill_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_bill_val_obis[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

    def proc_bill_scaler_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_bill_val_obis) - 1)

        for idx in range(1, len(all_bill_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_bill_val_obis[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)


    def send_num_bill_entry_resp(self):
        fun_name = "send_num_bill_entry_resp()"
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X06)

        value = self.meter_max_bill_cnt
        ba = bytearray(struct.pack("i", value*10000))
        for idx in range (0, 4):
            self.send_byte_arr.append(ba[3 - idx])

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending  Num Of Billing entry response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def send_bill_entry_resp(self):
        fun_name = "send_bill_entry_resp()"
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X16)
        self.send_byte_arr.append(0X01)
        

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending  Billing order entry response, Len : %d"%(fun_name, len(self.send_byte_arr))
        self.dbg_log(self.INFORM, dbg_buf)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)
        
    def send_mid_night_val(self, send_init_flag):
        fun_name = "send_mid_night_val()"
        
        self.send_byte_arr = bytearray()
        
        self.send_byte_arr.append(self.START_END_FLAG)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)
        
        if self.max_num_mid_night_data > 2:
            self.send_byte_arr.append(0X02)
        else:
            self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)

        value = self.send_next_blk_cnt
        ba = bytearray(struct.pack("i", value))
        for idx in range (0, 4):
            self.send_byte_arr.append(ba[3 - idx])

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X82)
        self.send_byte_arr.append(0X03)
        self.send_byte_arr.append(0Xbc)

        if self.send_get_next_block_flag == 0:
            self.send_byte_arr.append(0X01)
            self.send_byte_arr.append(self.max_num_mid_night_data)

        ls_loop = self.max_num_mid_night_data
        if ls_loop > 2:
            self.last_mid_night_qry = 1
            self.last_ls_qry = 0
            self.last_billing_qry = 0
            for ls_idx in range(0, 2):
                self.fill_mid_night_data(ls_idx)
                self.max_num_mid_night_data = self.max_num_mid_night_data - 1

        else:
            self.last_mid_night_qry = 0
            dbg_buf = "%-25s : Last resp, Num of days Midnight : %d"%(fun_name, self.max_num_mid_night_data)
            self.dbg_log(self.INFORM, dbg_buf)
            for ls_idx in range(0, ls_loop):
                self.fill_mid_night_data(ls_idx)
                self.max_num_mid_night_data = self.max_num_mid_night_data - 1

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        dbg_buf = "%-25s : Sending  Midnight dayIdx : %d value response, Len : %d"%(
            fun_name,(30 - self.max_num_mid_night_data), len(self.send_byte_arr))

        #print "Sending date info : ",(time.localtime(int(self.mid_night_curr_date)))
        
        self.dbg_log(self.INFORM, dbg_buf)
        
        if send_init_flag != 0:
            self.print_data(self.send_byte_arr)
            self.send_resp(self.send_byte_arr)

        self.send_next_blk_cnt = self.send_next_blk_cnt + 1

    def fill_mid_night_data(self, ls_idx):
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_mid_night_val_obis))

        for idx in range(0, len(all_mid_night_val_obis)):
            if idx == 0:
                self.send_byte_arr.append(0X09)
                self.send_byte_arr.append(0X0C)

                curr_time = time.localtime(int(self.mid_night_curr_date))
                year = curr_time.tm_year
                month = curr_time.tm_mon
                day = curr_time.tm_mday
                hour = curr_time.tm_hour
                minute = curr_time.tm_min
                sec = curr_time.tm_sec

                self.send_byte_arr.append((year >> 8) & 0XFF)
                self.send_byte_arr.append((year) & 0XFF)
                self.send_byte_arr.append((month) & 0XFF)
                self.send_byte_arr.append((day) & 0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append((hour) & 0XFF)
                self.send_byte_arr.append((minute) & 0XFF)
                self.send_byte_arr.append((sec) & 0XFF)
                self.send_byte_arr.append(0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append(0X4A)
                self.send_byte_arr.append(0X00)

            else:
                self.send_byte_arr.append(0X05)
                value = random.randint(20000, 25000)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
        self.mid_night_curr_date = self.mid_night_curr_date + (24*60*60)

    def proc_mid_night_scaler_val_type(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_mid_night_val_obis)-1)

        for idx in range(1, len(all_mid_night_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0Xfc)
            self.send_byte_arr.append(0X16)
            self.send_byte_arr.append(0X1f)
            idx = idx + 0

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def proc_mid_night_scaler_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_mid_night_val_obis) - 1)

        for idx in range(1, len(all_mid_night_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_mid_night_val_obis[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def proc_mid_night_val_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_mid_night_val_obis))

        for idx in range(0, len(all_mid_night_val_obis)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_mid_night_val_obis[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def send_event_val(self, recv_event_idx):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(self.max_num_event)

        event_code = 0
        self.tot_event_data_list = []
        for event_idx in range(0, self.max_num_event):
            self.event_data_list = []
            if recv_event_idx == 0:
                if (event_idx % 2) == 0:
                    event_code = 9
                else:
                    event_code = 10
            elif recv_event_idx == 1:
                if (event_idx % 2) == 0:
                    event_code = 65
                else:
                    event_code = 66
            elif recv_event_idx == 2:
                if (event_idx % 2) == 0:
                    event_code = 101
                else:
                    event_code = 102
            elif recv_event_idx == 3:
                if (event_idx % 2) == 0:
                    event_code = 151
                else:
                    event_code = 152
            elif recv_event_idx == 4:
                if (event_idx % 2) == 0:
                    event_code = 201
                else:
                    event_code = 202
                    
            elif recv_event_idx == 5:
                event_code = 251

            elif recv_event_idx == 6:
                if (event_idx % 2) == 0:
                    event_code = 301
                else:
                    event_code = 302

            self.event_data_list.append(event_code)
            #event_code = event_code*10000
       
            self.send_byte_arr.append(0X02)
            if recv_event_idx==2 or recv_event_idx==3 or recv_event_idx==5 or recv_event_idx==6:
                self.send_byte_arr.append(2)
            else:
                self.send_byte_arr.append(len(all_event_val_obis_det))

            for idx in range(0, len(all_event_val_obis_det)):
                if idx == 0:
                    self.send_byte_arr.append(0X09)
                    self.send_byte_arr.append(0X0C)

                    #current_time1 = time.time()
                    #current_time1 = current_time1 - (event_idx*24*60*60)
                    #curr_time = time.localtime(int(current_time1))
                    year = 2019#curr_time.tm_year Fixed this as per Praveen req.
                    month = 10#curr_time.tm_mon
                    day = 21#curr_time.tm_mday
                    hour = 12#curr_time.tm_hour
                    minute = 0#curr_time.tm_min
                    sec = 0#curr_time.tm_sec

                    self.send_byte_arr.append((year >> 8) & 0XFF)
                    self.send_byte_arr.append((year) & 0XFF)
                    self.send_byte_arr.append((month) & 0XFF)
                    self.send_byte_arr.append((day) & 0XFF)
                    self.send_byte_arr.append(0X01)
                    self.send_byte_arr.append((hour) & 0XFF)
                    self.send_byte_arr.append((minute) & 0XFF)
                    self.send_byte_arr.append((sec) & 0XFF)
                    self.send_byte_arr.append(0XFF)
                    self.send_byte_arr.append(0X01)
                    self.send_byte_arr.append(0X4A)
                    self.send_byte_arr.append(0X00)

                    value = "%04d-%02d-%02d %02d:%02d:%02d"%(year,month,day,hour,minute,sec)
                    self.event_data_list.append(value)

                elif idx == 1 or idx == 2 or idx == 3:
                    if recv_event_idx==2 or recv_event_idx==3 or recv_event_idx==5 or recv_event_idx==6:
                        value = 0
                    else:
                        self.send_byte_arr.append(0X05)
                        value = random.randint(10900,20100)
                        ba = bytearray(struct.pack("i", value))
                        for idx in range (0, 4):
                            self.send_byte_arr.append(ba[3 - idx])

                    self.event_data_list.append(float(float(value)/10000))

                elif idx == 4 or idx == 5 or idx == 6:
                    if recv_event_idx==2 or recv_event_idx==3 or recv_event_idx==5 or recv_event_idx==6:
                        value = 0
                    else:
                        self.send_byte_arr.append(0X05)
                        value = random.randint(630000, 635000)
                        ba = bytearray(struct.pack("i", value))
                        for idx in range (0, 4):
                            self.send_byte_arr.append(ba[3 - idx])
                    
                    self.event_data_list.append(float(float(value)/10000))

                elif idx == 7 or idx == 8 or idx == 9:
                    if recv_event_idx==2 or recv_event_idx==3 or recv_event_idx==5 or recv_event_idx==6:
                        value = 0
                    else:
                        self.send_byte_arr.append(0X05)
                        value = random.randint(10000,10001)
                        ba = bytearray(struct.pack("i", value))
                        for idx in range (0, 4):
                            self.send_byte_arr.append(ba[3 - idx])

                    self.event_data_list.append(float(float(value)/10000))

                elif idx == 10:
                    if recv_event_idx==2 or recv_event_idx==3 or recv_event_idx==5 or recv_event_idx==6:
                        value = 0
                    else:
                        self.send_byte_arr.append(0X05)
                        value = random.randint(200000, 250000)
                        ba = bytearray(struct.pack("i", value))
                        for idx in range (0, 4):
                            self.send_byte_arr.append(ba[3 - idx])

                    self.event_data_list.append(float(float(value)/10000))

                else:
                    self.send_byte_arr.append(0X05)
                    ba = bytearray(struct.pack("i", event_code))
                    for idx in range (0, 4):
                        self.send_byte_arr.append(ba[3 - idx])
            
            self.update_event_val_table()
            self.tot_event_data_list.append(self.event_data_list)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
                    

    def send_num_event(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X06)

        value = self.max_num_event
        ba = bytearray(struct.pack("i", value))
        for idx in range (0, 4):
            self.send_byte_arr.append(ba[3 - idx])

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        

    def send_event_entry_order(self):

        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X16)
        self.send_byte_arr.append(0X01)
        

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

    def proc_event_scaler_val_type(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_event_val_obis_det)-1)

        for idx in range(1, len(all_event_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X0f)
            if idx == (len(all_event_val_obis_det)-1):
                print "--->>>>>",idx
                self.send_byte_arr.append(0)
            else:
                self.send_byte_arr.append(0Xfc)
            self.send_byte_arr.append(0X16)
            self.send_byte_arr.append(0X1f)
            idx = idx + 0

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def proc_event_scaler_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_event_val_obis_det) - 1)

        for idx in range(1, len(all_event_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_event_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        
    def proc_event_val_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_event_val_obis_det))

        for idx in range(0, len(all_event_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_event_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    def send_error_blk_data_resp(self):
        self.send_byte_arr = bytearray()
            
        self.send_byte_arr.append(self.START_END_FLAG)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)
            

        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X01)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        self.print_data(self.send_byte_arr)
        
        self.send_resp(self.send_byte_arr)

    def send_blk_data_resp(self):
        fun_name = "send_blk_data_resp()"
        
        try:
            self.send_byte_arr = bytearray()
            
            self.send_byte_arr.append(self.START_END_FLAG)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(self.dest_addr)
            self.send_byte_arr.append(self.src_addr)

            self.ctrl_field = 0
            self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
            self.ctrl_field = self.ctrl_field | (1 << 4)
            self.ctrl_field = self.ctrl_field | (self.send_seq_num)
            self.send_byte_arr.append((self.ctrl_field & 0XFF))

            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

            self.send_byte_arr.append(0XE6)
            self.send_byte_arr.append(0XE7)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0XC4)
            
            if self.num_ls_block > 4:
                self.send_byte_arr.append(0X02)
            else:
                self.send_byte_arr.append(0X01)
            
            self.send_byte_arr.append(0X81)
            self.send_byte_arr.append(0X00)

            value = self.send_next_blk_cnt
            ba = bytearray(struct.pack("i", value))
            for idx in range (0, 4):
                self.send_byte_arr.append(ba[3 - idx])

            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X82)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0Xbc)

            if self.send_get_next_block_flag == 0:
                self.send_byte_arr.append(0X01)
                if self.num_ls_block > 255:
                    self.send_byte_arr.append(255)
                else:
                    self.send_byte_arr.append(self.num_ls_block)

            ls_loop = self.num_ls_block
            if ls_loop > 4:
                self.last_ls_qry = 1
                self.last_billing_qry = 0
                self.last_mid_night_qry = 0
                for ls_idx in range(0, 4):
                    self.fill_ls_data(ls_idx)
                    self.num_ls_block = self.num_ls_block - 1
                    
            else:
                self.last_ls_qry = 0
                for ls_idx in range(0, ls_loop):
                    self.fill_ls_data(ls_idx)
                    self.num_ls_block = self.num_ls_block - 1
                    

            loc_len = len(self.send_byte_arr)
            cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
            self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
            self.send_byte_arr[2] = (cntrl_addr & 0XFF)

            cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
            cal_check_sum ^= 0xFFFF
            high_fcs = (cal_check_sum) & 0xFF
            low_fcs = (cal_check_sum >> 8) & 0xFF
            self.send_byte_arr[6] = high_fcs
            self.send_byte_arr[7] = low_fcs

            loc_len = len(self.send_byte_arr)
            cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
            cal_check_sum ^= 0xFFFF
            high_fcs = (cal_check_sum) & 0xFF
            low_fcs = (cal_check_sum >> 8) & 0xFF
            self.send_byte_arr.append(high_fcs)
            self.send_byte_arr.append(low_fcs)

            self.send_byte_arr.append(self.START_END_FLAG)

            dbg_buf = "%-25s : Sending  LS blk_dx : %d block data for date time : %02d_%02d_%04d %02d:%02dvalue response, Len : %d"%(fun_name,
                self.num_ls_block, self.st_date_time[2], self.st_date_time[3],
                ((self.st_date_time[0]<<8)|self.st_date_time[1]),
                self.st_date_time[4], self.st_date_time[5], len(self.send_byte_arr))
            
            self.dbg_log(self.INFORM, dbg_buf)

            self.print_data(self.send_byte_arr)
            
            self.send_resp(self.send_byte_arr)

            self.send_next_blk_cnt = self.send_next_blk_cnt + 1

        except Exception as e:
            #print self.num_ls_block, self.send_next_blk_cnt
            dbg_buf = "%-25s : Error in sending block data resp. Error : %s"%(fun_name,e)
            self.dbg_log(self.REPORT, dbg_buf)

    def fill_ls_data(self, ls_idx):
        fun_name = "fill_ls_data()"
        
        self.ls_data_list = []
        dbg_buf = "%-25s : Sending  LS blk_dx : %d block data for date time : %02d_%02d_%04d %02d:%02d"%(
            fun_name,(self.num_ls_block), self.st_date_time[2], self.st_date_time[3],(
                (self.st_date_time[0]<<8)|self.st_date_time[1]),self.st_date_time[4], self.st_date_time[5])

        self.dbg_log(self.INFORM, dbg_buf)

        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_ls_val_obis_det))

        if(self.st_date_time[5] > 59):
            self.st_date_time[4] = self.st_date_time[4] + 1
            self.st_date_time[5] = (self.st_date_time[5] % 60)

            if(self.st_date_time[4] >23):
                self.st_date_time[3] = self.st_date_time[3] + 1
                self.st_date_time[4] = (self.st_date_time[4] % 24)
        
        for idx in range(0, len(all_ls_val_obis_det)):
            if idx == 0:
                self.send_byte_arr.append(0X09)
                self.send_byte_arr.append(0X0C)

                self.send_byte_arr.append(self.st_date_time[0] & 0XFF)
                self.send_byte_arr.append(self.st_date_time[1] & 0XFF)
                self.send_byte_arr.append(self.st_date_time[2] & 0XFF)
                self.send_byte_arr.append(self.st_date_time[3] & 0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append(self.st_date_time[4] & 0XFF)
                self.send_byte_arr.append(self.st_date_time[5] & 0XFF)
                self.send_byte_arr.append(0)
                self.send_byte_arr.append(0XFF)
                self.send_byte_arr.append(0XFF)
                self.send_byte_arr.append(0X4A)
                self.send_byte_arr.append(0X00)
                
                year = (self.st_date_time[0]<<8)|self.st_date_time[1]
                month = self.st_date_time[2]
                day = self.st_date_time[3]
                hour = self.st_date_time[4]
                minute = self.st_date_time[5]
                value = "%04d-%02d-%02d %02d:%02d"%(year,month,day,hour,minute)

                self.ls_data_list.append(value)

            elif idx == 1 or idx == 2 or idx == 3:
                self.send_byte_arr.append(0X05)
                value = random.randint(9900,10100)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                
                self.ls_data_list.append(float(float(value)/10000))

            elif idx == 4 or idx == 5 or idx == 6:
                self.send_byte_arr.append(0X05)

                value = random.randint(630000, 635000)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                
                self.ls_data_list.append(float(float(value)/10000))
                        
            elif idx == 7:
                self.send_byte_arr.append(0X05)

                value = random.randint(499900, 500010)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                
                self.ls_data_list.append(float(float(value)/10000))
                        
            else:
                self.send_byte_arr.append(0X05)

                value = random.randint(20000, 25000)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                
                self.ls_data_list.append(float(float(value)/10000))

        self.st_date_time[5] = self.st_date_time[5] + 15

        self.update_ls_table()
            
    def proc_block_scaler_val_type(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_ls_val_obis_det))

        for idx in range(0, len(all_ls_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0Xfc)
            self.send_byte_arr.append(0X16)
            self.send_byte_arr.append(0X1f)
            idx = idx + 0

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
  
    def proc_block_val_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_ls_val_obis_det))

        for idx in range(0, len(all_ls_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_ls_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        
    def proc_block_scaler_obis(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        
        self.send_byte_arr.append(len(all_ls_val_obis_det) - 1)

        for idx in range(1, len(all_ls_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_ls_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
        
    
    def send_int_period_blk_resp(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X06)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0X03)
        self.send_byte_arr.append(0X84)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

        
    def proc_inst_scal_val_type(self):
        self.send_byte_arr = bytearray()
        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_inst_val_obis_det))

        for idx in range(0, len(all_inst_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0Xfc)
            self.send_byte_arr.append(0X16)
            self.send_byte_arr.append(0X1f)
            idx = idx + 0

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)
            

    def proc_inst_val_type(self):
        self.inst_val_list = []

        self.store_eng_inst_init_val()
        
        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)
        
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X02)
        self.send_byte_arr.append(len(all_inst_val_obis_det))

        for idx in range(0, len(all_inst_val_obis_det)):
            if idx == 0:
                self.send_byte_arr.append(0X09)
                self.send_byte_arr.append(0X0C)

                curr_time = datetime.now()

                year = curr_time.year
                month = curr_time.month
                day = curr_time.day
                hour = curr_time.hour
                minute = curr_time.minute
                sec = curr_time.second

                self.send_byte_arr.append((year >> 8) & 0XFF)
                self.send_byte_arr.append((year) & 0XFF)
                self.send_byte_arr.append((month) & 0XFF)
                self.send_byte_arr.append((day) & 0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append((hour) & 0XFF)
                self.send_byte_arr.append((minute) & 0XFF)
                self.send_byte_arr.append((sec) & 0XFF)
                self.send_byte_arr.append(0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append(0X4A)
                self.send_byte_arr.append(0X00)
                
                value = "%04d-%02d-%02d %02d:%02d:%02d"%(year,month,day,hour,minute,sec)
                self.inst_val_list.append(value)
                
            elif idx == 1 or idx == 2 or idx == 3:
                self.send_byte_arr.append(0X05)
                value = random.randint(9980, 10010)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 4 or idx == 5 or idx == 6:
                self.send_byte_arr.append(0X05)

                value = random.randint(630000, 635000)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 7 or idx == 8 or idx == 9 or idx == 10:
                self.send_byte_arr.append(0X05)

                value = random.randint(9980, 10010)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 11:
                self.send_byte_arr.append(0X05)

                value = random.uniform(499900, 500010)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))
                
            elif idx == 12 or idx == 13 or idx == 14:
                self.send_byte_arr.append(0X05)

                value = random.randint(20000, 25000)
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))
                
            elif idx == 15:
                self.send_byte_arr.append(0X05)
                value = self.meter_max_bill_cnt
                ba = bytearray(struct.pack("i", value*10000))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])

                self.inst_val_list.append(float(float(value)/10000))
                
            elif idx == 16:
                self.send_byte_arr.append(0X09)
                self.send_byte_arr.append(0X0C)

                curr_time = datetime.now()

                year = curr_time.year
                month = curr_time.month
                day = 1
                hour = 0
                minute = 0
                sec = 0

                self.send_byte_arr.append((year >> 8) & 0XFF)
                self.send_byte_arr.append((year) & 0XFF)
                self.send_byte_arr.append((month) & 0XFF)
                self.send_byte_arr.append((day) & 0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append((hour) & 0XFF)
                self.send_byte_arr.append((minute) & 0XFF)
                self.send_byte_arr.append((sec) & 0XFF)
                self.send_byte_arr.append(0XFF)
                self.send_byte_arr.append(0X01)
                self.send_byte_arr.append(0X4A)
                self.send_byte_arr.append(0X00)

                value = "%04d-%02d-%02d %02d:%02d:%02d"%(year,month,day,hour,minute,sec)
                self.inst_val_list.append(value)
            
            elif idx == 17:
                self.send_byte_arr.append(0X05)
                value = self.inst_cumm_kwh + self.inst_cumm_kwh * 0.01
                value = value*10000
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 18:
                self.send_byte_arr.append(0X05)
                value = self.inst_cumm_kvarh_lag + self.inst_cumm_kvarh_lag * 0.01
                value = value*10000
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 19:
                self.send_byte_arr.append(0X05)
                value = self.inst_cumm_kvarh_lead + self.inst_cumm_kvarh_lead * 0.01
                value = value*10000
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 20:
                self.send_byte_arr.append(0X05)
                value = self.inst_cumm_kvarh + self.inst_cumm_kvarh * 0.01
                value = value*10000
                ba = bytearray(struct.pack("i", value))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
                self.inst_val_list.append(float(float(value)/10000))

            elif idx == 21 or idx == 22 or idx == 23 or idx == 24:
                self.send_byte_arr.append(0X05)
                value = 5
                ba = bytearray(struct.pack("i", value*10000))
                for idx in range (0, 4):
                    self.send_byte_arr.append(ba[3 - idx])
                    
                self.inst_val_list.append(value)
            
                
        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

    def proc_inst_val_obis(self):
        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X82)
        self.send_byte_arr.append(0X03)
        self.send_byte_arr.append(0Xf2)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(len(all_inst_val_obis_det))

        for idx in range(0, len(all_inst_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)

            if idx == 0 or idx == 16:
                self.send_byte_arr.append(0X08)
            else:
                self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_inst_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)


    def proc_inst_scal_obis(self):
        self.send_byte_arr = bytearray()

        self.send_byte_arr.append(self.START_END_FLAG)

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(self.dest_addr)
        self.send_byte_arr.append(self.src_addr)

        self.ctrl_field = 0
        self.ctrl_field = self.ctrl_field | (self.recv_seq_num << 5)
        self.ctrl_field = self.ctrl_field | (1 << 4)
        self.ctrl_field = self.ctrl_field | (self.send_seq_num)
        self.send_byte_arr.append((self.ctrl_field & 0XFF))

        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X00)

        self.send_byte_arr.append(0XE6)
        self.send_byte_arr.append(0XE7)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0XC4)

        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(0X81)
        self.send_byte_arr.append(0X00)
        self.send_byte_arr.append(0X01)
        self.send_byte_arr.append(len(all_inst_val_obis_det) - 1)

        for idx in range(1, len(all_inst_val_obis_det)):
            self.send_byte_arr.append(0X02)
            self.send_byte_arr.append(0X04)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X03)

            self.send_byte_arr.append(0X09)
            self.send_byte_arr.append(0X06)

            for obs_idx in range(0, 6):
                data = all_inst_val_obis_det[idx][obs_idx]
                self.send_byte_arr.append(data)

            self.send_byte_arr.append(0X0f)
            self.send_byte_arr.append(0X03)
            self.send_byte_arr.append(0X12)
            self.send_byte_arr.append(0X00)
            self.send_byte_arr.append(0X00)

        loc_len = len(self.send_byte_arr)
        cntrl_addr = 0xA000 | (len(self.send_byte_arr) + 1)&0XFF
        self.send_byte_arr[1] = ((cntrl_addr >> 8)& 0XFF)
        self.send_byte_arr[2] = (cntrl_addr & 0XFF)

        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:6], (6-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr[6] = high_fcs
        self.send_byte_arr[7] = low_fcs

        loc_len = len(self.send_byte_arr)
        cal_check_sum = self.pppfcs16(self.PPPINITFCS16,self.send_byte_arr[1:loc_len], (loc_len-1))
        cal_check_sum ^= 0xFFFF
        high_fcs = (cal_check_sum) & 0xFF
        low_fcs = (cal_check_sum >> 8) & 0xFF
        self.send_byte_arr.append(high_fcs)
        self.send_byte_arr.append(low_fcs)

        self.send_byte_arr.append(self.START_END_FLAG)

    def send_power_on_msg(self):
        fun_name = "send_power_on_msg()"
        temp_text = ""
        json_text = ""
        dbl_qt = '"'

        try:
            json_text = json_text + "{"
            temp_text = "%sSEQ_NUM%s:%s%d%s,"%(dbl_qt,dbl_qt,dbl_qt,2,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sIMEI%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.imei,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sSERIAL_NUM%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.meter_ser_num,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sDATE_TIME%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,time.strftime('%Y-%m-%d %H:%M:%S'),dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sEVENT_MSG%s:%s%s%s"%(dbl_qt,dbl_qt,dbl_qt,"Power On",dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "}"

            self.modem_sock_fd.send(json_text)

            dbg_buf = "%-25s : Send Power On msg resp len : %d" % (fun_name, len(json_text))
            self.dbg_log(self.REPORT, dbg_buf)

            #dbg_buf = json_text
            #self.dbg_log(self.INFORM, dbg_buf)
            
            return True
        
        except Exception as e:
            dbg_buf = "%-25s : Send Power On message error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False
            
    def send_modem_startup_msg(self):
        fun_name = "send_modem_startup_msg()"
        temp_text = ""
        json_text = ""
        dbl_qt = '"'

        try:
            json_text = json_text + "{"
            temp_text = "%sGENERAL%s:"%(dbl_qt,dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "{"
            temp_text = "%sSEQ_NUM%s:%s%d%s,"%(dbl_qt,dbl_qt,dbl_qt,1,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sLOCATION%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.met_loc,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sDESCRIPTION%s:%s%s%s"%(dbl_qt,dbl_qt,dbl_qt,"",dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "},"

            temp_text = "%sMODEM_INFO%s:"%(dbl_qt,dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "{"
            temp_text = "%sIMEI%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.imei,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sIMSI%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"404450959190566",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sCCID%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"89914509009591905669",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sOPERATOR%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"Cms_Operator",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sAT_MDM_FW_VERSION%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"G510_V0M.00.1A_T16",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sAPP_FW_VER%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"SER2GPRS_2G_CMS_1.0_05-Sep-19",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sSIGNAL_STR%s:%s%d%s,"%(dbl_qt,dbl_qt,dbl_qt,30,dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sREG_NETWORK%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,"Airtel",dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sMDM_TIME%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,time.strftime('%Y-%m-%d %H:%M:%S'),dbl_qt)
            json_text = json_text + temp_text
            ppp_ip = socket.gethostbyname(socket.gethostname())
            temp_text = "%sPPP_IP%s:%s%s%s"%(dbl_qt,dbl_qt,dbl_qt,ppp_ip,dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "},"

            temp_text = "%sTRANS_SERVER_INFO%s:"%(dbl_qt,dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "{"
            temp_text = "%sIP%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.config_det["host_ip"],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPORT%s:%s%d%s"%(dbl_qt,dbl_qt,dbl_qt,self.config_det["host_port"],dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "},"

            temp_text = "%sCMD_SERVER_INFO%s:"%(dbl_qt,dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "{"
            temp_text = "%sIP%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.config_det["host_ip"],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPORT%s:%s%d%s"%(dbl_qt,dbl_qt,dbl_qt,self.config_det["modem_port"],dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "}"

            json_text = json_text + "}"

            self.modem_sock_fd.send(json_text)

            dbg_buf = "%-25s : Send HC msg resp len : %d" % (fun_name, len(json_text))
            self.dbg_log(self.INFORM, dbg_buf)

            #dbg_buf = json_text
            #self.dbg_log(self.INFORM, dbg_buf)
            
            return True
        
        except Exception as e:
            dbg_buf = "%-25s : Send hc message error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            return False
    
    def send_period_event_data(self):
        fun_name = "send_period_event_data()"
        temp_text = ""
        json_text = ""
        dbl_qt = '"'

        try:
            json_text = json_text + "{\n"

            temp_text = "%sTOTAL_EVENT%s:%s%d%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.max_num_event,dbl_qt)
            json_text = json_text + temp_text

            temp_text = "%sEvent Det%s:\n"%(dbl_qt,dbl_qt)
            json_text = json_text + temp_text

            json_text = json_text + "[\n"

            for idx in range (0, self.max_num_event):
                self.update_event_val_table()

                json_text = json_text + "{\n"
                temp_text = "%sEVENT_TIME%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][1],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sEVENT_CODE%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][0],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sCURR_1%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][2],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sCURR_2%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][3],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sCURR_3%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][4],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sVOLT_1%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][5],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sVOLT_2%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][6],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sVOLT_3%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][7],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sPF_1%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][8],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sPF_2%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][9],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sPF_3%s:%s%s%s,"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][10],dbl_qt)
                json_text = json_text + temp_text
                temp_text = "%sENG_KWH%s:%s%s%s"%(dbl_qt,dbl_qt,dbl_qt,self.tot_event_data_list[idx][11],dbl_qt)
                json_text = json_text + temp_text

                if idx < self.max_num_event-1:
                    json_text = json_text + "},\n"
                else:
                    json_text = json_text + "}\n"

            json_text = json_text + "]\n"
            json_text = json_text + "}\n"

            #self.modem_sock_fd.send(json_text)
            #print json_text

            dbg_buf = "%-25s : Send Periodic Event data msg len : %d" % (fun_name, len(json_text))
            self.dbg_log(self.INFORM, dbg_buf)

        except Exception as e:
            dbg_buf = "%-25s : Send peripod Event data message error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)

    def send_period_inst_val_msg(self):
        fun_name = "send_period_inst_val_msg()"

        temp_text = ""
        json_text = ""
        dbl_qt = '"'
        
        try:
            json_text = json_text + "{\n"
            temp_text = "%sTIME%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[0],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sCURR_1%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[1],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sCURR_2%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[2],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sCURR_3%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[3],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sVOLT_1%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[4],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sVOLT_2%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[5],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sVOLT_3%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[6],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPF_1%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[7],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPF_2%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[8],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPF_3%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[9],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPF_AVG%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[10],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sFREQ%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[11],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKVA%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[12],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKW%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[13],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKVAR%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[14],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sBILL_CNT%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[15],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sBILL_DATE%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[16],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKWH%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[17],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKVARH_LAG%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[18],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKVARH_LEAD%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[19],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sKVAH%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[20],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPW_FAIL_CNT%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[21],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sTAMP_CNT%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[22],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPC_CNT%s:%s%s%s,\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[23],dbl_qt)
            json_text = json_text + temp_text
            temp_text = "%sPOW_FAIL_CNT%s:%s%s%s\n"%(dbl_qt,dbl_qt,dbl_qt,self.inst_val_list[24],dbl_qt)
            json_text = json_text + temp_text
            json_text = json_text + "}\n"

            #self.modem_sock_fd.send(json_text)
            #print json_text
            
            dbg_buf = "%-25s : Send Periodic inst val msg len : %d" % (fun_name, len(json_text))
            self.dbg_log(self.INFORM, dbg_buf)

            self.update_inst_val_table()
            
        except Exception as e:
            dbg_buf = "%-25s : Send peripod inst val message error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
            
    def update_bill_data_table(self, bill_profile):
        fun_name = "update_bill_data_table()"
        try:
            #print "------------------>>>",len(self.bill_data_list),len(all_bill_val_obis)
            table_name = "BILLING_"+self.imei
            sql_qry = "insert into CMS_METER_SIMULATOR.%s(\
                BILLING_DATE,BILLING_PROFILE,AVG_PF,CUM_ENG_KWH,\
                CUM_ENG_KWH_TZ1,CUM_ENG_KWH_TZ2,CUM_ENG_KWH_TZ3,CUM_ENG_KWH_TZ4,\
                CUM_ENG_KWH_TZ5,CUM_ENG_KWH_TZ6,CUM_ENG_KWH_TZ7,CUM_ENG_KWH_TZ8,\
                CUM_ENG_KVARH_LAG,CUM_ENG_KVARH_LEAD,CUM_ENG_KVAH,\
                CUM_ENG_KVAH_TZ1,CUM_ENG_KVAH_TZ2,CUM_ENG_KVAH_TZ3,CUM_ENG_KVAH_TZ4,\
                CUM_ENG_KVAH_TZ5,CUM_ENG_KVAH_TZ6,CUM_ENG_KVAH_TZ7,CUM_ENG_KVAH_TZ8,\
                ENERGY_ACT_TOTAL,DEMAND_ACT_TOTAL_TS,ENERGY_APP_TOTAL,\
                DEMAND_APP_TOTAL_TS,DEMAND_APP_TOTAL,DEMAND_ACT_TOTAL,\
                DEMAND_ACT_TOTAL_TS_1,ENERGY_APP_TOTAL_1,DEMAND_ACT_TOTAL_TS_2,ENERGY_APP_TOTAL_2,\
                DEMAND_ACT_TOTAL_TS_3,ENERGY_APP_TOTAL_3,DEMAND_ACT_TOTAL_TS_4,ENERGY_APP_TOTAL_4,\
                DEMAND_ACT_TOTAL_TS_5,ENERGY_APP_TOTAL_5,DEMAND_ACT_TOTAL_TS_6,ENERGY_APP_TOTAL_6,\
                DEMAND_ACT_TOTAL_TS_7,ENERGY_APP_TOTAL_7,DEMAND_ACT_TOTAL_TS_8,ENERGY_APP_TOTAL_8,\
                DEMAND_APP_TOTAL_TS_1,DEMAND_APP_TOTAL_1,DEMAND_APP_TOTAL_TS_2,DEMAND_APP_TOTAL_2,\
                DEMAND_APP_TOTAL_TS_3,DEMAND_APP_TOTAL_3,DEMAND_APP_TOTAL_TS_4,DEMAND_APP_TOTAL_4,\
                DEMAND_APP_TOTAL_TS_5,DEMAND_APP_TOTAL_5,DEMAND_APP_TOTAL_TS_6,DEMAND_APP_TOTAL_6,\
                DEMAND_APP_TOTAL_TS_7,DEMAND_APP_TOTAL_7,DEMAND_APP_TOTAL_TS_8,DEMAND_APP_TOTAL_8\
                )values(\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s'\
                    )"%(table_name, self.bill_data_list[0], bill_profile,
                    self.bill_data_list[1],

                    self.bill_data_list[2],
                    self.bill_data_list[3],self.bill_data_list[4],self.bill_data_list[5],self.bill_data_list[6],
                    self.bill_data_list[7],self.bill_data_list[8],self.bill_data_list[9],self.bill_data_list[10],
                    
                    self.bill_data_list[11],self.bill_data_list[12],

                    self.bill_data_list[13],
                    self.bill_data_list[14],self.bill_data_list[15],self.bill_data_list[16],self.bill_data_list[17],
                    self.bill_data_list[18],self.bill_data_list[19],self.bill_data_list[20],self.bill_data_list[21],

                    self.bill_data_list[22],self.bill_data_list[23],self.bill_data_list[40],self.bill_data_list[41],
                    self.bill_data_list[24], self.bill_data_list[24],

                    self.bill_data_list[25],self.bill_data_list[24],self.bill_data_list[43],self.bill_data_list[42],
                    self.bill_data_list[27],self.bill_data_list[26],self.bill_data_list[45],self.bill_data_list[44],
                    self.bill_data_list[29],self.bill_data_list[28],self.bill_data_list[47],self.bill_data_list[46],
                    self.bill_data_list[31],self.bill_data_list[30],self.bill_data_list[49],self.bill_data_list[48],
                    self.bill_data_list[33],self.bill_data_list[32],self.bill_data_list[51],self.bill_data_list[50],
                    self.bill_data_list[35],self.bill_data_list[34],self.bill_data_list[53],self.bill_data_list[52],
                    self.bill_data_list[37],self.bill_data_list[36],self.bill_data_list[55],self.bill_data_list[54],
                    self.bill_data_list[39],self.bill_data_list[38],self.bill_data_list[57],self.bill_data_list[56]
                )

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : billing 1 row updated" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)

        except Exception as e:
            dbg_buf = "%-25s : Update Billing table error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)


    def update_ls_table(self):
        fun_name = "update_ls_table()"
        try:
            table_name = "LOAD_SURVEY_"+self.imei
            sql_qry = "insert into CMS_METER_SIMULATOR.%s(\
                LS_DATE,VOLT_1,VOLT_2,VOLT_3\
                ,CURR_1,CURR_2,CURR_3,FREQ\
                ,ENGY_KWH_EXP,ENGY_KWH_IMP,ENGY_KVARH_LAG\
                ,ENGY_KVARH_LEAD,ENGY_KVAH_EXP,ENGY_KVAH_IMP)values('%s',\
                '%s','%s','%s','%s','%s','%s','%s','%s','%s',\
                '%s','%s','%s','%s'\
                )"%(table_name,
                            self.ls_data_list[0],self.ls_data_list[6],
                            self.ls_data_list[5],self.ls_data_list[5],
                            self.ls_data_list[1],self.ls_data_list[2],
                            self.ls_data_list[3],self.ls_data_list[7],
                            self.ls_data_list[8],self.ls_data_list[9],
                            self.ls_data_list[10],self.ls_data_list[11], 
                            self.ls_data_list[12],self.ls_data_list[13])
            
            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Load survey 1 row updated" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)

        except Exception as e:
            dbg_buf = "%-25s : Update Load survey table error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
    
    def update_event_val_table(self):
        fun_name = "update_event_val_table()"
        try:
            table_name = "EVENT_"+self.imei
            sql_qry = "insert into CMS_METER_SIMULATOR.%s(EVENT_TIME,EVENT_CODE,VOLT_1,VOLT_2,VOLT_3\
                ,CURR_1,CURR_2,CURR_3,PF_1,PF_2,PF_3,ENG_KWH)values('%s','%s','%s','%s','%s','%s','%s',\
                    '%s','%s','%s','%s','%s')"%(table_name,
                self.event_data_list[1],self.event_data_list[0],
                self.event_data_list[5],self.event_data_list[6],
                self.event_data_list[7],self.event_data_list[2],
                self.event_data_list[3],self.event_data_list[4],
                self.event_data_list[8],self.event_data_list[9],
                self.event_data_list[10],self.event_data_list[11]
                )

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()

            dbg_buf = "%-25s : Event 1 row updated" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)

        except Exception as e:
            dbg_buf = "%-25s : Update Event table error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)

    def update_inst_val_table(self):
        fun_name = "update_inst_val_table()"
        try:
            table_name = "INST_"+self.imei
            sql_qry = "insert into CMS_METER_SIMULATOR.%s(\
                        INST_DATE,CURR_1,CURR_2,CURR_3,VOLT_1,VOLT_2,\
                        VOLT_3,PF_1,PF_2,PF_3,AVG_PF,FREQ,APP_POWER,\
                        ACT_POWER,REACT_POWER,CUM_BILLING_COUNT,BILLING_DATE,\
                        CUMENGY_KWH,CUMENGY_KVARH_LAG,CUMENGY_KVARH_LEAD,\
                        CUMENGY_KVAH,NUM_POWER_FAIL,NUM_POWER_FAIL_DUR,\
                        CUM_TAMPER_COUNT,CUM_PGM_COUNT)values('%s',\
                        '%s','%s','%s','%s','%s','%s','%s','%s','%s',\
                        '%s','%s','%s','%s','%s','%s','%s','%s','%s',\
                        '%s','%s','%s','%s','%s','%s'\
                        )"%(table_name,
                            self.inst_val_list[0], self.inst_val_list[1],
                            self.inst_val_list[2], self.inst_val_list[3],
                            self.inst_val_list[4], self.inst_val_list[5],
                            self.inst_val_list[6], self.inst_val_list[7],
                            self.inst_val_list[8], self.inst_val_list[9],
                            self.inst_val_list[10], self.inst_val_list[11],
                            self.inst_val_list[12], self.inst_val_list[13],
                            self.inst_val_list[14], self.inst_val_list[15],
                            self.inst_val_list[16], self.inst_val_list[17],
                            self.inst_val_list[18], self.inst_val_list[19],
                            self.inst_val_list[20], self.inst_val_list[21],
                            self.inst_val_list[22], self.inst_val_list[23],
                            self.inst_val_list[24])

            self.g_curs.execute(sql_qry)
            self.g_conn.commit()
            dbg_buf = "%-25s : Inst 1 row updated" % (fun_name)
            self.dbg_log(self.INFORM, dbg_buf)
                
        except Exception as e:
            dbg_buf = "%-25s : Update inst table error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)

    def store_eng_inst_init_val(self):
        fun_name = "store_eng_inst_init_val()"
        try:
            self.inst_cumm_kwh = 1
            self.inst_cumm_kvarh_lag = 1
            self.inst_cumm_kvarh_lead = 1
            self.inst_cumm_kvarh = 1
                
            table_name = "INST_"+self.imei
            sql_qry = "SELECT CUMENGY_KWH, CUMENGY_KVARH_LAG, CUMENGY_KVARH_LEAD,\
                        CUMENGY_KVAH  FROM CMS_METER_SIMULATOR.%s ORDER BY UPDATE_TIME DESC"%(table_name)
            
            self.g_curs.execute(sql_qry)
            myresult = self.g_curs.fetchall()
            if len(myresult):
                self.inst_cumm_kwh = myresult[0][0]
                self.inst_cumm_kvarh_lag = myresult[0][1]
                self.inst_cumm_kvarh_lead = myresult[0][2]
                self.inst_cumm_kvarh = myresult[0][3]
        
        except Exception as e:
            dbg_buf = "%-25s : Fetch init value table error : %s" % (fun_name, e)
            self.dbg_log(self.REPORT, dbg_buf)
        return 

#   -----------------------------------------------------------------
try:
    if len(sys.argv) <3:
        print "Please use imei and sernum "
        sys.exit(0)

    #print len(sys.argv)
    imei = sys.argv[1]
    ser_num = int(sys.argv[2])

    #print imei,ser_num
    main_class(imei, ser_num)
        
except (KeyboardInterrupt, SystemExit):
    dbg_buf = "Keyboard interrupt Returning from system"
    print dbg_buf
    time.sleep(2)
    sys.exit(0)

#   ----------------End Of File----------------------------------------------
