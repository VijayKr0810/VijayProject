import sys
import os
import time
import ctypes
import struct
import serial
import threading
import thread

from ctypes import *
from datetime import datetime
from struct import *

##############################################################################
CONFIG_FILE = "config.cfg"
LOG_FILE_PATH = "io_simulator.log"
MAX_FILE_SIZE = 500*1024
dbg_msg = ['[Inform]', '[Warning]', '[Severe]', '[Fatal]', '[Report]']

file_ptr = None

INFORM = dbg_msg[0]
WARNING = dbg_msg[1]
SEVERE = dbg_msg[2]
FATAL = dbg_msg[3]
REPORT = dbg_msg[4]

ser_fd = None
thread_live_flag = True

MAX_NUM_CARDS = 32
MAX_NO_OF_INPUT = 16
MAX_ANALOG_INPUTS = 8
PING_COMMAND = 0x01
GET_CARD_INFO = 0x02
GET_CONFIG = 0x03
GET_DATA = 0x04
GET_TIMESTAMP = 0x05
SET_CONFIG = 0x07
SET_OUTPUT_COMMAND = 0x08
SET_TIMESTAMP = 0x09

PING_COMMAND_SUCCESS = 0x0A
POLLED_DATA = 0x0B
DEFAULT_DATA = 0x0C
NO_DATA = 0x0D
SET_CONFIG_SUCCESS = 0x0E
SET_CONFIG_FAILED = 0x0F
SET_OUTPUT_COMMAND_SUCCESS = 0x10
SET_OUTPUT_COMMAND_FAILED = 0x11
SET_TIMESTAMP_SUCCESS = 0x12
SET_TIMESTAMP_FAILED = 0x13
GET_TIMESTAMP_SUCCESS = 0x14
GET_TIMESTAMP_FAILED = 0x15

CMS_IO_DI4 = 0x01
CMS_IO_DI8 = 0x02
CMS_IO_DI16 = 0x03
CMS_IO_DO4 = 0x04
CMS_IO_DO8 = 0x05
CMS_IO_DO16 = 0x06
CMS_IO_AI4 = 0x07
CMS_IO_AI8 = 0x08
CMS_IO_AI16 = 0x09

info_card_list = []

analog_curr_val_list = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7]
ai_volt_val_list = [220.0, 220.1, 220.2, 220.3, 220.4, 220.5, 220.6, 220.7]

crc_table = [
            0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
            0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
            0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
            0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
            0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
            0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
            0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
            0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
            0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
            0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
            0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
            0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
            0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
            0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
            0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
            0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
            0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
            0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
            0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
            0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
            0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
            0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
            0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
            0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
            0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
            0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
            0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
            0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
            0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
            0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
            0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
            0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
        ]
##############################################################################


class time_str(Structure):
    _pack_ = 1
    _fields_ = [
                ("sec", c_ubyte),
                ("min", c_ubyte),
                ("hour", c_ubyte),
                ("day", c_ubyte),
                ("month", c_uint16),
                ("year", c_ubyte)
            ]


class di_val(Structure):
    _pack_ = 1
    _fields_ = [
                ("seq_num", c_ubyte),
                ("input_id", c_ubyte),
                ("input_state", c_ubyte),
                ("time_str", time_str)
            ]


class input_card_get_data_response(Structure):
    _pack_ = 1
    _fields_ = [
                ("card_id", c_ubyte),
                ("command_id", c_ubyte),
                ("resp_type", c_ubyte),
                ("di_val", di_val*MAX_NO_OF_INPUT),
                ("curr_status", c_uint16),
                ("crc", c_uint16)
            ]


class card_command_msg(Structure):
    _pack_ = 1
    _fields_ = [
                ("card_id", c_ubyte),
                ("command_id", c_ubyte),
                ("command_type", c_ubyte),
                ("crc", c_uint16)
            ]


class card_info(Structure):
    _pack_ = 1
    _fields_ = [
        ("card_id", c_ubyte),
        ("card_type", c_ubyte),
        ("serail_num", c_char*64)
        ]


class card_info_resp_msg(Structure):
    _pack_ = 1
    _fields_ = [
        ("card_id", c_ubyte),
        ("command_id", c_ubyte),
        ("resp_type", c_ubyte),
        ("card_info_t", card_info),
        ("crc", c_uint16)
        ]


class ai_data(Structure):
    _pack_ = 1
    _fields_ = [
                ("card_id", c_ubyte),
                ("channel_id", c_ubyte),
                ("ai_data", c_float),
                ("time_str", time_str)
            ]


class analog_card_get_data_response(Structure):
    _pack_ = 1
    _fields_ = [
                ("card_id", c_ubyte),
                ("command_id", c_ubyte),
                ("resp_type", c_ubyte),
                ("ai_data", ai_data*8),
                ("crc", c_uint16)
            ]


class get_time_stamp_resp(Structure):
    _pack_ = 1
    _fields_ = [
                ("card_id", c_ubyte),
                ("command_id", c_ubyte),
                ("resp_type", c_ubyte),
                ("time_str", time_str),
                ("crc", c_uint16)
            ]

card_command_msg_t = card_command_msg()
card_info_resp_msg_t = card_info_resp_msg()
di_data_resp = input_card_get_data_response()
ai_data_resp = analog_card_get_data_response()
get_time_stamp = get_time_stamp_resp()

##############################################################################


def write_into_file(dbg_buff):
    global file_ptr

    if not file_ptr:
        file_ptr = open(LOG_FILE_PATH, 'w')
        file_ptr . write(dbg_buff)
    else:
        file_ptr = open(LOG_FILE_PATH, 'a')
        file_ptr . write(dbg_buff)

    statinfo = os.stat(LOG_FILE_PATH)
    if(statinfo . st_size > MAX_FILE_SIZE):
        file_ptr . close()
        os.remove(LOG_FILE_PATH)
        file_ptr = open(LOG_FILE_PATH, 'w')


def dbg_log(mode, dbg_str):
    dbg_buff = '%s : %s : %s\n' % (
                (datetime.now().strftime('%d_%b_%Y %H:%M:%S'), mode, dbg_str))
    print (dbg_buff)

    write_into_file(dbg_buff)

##############################################################################
def print_data(msg1, msg_len):
    frame_cnt = 1
    frame_len = 16

    msg=bytearray()
    for str in msg1:
        msg.append(str)

    data = ""
    dbg_data = ""

    for idx in range(0, msg_len):
        data = "%02X " % (msg[idx])
        #data = hex(msg[idx])[2:]
        dbg_data = dbg_data + data
        if (((frame_cnt * frame_len-1) == idx) and (idx > 0)):
            dbg_log(INFORM, dbg_data)
            dbg_data = ""
            frame_cnt += 1

    dbg_log(INFORM, dbg_data)


def serial_init(com_port):
    fun_name = "serial_init()"
    global ser_fd

    try:
        ser_fd = serial.Serial()
        ser_fd.port = com_port
        ser_fd.baudrate = 115200
        ser_fd.timeout = 0.01

        ser_fd.open()
        if(ser_fd.is_open):
            dbg_buff = "%-25s : serial Init done for %s" % (fun_name, com_port)
            dbg_log(INFORM, dbg_buff)
            return True
        else:
            return False

    except Exception as e:
        dbg_buff = "%-25s : serial Init failed : error : %s" % (fun_name, e)
        dbg_log(REPORT, dbg_buff)
        return False


def read_ser_port():
    fun_name = "read_ser_port()"

    recv_data = ""
    try:
        recv_data = ser_fd.read(256)

        if(len(recv_data)):
            dbg_buff = "%-25s : #read bytes : %d" % (fun_name, len(recv_data))
            dbg_log(INFORM, dbg_buff)
            return len(recv_data), recv_data

        else:
            return len(recv_data), recv_data

    except Exception as e:
        dbg_buff = "%-25s : read serial failed : error : %s" % (fun_name, e)
        dbg_log(REPORT, dbg_buff)
        return False, recv_data


def write_ser_port(msg):
    fun_name = "write_ser_port()"

    try:
        ser_fd.write(msg)
        dbg_buff = "%-25s : #write num bytes : %d" % (fun_name, len(msg))
        dbg_log(INFORM, dbg_buff)
        print_data(msg,len(msg))
        return True

    except Exception as e:
        dbg_buff = "%-25s : write serial failed : error : %s" % (fun_name, e)
        dbg_log(REPORT, dbg_buff)
        return False

##############################################################################


def crc_data(data):
    crc = 0

    crc_high = 0xFF
    crc_low = 0xFF

    for w in data:
        index = crc_low ^ (w)
        crc_val = crc_table[index]
        crc_temp = crc_val/256
        crc_val_low = crc_val - (crc_temp*256)
        crc_low = crc_val_low ^ crc_high
        crc_high = crc_temp
        crc = crc_high*256 + crc_low

    return crc


def validate_resp(res_byte, crc_len):
    fun_name = "validate_resp()"

    recv_crc = res_byte[crc_len-1] & 0xFF
    recv_crc = recv_crc << 8
    recv_crc = recv_crc | (res_byte[crc_len-2] & 0xFF)

    cal_str = []
    for idx in range(0, len(res_byte)-2):
        cal_str.append(res_byte[idx])

    cal_crc = crc_data(cal_str)

    if(cal_crc == recv_crc):
        return True
    else:
        dbg_buff = "%-25s : invalid crc : recv_crc : %X, cal_crc : %X" % \
                                (fun_name, recv_crc, cal_crc)
        dbg_log(INFORM, dbg_buff)
        return False


def resp_for_cmd(card_id, command_id, command_type):
    fun_name = "resp_for_cmd()"
    global info_card_list

    try:
        if((command_type == GET_CARD_INFO) or (command_type == PING_COMMAND)):

            info_card_list[card_id].command_id = command_id

            crc_str = ""
            crc_str = string_at(addressof(info_card_list[card_id]),
                                sizeof(info_card_list[card_id])-2)

            cal_str = []
            cal_str1 = bytearray()

            for data in crc_str:
                cal_str1.append(data)

            for idx in range(0, len(cal_str1)):
                cal_str.append(cal_str1[idx])

            cal_crc = crc_data(cal_str)

            info_card_list[card_id].crc = cal_crc
            #print_data(info_card_list[card_id], sizeof(info_card_list[card_id]))
            
            send_str = ""
            send_str = string_at(addressof(info_card_list[card_id]),
                                 sizeof(info_card_list[card_id]))
            write_ser_port(send_str)

        elif(command_type == GET_TIMESTAMP):
            get_time_stamp.card_id = card_id
            get_time_stamp.command_id = command_id
            get_time_stamp.resp_type = GET_TIMESTAMP_SUCCESS

            time_now = datetime.now()
            get_time_stamp.time_str.sec = time_now.second
            get_time_stamp.time_str.min = time_now.minute
            get_time_stamp.time_str.hour = time_now.hour
            get_time_stamp.time_str.day = time_now.day
            get_time_stamp.time_str.month = time_now.month
            get_time_stamp.time_str.year = time_now.year

            crc_str = ""
            crc_str = string_at(addressof(get_time_stamp),
                                sizeof(get_time_stamp)-2)

            cal_str = []
            cal_str1 = bytearray()

            for data in crc_str:
                cal_str1.append(data)

            for idx in range(0, len(cal_str1)):
                cal_str.append(cal_str1[idx])

            cal_crc = crc_data(cal_str)

            di_data_resp.crc = cal_crc

            send_str = ""
            send_str = string_at(addressof(get_time_stamp),
                                 sizeof(get_time_stamp))
            write_ser_port(send_str)

        elif(command_type == GET_DATA):
            card_type = info_card_list[card_id].card_info_t.card_type

            if(card_type == CMS_IO_DI16):
                dbg_buff = "%-25s : input card id : %d sent get data resp" % \
                           (fun_name, card_id)
                dbg_log(INFORM, dbg_buff)

                di_data_resp.card_id = card_id
                di_data_resp.command_id = command_id
                di_data_resp.resp_type = POLLED_DATA

                for idx in range(0, MAX_NO_OF_INPUT):
                    di_data_resp.di_val[idx].seq_num = idx
                    di_data_resp.di_val[idx].input_id = idx+1
                    di_data_resp.di_val[idx].input_state = 1

                    time_now = datetime.now()
                    di_data_resp.di_val[idx].time_str.sec = time_now.second
                    di_data_resp.di_val[idx].time_str.min = time_now.minute
                    di_data_resp.di_val[idx].time_str.hour = time_now.hour
                    di_data_resp.di_val[idx].time_str.day = time_now.day
                    di_data_resp.di_val[idx].time_str.month = time_now.month
                    di_data_resp.di_val[idx].time_str.year = time_now.year

                di_data_resp.curr_status = 0xFFFF
                crc_str = ""
                crc_str = string_at(addressof(di_data_resp),
                                    sizeof(di_data_resp)-2)

                cal_str = []
                cal_str1 = bytearray()

                for data in crc_str:
                    cal_str1.append(data)

                for idx in range(0, len(cal_str1)):
                    cal_str.append(cal_str1[idx])

                cal_crc = crc_data(cal_str)

                di_data_resp.crc = cal_crc

                send_str = ""
                send_str = string_at(addressof(di_data_resp),
                                     sizeof(di_data_resp))
                write_ser_port(send_str)

            elif(card_type == CMS_IO_AI8):
                dbg_buff = "%-25s : Analog card :%d sent get data resp " % \
                           (fun_name, card_id)
                dbg_log(INFORM, dbg_buff)

                ai_data_resp.card_id = card_id
                ai_data_resp.command_id = command_id
                ai_data_resp.resp_type = POLLED_DATA

                for idx in range(0, MAX_ANALOG_INPUTS):
                    ai_data_resp.ai_data[idx].card_id = card_id
                    ai_data_resp.ai_data[idx].channel_id = idx+1
                    ai_data_resp.ai_data[idx].ai_data = ai_volt_val_list[idx]

                    time_now = datetime.now()
                    ai_data_resp.ai_data[idx].time_str.sec = time_now.second
                    ai_data_resp.ai_data[idx].time_str.min = time_now.minute
                    ai_data_resp.ai_data[idx].time_str.hour = time_now.hour
                    ai_data_resp.ai_data[idx].time_str.day = time_now.day
                    ai_data_resp.ai_data[idx].time_str.month = time_now.month
                    ai_data_resp.ai_data[idx].time_str.year = time_now.year

                crc_str = ""
                crc_str = string_at(addressof(ai_data_resp),
                                    sizeof(ai_data_resp)-2)

                cal_str = []
                cal_str1 = bytearray()

                for data in crc_str:
                    cal_str1.append(data)

                for idx in range(0, len(cal_str1)):
                    cal_str.append(cal_str1[idx])

                cal_crc = crc_data(cal_str)

                di_data_resp.crc = cal_crc

                send_str = ""
                send_str = string_at(addressof(ai_data_resp),
                                     sizeof(ai_data_resp))

                write_ser_port(send_str)

        return True

    except Exception as e:
        dbg_buff = "%-25s : error in response command : error : %s" % \
                   (fun_name, e)
        dbg_log(REPORT, dbg_buff)
        return False


def proc_command(ser_buff, ret):
    fun_name = "proc_command()"
    card_id, command_id, command_type = 0, 0, 0

    try:

        hex_str = ((hex(int(ser_buff.encode('hex'), 16)))[2:])[:-1]
        if((len(hex_str) % 2) != 0):
            hex_str = "0" + hex_str

        res_byte = bytearray()
        for idx in range(0, len(hex_str), 2):
            data = int(hex_str[idx], 16)
            data = data << 4
            data = data | int(hex_str[idx+1], 16)
            res_byte.append(data)

        if(len(res_byte) < 5):
            dbg_buff = "%-25s : byte should be min 5 byte" % (fun_name)
            dbg_log(REPORT, dbg_buff)
            return 1

        print_data(res_byte, len(res_byte))

        ret_val_resp = validate_resp(res_byte, len(res_byte))
        if not ret_val_resp:
            dbg_buff = "%-25s : crc mismatched" % (fun_name)
            dbg_log(REPORT, dbg_buff)
            return 1

        card_id = res_byte[0] & 0xFF
        command_id = res_byte[1] & 0xFF
        command_type = res_byte[2] & 0xFF

        dbg_buff = "%-25s : CardId : %d,command_id : %d,command_type : %d" % \
                   (fun_name, card_id, command_id, command_type)
        dbg_log(INFORM, dbg_buff)

        if((command_type == GET_CARD_INFO) or (command_type == GET_DATA)):

            ctypes.memmove(addressof(card_command_msg_t), ser_buff,
                           sizeof(card_command_msg_t))

        else:
            dbg_buff = "%-25s : Invalid command, No need to procced" % \
                       (fun_name)
            dbg_log(REPORT, dbg_buff)
            return 1

        resp_for_cmd_ret = resp_for_cmd(card_id, command_id, command_type)
        if not resp_for_cmd_ret:
            return 1

        return True

    except Exception as e:
        dbg_buff = "%-25s : error in proc command , error : %s" % \
                   (fun_name, e)
        dbg_log(REPORT, dbg_buff)
        return False


def proc_read_ser():
    fun_name = "proc_read_ser()"
    global thread_live_flag

    while True:
        try:
            if(thread_live_flag):
                ret, ser_buff = read_ser_port()
                if not ret:
                    continue
                else:
                    if not proc_command(ser_buff, ret):
                        break
            else:
                break

        except Exception as e:
            dbg_buff = "%-25s : proc_read_ser thread : error : %s" % \
                       (fun_name, e)
            dbg_log(REPORT, dbg_buff)

            if(ser_fd.is_open):
                ser_fd.close()
                dbg_buff = "%-25s : closing serial fd " % (fun_name)
                dbg_log(REPORT, dbg_buff)
                thread_live_flag = False

            break

    dbg_buff = "%-25s : Error in main recv thread , need to stop" % \
               (fun_name)
    dbg_log(INFORM, dbg_buff)
    thread_live_flag = False

    return

##############################################################################


def fill_def_card():
    fun_name = "fill_def_card()"
    global info_card_list

    input_card, output_card, analog_card = 0, 0, 0

    info_card_list.append("")

    for card_id in range(1, 33):

        card_info_resp_msg_t = card_info_resp_msg()
        card_info_resp_msg_t.card_id = card_id
        card_info_resp_msg_t.command_id = 0
        card_info_resp_msg_t.resp_type = DEFAULT_DATA
        card_info_resp_msg_t.card_info_t.card_id = card_id

        if (card_id <= 20):
            input_card += 1
            card_info_resp_msg_t.card_info_t.card_type = CMS_IO_DI16
            serial_num = "0619-%04d-InputCard" % (input_card)
            card_info_resp_msg_t.card_info_t.serail_num = serial_num

        else:
            analog_card += 1
            card_info_resp_msg_t.card_info_t.card_type = CMS_IO_AI8
            serial_num = "0619-%04d-AnalogCard" % (analog_card)
            card_info_resp_msg_t.card_info_t.serail_num = serial_num

        info_card_list.append(card_info_resp_msg_t)

        """
            elif((card_id<=25) and (card_id>15)):
            output_card+=1
            card_info_resp_msg_t.card_info_t.card_type=CMS_IO_DO8
            serial_num="0619-%04d-OutputCard"%(output_card)
            card_info_resp_msg_t.card_info_t.serail_num=serial_num
            #print "output card : ",output_card
            #info_card_list.append(card_info_resp_msg_t)
        """
    return True

##############################################################################


try:
    fun_name = "main()"

    num_cmd=len(sys.argv)

    if (num_cmd < 2):
        dbg_buff = "%-25s : Usage! please provide com port" % (fun_name)
        #dbg_log(REPORT, dbg_buff)
        print dbg_buff
        sys.exit(0)

    com_port = sys.argv[1:]
    print "Init for comport : ",com_port[0]
    
    if not serial_init(com_port[0]):
        dbg_buff = "%-25s : serial init failed" % (fun_name)
        dbg_log(REPORT, dbg_buff)
        sys.exit(0)

    if not fill_def_card():
        dbg_buff = "%-25s : fill def card  failed" % (fun_name)
        dbg_log(REPORT, dbg_buff)
        sys.exit(0)

    ser_read_thred = threading.Thread(target=proc_read_ser)
    ser_read_thred.start()

    while thread_live_flag:
        pass

    if(ser_fd.is_open):
        ser_fd.close()
        dbg_buff = "%-25s : closing serial fd " % (fun_name)
        dbg_log(REPORT, dbg_buff)

    time.sleep(5)
    dbg_buff = "%-25s : exiting from main " % (fun_name)
    dbg_log(REPORT, dbg_buff)


except (KeyboardInterrupt, SystemExit):
    dbg_buff = "%-25s : Keyboard interrupt" % (fun_name)
    dbg_log(REPORT, dbg_buff)

    if(ser_fd.is_open):
        ser_fd.close()
        dbg_buff = "%-25s : closing serial fd " % (fun_name)
        dbg_log(REPORT, dbg_buff)

    thread_live_flag = False
    time.sleep(5)

    dbg_buff = "%-25s : exiting from main " % (fun_name)
    dbg_log(REPORT, dbg_buff)

    sys.exit(0)

#                                               End of File
##############################################################################
