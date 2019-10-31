"""from flask import Flask
app = Flask(__name__)

@app.route("/")

def hello():
    return "Hello World!"
"""

def print_data(msg, msg_len):
    frame_cnt = 1
    frame_len = 16

    data = ""
    print_data = ""

    for idx in range(0, msg_len):
        data = str(msg[idx])
        print_data = print_data + data + " "
        if (((frame_cnt * frame_len-1) == idx) and (idx > 0)):
            print print_data
            print_data = ""

    print (print_data)
    print len(print_data)


data=bytearray()
data.append(0x01)
data.append(0x01)
data.append(0x01)
data.append(0x01)
data.append(0x01)
data.append(0x01)
data.append(0x01)
data.append(0x01)

data = "123456789"
print_data(data,len(data))
