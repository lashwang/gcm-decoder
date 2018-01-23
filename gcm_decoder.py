
import os
import csv
import glob
import struct 
import binascii
from gcm_data_format_pb2 import *

CSV_ROW_DELIMITER = ","

"""
Porting from com.google.android.gsf.gtalkservice.proto.ProtoBufStreamParserImpl.parse()
"""

Mobile_ProtoBuf_Stream_Configuration = {0:HeartbeatPing,
                                        1:HeartbeatAck,
                                        2:LoginRequest,
                                        3:LoginResponse,
                                     #   6:PresenceStanza,
                                        7:IqStanza,
                                        8:DataMessageStanza,
                                      #  9:BatchPresenceStanza,
                                      #  13:BindAccountRequest, 
                                      #  14:BindAccountResponse,
                                        }

class ChannelDecoder(object):
    
    def __init__(self):
        self._buf = []
        self._position = 0
        self._is_first_packet = True
        
    
    def decode(self, data):
        total_msg = []
        if data is None or len(data) <1:
            print "The input data is empty"
            return total_msg
        #
        self._buf.extend(data)
        if self._is_first_packet:
            self._is_first_packet = False
            version = self._read_byte_as_int()
            #
            msg = {'tag':-1,
                   'length':1,
                   'content': "%d (XMPPVersion=%d)\n" % (version, version) ,
                   'type': 'OpenStream'
                    }
            total_msg.append(msg)            
        #
        msg_list = self._parse()
        if len(msg_list) > 0:
            total_msg.extend(msg_list)
            self._reset_position()
        #
        return total_msg

    def _parse(self):
        msg_list = []
        while True:
            start_pos = self._position;
            (tag, length, payload) = self._parse_header()
            if tag is None:
                break
            #
#             payload_str = binascii.hexlify(bytearray(payload))
#             print "Get tag:%d, length:%d, raw data:\n%s" \
#                % (tag, length, payload_str)
            
            msg = {'tag':tag, 'length':length}
            protobuf_cls = Mobile_ProtoBuf_Stream_Configuration.get(tag)
            if not protobuf_cls:
                content = binascii.hexlify(payload)
                content_type = 'NA'
                raise Exception("Not supported msg, tag:%d" % tag)
            else:
                protobuf_obj = protobuf_cls()
                try:
                    protobuf_obj.ParseFromString(str(bytearray(payload)))
                    content = protobuf_obj
                    content_type = protobuf_obj.__class__.__name__
                except RuntimeError as error:
                    print "data:\n", payload
                    self._position = start_pos
            #
            msg['type'] = content_type
            msg['content'] = content
            msg_list.append(msg)
        #
        return msg_list
    
    def _parse_header(self):
        start_pos = self._position
        tag = self._read_byte_as_int()
        if tag is None:
            return (None, 0, None)
        #
        length = 0
        read_bytes = 0
        while True :
            if read_bytes > 5:
                raise Exception("Attempting to read more than 5 bytes of length.  Should not happen.")  
            #
            current_byte = self._read_byte_as_int()     
            if current_byte is None:
                self._position = start_pos
                return (None, 0, None)
            #       
            new_byte_posistion = 7 * read_bytes
            length |= (current_byte & 0x7F) << new_byte_posistion
            read_bytes +=1
            if (current_byte & 0x80) == 0:
                break
        #
        payload = []
        if length > 0:
            payload = self._read_bytes(length)
            if payload is None:
                print "Can't read enough data when parse tag:%d, expected len:%d" % (tag, length)
                self._position = start_pos
                return (None, 0, None)
        #
        return (tag, length, payload)
        
    def _read_bytes(self, length):
        start = self._position
        end = start + length
        if end > len(self._buf):
            return None
        #
        data = self._buf[start:end]
        self._position = end
        return data    
    
    def _read_byte_as_int(self):
        if self._position >= len(self._buf):
            return None
        raw_data = self._buf[self._position]
        self._position +=1
        #
        #Prior to Python 3.0, reading a file, even a binary file, 
        #returns a string. What you're calling a "bytes" object is really a string.
        #We need unpack it to int 
        b_data = struct.unpack('B', raw_data) # decode 
        return b_data[0]     
        
    def _reset_position(self):
        if self._position < len(self._buf):
            self._buf = self._buf[self._position:]
        else:
            self._buf = []
        #
        self._position = 0



class GCMDecoder(object):
    
    def __init__(self):
        pass
    
    def decode(self, path):
        print "decode path:%s" % path
        if not os.path.exists(path):
            print "Not found path:%s" % path
            return
        #
        if os.path.isdir(path):
            files = glob.glob(os.path.join(path, "*.log"))
            for one_file in files:
                self.decode_file(one_file)
        else:
            self.decode_file(path)
                
                
    def decode_file(self, input_file):
        print "decode file:%s" % input_file
        #
        items = input_file.split(".")
        items[-1] = "info"
        infofile = ".".join(items)
        if not os.path.exists(infofile):
            print "Can't find info file:%s" % infofile 
            return
        #
        data_infos = GCMDecoder.load_csv(infofile)
        with open(input_file, "rb") as file_obj:
            dump_data = file_obj.read()
        #
        items[-1] = "txt"
        output_file = ".".join(items)
        #
        total_msg = []
        downstream_decoder = ChannelDecoder() #From Server
        upstream_decoder = ChannelDecoder() #From app
        pre_start = 0
        pre_length = 0
        connect = 0
        for info in data_infos:
            error = False
            start = int(info[0])
            length = int(info[1])
            if length == 1:
                pre_start = start
                pre_length = length
                connect = 1
                continue
            if connect:
                start = pre_start
                length += pre_length
                connect = 0

            msg_data = dump_data[start:start+length]
            #
            # if direction == 1:
            #    msg_list = downstream_decoder.decode(msg_data)
            #
            # else:
            # msg_list = upstream_decoder.decode(msg_data)
            #
            try:
                msg_list = upstream_decoder.decode(msg_data)
                direction = 0
                error = False
            except BaseException:
                print "get upstream_decoder error"
                error = True

            if error:
                try:
                    msg_list = downstream_decoder.decode(msg_data)
                    direction = 1
                    error = False
                except BaseException:
                    print "get downstream_decoder error"
                    error = True

            #print "msg_list:\n", len(msg_list) #TypeError: 'int' object is not callable??
            for msg in msg_list:
                msg['timestamp'] = 0
                msg['direction'] = direction
                total_msg.append(msg)
        #
        self.output_msgs(total_msg, output_file)

    @staticmethod
    def output_msgs(msg_list, output_file):
        with open(output_file, "wb") as file_obj:
            for msg in msg_list:  
                header = "%s %s %s \n%s \r\n" % (msg['timestamp'], msg['type']
                                             , "====>" if msg['direction'] == 0 else "<===="
                                             , str(msg['content']) )
                file_obj.write(header)
            #
            print "=======Save decoded gcm messages to file:%s" % output_file
        
    @staticmethod
    def load_csv(csv_file_path, skip_headers=False):
        rows = []
        with open(csv_file_path, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=CSV_ROW_DELIMITER)
            row_index = 0
            for row in reader:
                row_index += 1
                if skip_headers and row_index == 1:
                    continue
                #
                rows.append(row)
        #
        return rows  


if __name__ == "__main__":
    import sys
    if len(sys.argv) <= 1:
        print "No enought argument. Usage:\n python gcm_decoder.py file or folder path"
        exit(0)
    #
    decoder = GCMDecoder()
    decoder.decode(sys.argv[1])
        