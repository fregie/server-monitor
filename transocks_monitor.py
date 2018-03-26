#! /usr/bin/env python
#-*- coding:utf-8 -*-

import re
import pytz, time, datetime
import codecs
import json, getopt, sys
from influxdb import InfluxDBClient
from yunpian_python_sdk.model import constant as YC
from yunpian_python_sdk.ypclient import YunpianClient

def get_config(config_file = "/etc/transocks_monitor/monitor.json"):
    try:
        config_fo = codecs.open(config_file, "r+",'utf-8')
        config = json.load(config_fo)
    except :
        print "config file error"
        sys.exit(2)
    else:
        config_fo.close()
        return config

config_file = "/etc/transocks_monitor/monitor.json"
try:
    opts, args = getopt.getopt(sys.argv[1:],"hc:",["config="])
except getopt.GetoptError:
    print 'transocks_monitor.py -c <config_file>'
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print 'transocks_monitor.py -c <config_file>'
        sys.exit()
    elif opt in ("-c", "--config"):
        config_file = arg

config = get_config(config_file)

#------------------ config ------------------#
log_file = config['log_file']
tmp_file = config["tmp_file"]
influxDB_host = config['influxDB_host']
influxDB_port = config['influxDB_port']
influxDB_user = config['influxDB_user']
influxDB_pswd = config['influxDB_pswd']
influxDB_DBname = config['influxDB_DBname']

YP_APIKEY = config['YP_APIKEY']
alert_numbers = config['alert_numbers']

# 会匹配host name中含有以下字符串的服务器
server_types = config['server_types']
interval = config['interval']
# 平均带宽低于该值会发出告警
bottom_line = config['bottom_line']
#-----------------------------------------------#

#init
log_fo = codecs.open(log_file, "a+",'utf-8')
client = InfluxDBClient(host=influxDB_host, port=influxDB_port, username=influxDB_user, password=influxDB_pswd, database=influxDB_DBname)
YP_clnt = YunpianClient(YP_APIKEY)

def get_hosts_list(name_list):
    hosts_list = []
    cmd = "SHOW TAG VALUES FROM \"interface_rx\" WITH KEY = \"host\""
    _result = client.query(cmd)
    result_generator = _result.items()[0][1]
    for host in result_generator:
        for name in name_list:
            if re.match(name,host['value']):
                hosts_list.append(host['value'])
                break

    return hosts_list

def get_instance_name(host):
    cmd = "SELECT * FROM interface_rx WHERE \"host\"='{0}' AND \"type\" = 'if_octets' limit 1;".format(host)
    _result = client.query(cmd)
    if len(_result):
        result_generator = _result.items()[0][1]
        return result_generator.next()['instance']
    else:
        raise Exception('can\'t fine data')

def get_two_times(range_min=10):
    result_time = {}
    tz = pytz.timezone('UTC')
    result_time['current'] = datetime.datetime.now(tz).strftime("%04Y-%02m-%02dT%02H:%02M:%02SZ")
    just_ts = time.mktime(time.strptime(result_time['current'],"%Y-%m-%dT%H:%M:%SZ"))-int(range_min)*60
    result_time['just_now'] = time.strftime("%04Y-%02m-%02dT%02H:%02M:%02SZ",time.localtime(just_ts))

    return result_time

def get_spread_data(host,key,instance,interval=10):
    # result_time = get_two_times(interval)
    cmd = "SELECT SPREAD(\"value\") FROM {0} WHERE \"host\"='{1}' AND \"type\" = 'if_octets' \
   AND time > now() - {2}m AND \"instance\" = '{3}';" \
    .format(key, host, interval, instance)
    # print cmd
    _result = client.query(cmd)
    # print _result
    if len(_result):
        result_generator = _result.items()[0][1]
        return result_generator.next()['spread']
    else:
        raise Exception('can\'t fine data')

def get_average_bandwidth(host,interval=10):
    instance = get_instance_name(host)
    rx_spread = get_spread_data(host,'interface_rx',instance,interval)
    tx_spread = get_spread_data(host,'interface_tx',instance,interval)
    rx_average_bandwidth = rx_spread/(interval*60)
    tx_average_bandwidth = tx_spread/(interval*60)

    return (rx_average_bandwidth,tx_average_bandwidth)

def get_ss_user_count(host,interval=5):
    cmd = "SELECT \"value\" FROM \"ss_value\" WHERE \"host\"='{0}' AND \"type_instance\" = 'total'\
           AND time > now() - {1}m limit 1;".format(host, interval)
    # print cmd
    _result = client.query(cmd)
    # print 
    if len(_result):
        result_generator = _result.items()[0][1]
        return result_generator.next()['value']
    else:
        raise Exception('can\'t fine data')

def get_cpu_load(host,interval=1):
    cpu_load_list = []
    instance_list = []
    cmd = "SHOW TAG VALUES FROM \"cpu_value\" WITH KEY = \"instance\" WHERE \"host\"='{0}'".format(host)
    _result = client.query(cmd)
    result_generator = _result.items()[0][1]
    for instance in result_generator:
        instance_list.append(instance['value'])

    for instance in instance_list:
        cpu = {}
        cpu['instance'] = instance
        cmd = "SELECT \"value\" FROM \"cpu_value\" WHERE \"host\"='{0}' AND \"type_instance\" != 'idle'\
            AND \"type\" = 'percent' AND time > now() - {1}m AND \"instance\"='{2}' limit 7;".format(host, interval, instance)
        # print cmd
        _result = client.query(cmd)
        # print _result
        if len(_result):
            cpu['load'] = 0
            result_generator = _result.items()[0][1]
            for i in range(7):
                cpu['load'] += float(result_generator.next()['value'])
        else:
            raise Exception('can\'t fine data')
        # print(cpu)
        cpu_load_list.append(cpu)

    return cpu_load_list

def send_err_msg(err_reason):
    #err_reason = err_reason[-10:] #api要求参数只能10个字节长度,截取最后10个字节
    msg_text = '【穿月科技】您的服务器出现异常，请及时处理。异常原因：{0}'.format(err_reason)
    for number in alert_numbers:
        param = {YC.MOBILE:number, YC.TEXT:msg_text}
        r = YP_clnt.sms().single_send(param)
        if r.code() == 0 :
            log_fo.write("send alert msg to success\n")
        else:
            log_fo.write("send alert msg to failed\n")

def get_tmp_json():
    try:
        tmp_fo = codecs.open(tmp_file, "r+",'utf-8')
        json_obj = json.load(tmp_fo)
    except :
        json_obj = {}
    else:
        tmp_fo.close()
    
    return json_obj

def write_tmp_json(json_obj):
    tmp_fo = codecs.open(tmp_file, "w+",'utf-8')
    json.dump(json_obj,tmp_fo)
    tmp_fo.close()

def server_OK(hosts_status,host):
    if hosts_status.has_key(host) and hosts_status[host] != 'OK':
        send_err_msg("[{0}]服务器恢复正常".format(host))
    hosts_status[host] = 'OK'

def server_error(hosts_status,host,err_reason):
    if hosts_status.has_key(host) and hosts_status[host] == 'error':
        return
    hosts_status[host] = 'error'
    send_err_msg("[{0}]{1}".format(host, err_reason))

hosts_status = get_tmp_json()
hosts_list = get_hosts_list(server_types)
log_fo.write(time.strftime('%Y-%m-%d %H:%M:%S:\n',time.localtime(time.time())))
#check transocks
for host in hosts_list:
    try:
        rx_average_bandwidth,tx_average_bandwidth=get_average_bandwidth(host,interval)
        rx_average_bandwidth = rx_average_bandwidth*8/1024
        tx_average_bandwidth = tx_average_bandwidth*8/1024
        log_fo.write("[{0}] rx {1} kbps  tx {2} kbps\n".format(host,rx_average_bandwidth,tx_average_bandwidth))

        user_count = get_ss_user_count(host)
        cpu_load = get_cpu_load(host)

        # print("user count: {0}".format(user_count))
        # print("[{0}] rx {1} kbps  tx {2} kbps".format(host,rx_average_bandwidth,tx_average_bandwidth))
        # print("cpu load: {0}%".format(cpu_load))

        # transocks accel server need to monitor bandwidth
        if rx_average_bandwidth < bottom_line or tx_average_bandwidth < bottom_line:
            server_error(hosts_status, host, "带宽低于正常(rx:{1} tx:{2})".format(rx_average_bandwidth,tx_average_bandwidth))
            log_fo.write("[notice]bandwidth too low!! maybe server down!!\n")
        
        else:
            server_OK(hosts_status,host)
    except Exception:
        server_error(hosts_status, host, "获取数据失败")
        log_fo.write("get data form {0} failed,maybe dead\n".format(host))

#check abs
abs_host = ['abs-01','abs-02']
for host in abs_host:
    try:
        cpu_load = get_cpu_load(host)
        rx_average_bandwidth,tx_average_bandwidth=get_average_bandwidth(host,interval)

        for core in cpu_load:
            # print("[{0}]core{1}: {2}%".format(host,core['instance'],core['load']))
            log_fo.write("[{0}]core{1}: {2}%\n".format(host,core['instance'],int(core['load'])))
            if core['load'] > 35:
                server_error(hosts_status, host, "CPU负载超过35%")
                log_fo.write("[{0}]CPU负载超过35%".format(host))
            else:
                server_OK(hosts_status,host)

    except Exception:
        server_error(hosts_status, host, "获取数据失败")
        log_fo.write("get data form {0} failed,maybe dead\n".format(host))

write_tmp_json(hosts_status)
log_fo.close()