#!/usr/bin/env python
import string
import db
import datetime
import time

def room_get_machines(room_id):
    ip_dict = {}
    
    msmaster_db = db.DB_MYSQL()
    msmaster_db.connect(db.DB_CONFIG_MSMASTER.host, db.DB_CONFIG_MSMASTER.port, db.DB_CONFIG_MSMASTER.user, db.DB_CONFIG_MSMASTER.password, db.DB_CONFIG_MSMASTER.db)
    
    sql = 'select server_ip from mobile_ms where room_id=%d' % (room_id)
    #print sql
    
    msmaster_db.execute(sql)
    query_set_1 = msmaster_db.cur.fetchall()
    for row1 in query_set_1:         
        r1_index = 0
        for r1 in row1:
            if(r1_index == 0):
                ip_dict[r1] = '1'
            r1_index += 1
    
    msmaster_db.close()
    
    return ip_dict


def hits_to_ips(data_file, ip_dict):
    hits_num = 0
    
    filep = None
    try:
        filep = open(data_file, 'r')
    except IOError, e:
        print 'open %s error!' % (data_file)
        print e
        return 0
        
    while(True):
        line = filep.readline()
        if(line == ''):
            break           
        items = line.split(',')        
        if(len(items) < 5):
            continue
        client_ip   = items[0]
        task_hash   = items[3]
        server_ip   = items[4]
        if(ip_dict.has_key(server_ip)):
            hits_num += 1 
            #print '%s %d %s' % (server_ip, hits_num, task_hash)
    
    filep.close()    
    
    return hits_num

    
def room_hits_total(room_id, str_date):    
    ip_dict = room_get_machines(room_id)    
    '''
    for ip in ip_dict:
        print '%s' % (ip)
    '''
    str_year    = str_date[0:4]
    str_month   = str_date[4:6]
    str_day     = str_date[6:8]
    data_file = '/media2/log_project/oxeye/ecom_mobile/fbuffer/%s/%s/%s/logdata_%s_loc.result' % (str_year, str_month, str_day, str_date)
    hits_num = hits_to_ips(data_file, ip_dict)    
    
    print 'room_id %d, date %s, hit_num %d' % (room_id, str_date, hits_num)
    
    return True


def print_usage(program_name):
    print '%s [room_id] [date]' % (program_name)
    
def main():
    if(len(sys.argv) < 3):
        print_usage(sys.argv[0])
        return False
    
    room_id = string.atoi(sys.argv[1])
    begin_date = sys.argv[2]
        
    begin_day = datetime.date(string.atoi(begin_date[0:4]), string.atoi(begin_date[4:6]), string.atoi(begin_date[6:8])) 
    
    now_time = time.localtime(time.time())
    today = time.strftime("%Y%m%d", now_time)
    end_day = datetime.date(string.atoi(today[0:4]), string.atoi(today[4:6]), string.atoi(today[6:8]))
    
    day_num = 1
    d1 = begin_day
    while(True):    
        the_date = d1.strftime("%Y%m%d")
        #print the_date
        room_hits_total(room_id, the_date)
        
        delta = datetime.timedelta(days=day_num)
        d1 = d1 + delta
        if(d1 >= end_day):
            break
        
    
    #room_hits_hashs()
    return True


if __name__ == "__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    main()