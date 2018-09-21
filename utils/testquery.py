#!/bin/env python

import influxdb

if __name__ == '__main__':
    ic = influxdb.client.InfluxDBClient(host='localhost',port=9999,username="bob",password="blahblah")
    res = ic.query(query='SELECT * FROM mymeas',database='mydb')
    print res
