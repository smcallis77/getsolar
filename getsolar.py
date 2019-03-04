#!/usr/bin/env python

"""
  Copyright (c) 2018, Steve McAllister
  All Rights Reserved

"""

import sys
import os
import time
import sunspec.core.client as client
import sunspec.core.modbus as modbus
import sunspec.core.suns as suns
import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import logging
import syslog



"""
  Original suns options:
      -t: transport type: tcp or rtu (default: tcp)
      -a: modbus slave address (default: 1)
      -i: ip address to use for modbus tcp (default: localhost)
      -P: port number for modbus tcp (default: 502)
      -p: serial port for modbus rtu (default: /dev/ttyUSB0)
      -b: baud rate for modbus rtu (default: 9600)
      -T: timeout, in seconds (can be fractional, such as 1.5; default: 2.0)
      -m: specify model file
      -d: run as a daemon
      -D: debug mode (do not read any data)

"""

readRegister={
    "40004": "discard - Manufacturer",
    "40020": "discard - Model",
    "40036": "discard - EMPTY",
    "40044": "discard - Firmware Version",
    "40052": "discard - Serial Number",
    "40068": "discard - Device Address",
    "40071": "discard - Total Amps",
    "40072": "discard - Total Amps (Phase A)",
    "40073": "discard - Total Amps (Phase B)",
    "40074": "discard - Total Amps (Phase C)",
    "40076": "discard - Voltage (Phase AB)",
    "40077": "discard - Voltage (Phase BC)",
    "40078": "discard - Voltage (Phase CA)",
    "40079": "discard - Voltage (Phase A to N)",
    "40080": "discard - Voltage (Phase B to N)",
    "40081": "discard - Voltage (Phase C to N)",
    "40083": "AC Power",
    "40085": "discard - AC Frequency",
    "40087": "discard - Apparent Power",
    "40089": "discard - Reactive Power",
    "40091": "discard - Power Factor",
    "40093": "AC Lifetime Energy",
    "40096": "discard - DC Current",
    "40098": "discard - DC Voltage",
    "40100": "discard - DC Power",
    "40102": "discard - Cabinet Temperature",
    "40103": "discard - Heatsink Temperature",
    "40104": "discard - Transformer Temperature",
    "40105": "discard - Outside Temperature",
    "40107": "Operating State",
    "40108": "Vendor Status",
    "40109": "discard - Event 1",
    "40111": "discard - Event 2",
    "40113": "discard - Event Vendor 1",
    "40115": "discard - Event Vendor 2",
    "40117": "discard - Event Vendor 3",
    "40119": "discard - Event Vendor 4",
    "40123": "discard - Manufacturer",
    "40139": "discard - Model",
    "40155": "discard - Mode",
    "40163": "discard - Firmware Version",
    "40171": "discard - Serial Number",
    "40187": "discard - Device ID",
    "40190": "discard - AC Current",
    "40191": "discard - AC Current (Phase A)",
    "40192": "discard - AC Current (Phase B)",
    "40193": "discard - AC Current (Phase C)",
    "40195": "discard - AC Voltage",
    "40196": "discard - AC Voltage (Phase A)",
    "40197": "discard - AC Voltage (Phase B)",
    "40198": "discard - AC Voltage (Phase C)",
    "40199": "discard - AC Line to Line Voltage",
    "40200": "discard - AC Line to Line Voltage (AB)",
    "40201": "discard - AC Line to Line Voltage (BC)",
    "40202": "discard - AC Line to Line Voltage (CA)",
    "40204": "discard - AC Frequency",
    "40206": "Real Power",
    "40207": "discard - Real Power (Phase A)",
    "40208": "discard - Real Power (Phase B)",
    "40209": "discard - Real Power (Phase C)",
    "40211": "discard - Apparent Power",
    "40212": "discard - Apparent Power (Phase A)",
    "40213": "discard - Apparent Power (Phase B)",
    "40214": "discard - Apparent Power (Phase C)",
    "40216": "discard - Reactive Power",
    "40217": "discard - Reactive Power (Phase A)",
    "40218": "discard - Reactive Power (Phase B)",
    "40219": "discard - Reactive Power (Phase C)",
    "40221": "discard - Average Power Factor",
    "40222": "discard - Power Factor (Phase A)",
    "40223": "discard - Power Factor (Phase B)",
    "40224": "discard - Power Factor (Phase C)",
    "40226": "Exported Energy",
    "40228": "discard - Exported Energy (Phase A)",
    "40230": "discard - Exported Energy (Phase B)",
    "40232": "discard - Exported Energy (Phase C)",
    "40234": "Imported Energy",
    "40236": "discard - Imported Energy (Phase A)",
    "40238": "discard - Imported Energy (Phase B)",
    "40240": "discard - Imported Energy (Phase C)",
    "40243": "discard - Exported Apparent Energy",
    "40245": "discard - Exported Apparent Energy (Phase A)",
    "40247": "discard - Exported Apparent Energy (Phase B)",
    "40249": "discard - Exported Apparent Energy (Phase C)",
    "40251": "discard - Imported Apparent Energy",
    "40253": "discard - Imported Apparent Energy (Phase A)",
    "40255": "discard - Imported Apparent Energy (Phase B)",
    "40257": "discard - Imported Apparent Energy (Phase C)",
    "40260": "discard",
    "40262": "discard",
    "40264": "discard",
    "40266": "discard",
    "40268": "discard",
    "40270": "discard",
    "40272": "discard",
    "40274": "discard",
    "40276": "discard",
    "40278": "discard",
    "40280": "discard",
    "40282": "discard",
    "40284": "discard",
    "40286": "discard",
    "40288": "discard",
    "40290": "discard",
    "40293": "discard - Event Bits"
}

mqttClientName = "sunspec"
mqttHost = "ha.smcallister.org"
mqttPort = "1883"
mqttUsername = "homecontrol"
mqttPassword = "Cr44tchet$"
powerTopic= "house/solaredge/power/production"
exportTopic= "house/solaredge/power/export"
importTopic= "house/solaredge/power/import"
loadTopic= "house/solaredge/power/load"

# Initialise Influxdb data object
influxUser = 'telegraf'
influxPassword = 'critchet'
influxDBname = 'home_assistant'
influxDBuser = 'telegraf'
influxDBuser_password='critchet'
influxHost = 'localhost'
influxPort=8086
influxMeasure='Wh'
influxDomain='solaredge'
influxEntity='meters'

# Initialise syslog settings

_id = 'Solar Datalogger'
logStr = 'debug'
logFacilityLocalN = 1

# Initialise other settings
pidFile = '/var/run/getsolar/getsolar.pid'

class SysLogLibHandler(logging.Handler):
    """A logging handler that emits messages to syslog.syslog."""
    FACILITY = [syslog.LOG_LOCAL0,
                syslog.LOG_LOCAL1,
                syslog.LOG_LOCAL2,
                syslog.LOG_LOCAL3,
                syslog.LOG_LOCAL4,
                syslog.LOG_LOCAL5,
                syslog.LOG_LOCAL6,
                syslog.LOG_LOCAL7]
    def __init__(self, n):
        """ Pre. (0 <= n <= 7) """
        try:
            syslog.openlog(logoption=syslog.LOG_PID, facility=self.FACILITY[n])
        except Exception , err:
            try:
                syslog.openlog(syslog.LOG_PID, self.FACILITY[n])
            except Exception, err:
                try:
                    syslog.openlog('my_ident', syslog.LOG_PID, self.FACILITY[n])
                except:
                    raise
        # We got it
        logging.Handler.__init__(self)

    def emit(self, record):
        syslog.syslog(self.format(record))


def writePidFile():
    pid = str(os.getpid())
    f = open(pidFile, 'w')
    f.write(pid)
    f.close()

def rmPidFile():
    if os.path.exists(pidFile):
        os.remove(pidFile)

def getSolaredge(sd):

    global lastProductionEnergy,lastImportEnergy,lastExportEnergy
    maxretries=10

    if sd is not None:

        # read all models in the device
        retry=0
        while True:
            retry+=1
            try:
                sd.read()
            except client.SunSpecClientError, e:
                # Retry on read timeout
                if retry > maxRetries:
                    logging.critical("Sunspec: Too many retries. Giving up")
                    rmPidFile()
                    sys.exit(4)
                logging.info("Sunspec: {} - {}. Retrying".format(client.SunspecClientError, e))
                time.sleep(30)
                continue
#            except client.SunSpecClientError, e:
#                logging.critical("Sunspec: {} - {}. Exiting".format(client.SunSpecClientError, e))
#                rmPidFile()
#                sys.exit(2)
#            except modbus.client.ModbusClientTimeout, e:
#                # Retry on read timeout
#                if retry > maxRetries:
#                    logging.critical("Modbus: Too many retries. Giving up")
#                    rmPidFile()
#                    sys.exit(4)
#                logging.info("Modbus: {} - {}. Retrying".format(modbus.client.ModbusClientTimeout, e))
#                time.sleep(30)
#                continue
#            except modbus.client.ModbusClientError, e:
#                logging.critical("Modbus: {} - {}. Exiting".format(modbus.client.ModbusClientError, e))
#                rmPidFile()
#                sys.exit(3)
            break
        timeStamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        logging.info( 'Timestamp: %s' % (timeStamp))

        powerProduction=0.0
        powerImport=0.0
        powerExport=0.0
        powerLoad=0.0
        for model in sd.device.models_list:
            if model.model_type.label:
                label = '%s (%s)' % (model.model_type.label, str(model.id))
            else:
                label = '(%s)' % (str(model.id))
            # print('\nmodel: %s\n' % (label))
            for block in model.blocks:
                if block.index > 0:
                  index = '%02d:' % (block.index)
                else:
                  index = '   '
                for point in block.points_list:
                    if point.addr in readRegister: 
                        # print("Found a register {}, action = {}".format(point.addr,readRegister[point.addr]))
                        if readRegister[point.addr].startswith("discard"):
                            pass
                            # print("Register {} discarded".format(point.addr))
                        else:
                            if readRegister[point.addr].startswith("test"):
                                logging.debug("Now processing a point {}".format(point))
                            else:
                                if point.value is not None:
                                    if point.point_type.label:
                                        label = '   %s%s (%s):' % (index, point.point_type.label, point.point_type.id)
                                    else:
                                        label = '   %s(%s):' % (index, point.point_type.id)
                                    units = point.point_type.units
                                    if units is None:
                                        units = ''
                                    if point.point_type.type == suns.SUNS_TYPE_BITFIELD16:
                                        value = '0x%04x' % (point.value)
                                    elif point.point_type.type == suns.SUNS_TYPE_BITFIELD32:
                                        value = '0x%08x' % (point.value)
                                    else:
                                        value = str(point.value).rstrip('\0')
                                    logging.info('%-40s %20s %-10s' % (label, value, str(units)))
                                    if point.addr == "40083":
                                        # Publish current production power
                                        powerProduction=float(value)
                                    elif point.addr == "40206":
                                        if float(value) > 0:
                                            powerExport=float(value)
                                            powerImport=float(0)
                                        else:
                                            powerImport=float(value)*-1.0
                                            powerExport=float(0)
                                    elif point.addr == "40093":
                                        deltaProductionEnergy=float(value)-lastProductionEnergy
                                        lastProductionEnergy=float(value)
                                    elif point.addr == "40234":
                                        deltaImportEnergy=float(value)-lastImportEnergy
                                        lastImportEnergy=float(value)
                                    elif point.addr == "40226":
                                        deltaExportEnergy=float(value)-lastExportEnergy
                                        lastExportEnergy=float(value)
                    else:
                        logging.error("Register {} not found".format(point.addr))
        # Derive values
        powerLoad=powerProduction-powerExport+powerImport
        deltaConsumptionEnergy=deltaProductionEnergy-deltaExportEnergy+deltaImportEnergy
        deltaSelfConsumptionEnergy=deltaProductionEnergy-deltaExportEnergy
        md.publish(powerTopic,powerProduction/1000)
        md.publish(exportTopic,powerExport/1000)
        md.publish(importTopic,powerImport/1000)
        md.publish(loadTopic,powerLoad/1000)

        # Write energy values to influx
        influx_metric = [{
            'measurement': influxMeasure,
            'time': timeStamp,
            'tags': {
                'domain': influxDomain,
                'entity_id':influxEntity 
            },
            'fields': {
                 'Production': deltaProductionEnergy,
                 'Import': deltaImportEnergy,
                 'Export': deltaExportEnergy,
                 'Consumption': deltaConsumptionEnergy,
                 'Self-Consumption': deltaSelfConsumptionEnergy
            }
        }]

        dd.write_points(influx_metric,time_precision='s')

        # Print published values

        logging.info("Published values:")
        logging.info("Power Production: {}".format(powerProduction))
        logging.info("Power Export: {}".format(powerExport))
        logging.info("Power Import: {}".format(powerImport))
        logging.info("Power Load: {}".format(powerLoad))
        logging.info("Current Energy Production: {}".format(deltaProductionEnergy))
        logging.info("Current Energy Export: {}".format(deltaExportEnergy))
        logging.info("Current Energy Import: {}".format(deltaImportEnergy))
        logging.info("Current Energy Consumption: {}".format(deltaConsumptionEnergy))
        logging.info("Current Energy Self Consumption: {}".format(deltaSelfConsumptionEnergy))


if __name__ == "__main__":

    # Last published energy counters

    lastProductionEnergy=0.0
    lastImportEnergy=0.0
    lastExportEnergy=0.0

    parser = argparse.ArgumentParser(description='Get solar performance data from a sunspec compliant inverter')
    parser.add_argument('-t', metavar=' ',
                      default='tcp',
                      help='transport type: rtu, tcp, mapped [default: tcp]')
    parser.add_argument('-a', metavar=' ', type=int,
                      default=1,
                      help='modbus slave address [default: 1]')
    parser.add_argument('-i', metavar=' ',
                      default='localhost',
                      help='ip address to use for modbus tcp [default: localhost]')
    parser.add_argument('-P', metavar=' ', type=int,
                      default=502,
                      help='port number for modbus tcp [default: 502]')
    parser.add_argument('-p', metavar=' ',
                      default='/dev/ttyUSB0',
                      help='serial port for modbus rtu [default: /dev/ttyUSB0]')
    parser.add_argument('-b', metavar=' ',
                      default=9600,
                      help='baud rate for modbus rtu [default: 9600]')
    parser.add_argument('-T', metavar=' ', type=float,
                      default=2.0,
                      help='timeout, in seconds (can be fractional, such as 1.5) [default: 2.0]')
    parser.add_argument('-m', metavar=' ',
                      help='modbus map file')
    parser.add_argument('-d', action="store_true",
                      help='run as a daemon')
    parser.add_argument('-D', action="store_true",
                      help='run in debug mode')
    args = parser.parse_args()

    # Setup logging
    # Defines a logging level and logging format based on a given string key.
    LOG_ATTR = {'debug': (logging.DEBUG,
                          _id + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'),
                'info': (logging.INFO,
                         _id + ' %(levelname)-9s %(message)s'),
                'warning': (logging.WARNING,
                            _id + ' %(levelname)-9s %(message)s'),
                'error': (logging.ERROR,
                          _id + ' %(levelname)-9s %(message)s'),
                'critical': (logging.CRITICAL,
                             _id + ' %(levelname)-9s %(message)s')}
    loglevel, logformat = LOG_ATTR[logStr]

    # Configuring the logger
    logger = logging.getLogger()
    logger.setLevel(loglevel)

    # Clearing previous logs
    logger.handlers = []

    # Setting formaters and adding handlers.
    formatter = logging.Formatter(logformat)
    handlers = []
    handlers.append(SysLogLibHandler(logFacilityLocalN))
    for h in handlers:
        h.setFormatter(formatter)
        logger.addHandler(h)

    # if running as a daemon, record the PID
    if args.d:
        if os.path.isfile(pidFile):
            logging.error("PID already exists. Is getsolar already running? Remove {} if it is not".format(pidFile))
            rmPidFile()
            sys.exit(1)
        writePidFile()

    try:
        if args.t == 'tcp':
            sd = client.SunSpecClientDevice(client.TCP, args.a, ipaddr=args.i, ipport=args.P, timeout=args.T)
        elif args.t == 'rtu':
            sd = client.SunSpecClientDevice(client.RTU, args.a, name=args.p, baudrate=args.b, timeout=args.T)
        elif args.t == 'mapped':
            sd = client.SunSpecClientDevice(client.MAPPED, args.a, name=args.m)
        else:
            logging.critical('Unknown -t option: %s' % (args.t))
            rmPidFile()
            sys.exit(1)

    except client.SunSpecClientError, e:
        logging.critical('Sunspec: %s' % (e))
        rmPidFile()
        sys.exit(2)
    except modbus.client.ModbusClientError, e:
        logging.critical('Modbus: %s' % (e))
        rmPidFile()
        sys.exit(3)

    md = mqtt.Client(mqttClientName)
    md.username_pw_set(mqttUsername,mqttPassword)
    md.connect(mqttHost,mqttPort)
    md.loop_start()

    dd = InfluxDBClient(influxHost,influxPort,influxUser,influxPassword,influxDBname)

    # Setup 'last' energy counters

    result=dd.query('select sum(Production) as Production,sum(Export) as Export ,sum(Import) as Import from Wh')
    #print("Result: {}".format(result.raw))
    points=result.get_points()
    for point in points:
        lastProductionEnergy=point['Production']
        lastExportEnergy=point['Export']
        lastImportEnergy=point['Import']

    while True:
        if sd is not None:
            if args.D:
                print("Running in debug mode. Not reading any data")
            else:
                # Read registers
                pass
                data=getSolaredge(sd)
            time.sleep(60)
        else:
            break

