#!/usr/bin/env python3

# getsolar.py v1.1 20-July-2020

"""
  Copyright (c) 2018, Steve McAllister
  All Rights Reserved

"""

import sys
import os
import time
import solaredge_modbus
import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import logging
import syslog
import json



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

mqttClientName = "getsolar"
mqttHost = "ha.smcallister.org"
mqttPort = "1883"
mqttUsername = "homecontrol"
mqttPassword = "Cr44tchet$"
powerTopic= "house/solaredge/power/production"
exportTopic= "house/solaredge/power/export"
importTopic= "house/solaredge/power/import"
loadTopic= "house/solaredge/power/load"
inverterTopic="house/solaredge/inverter/state"
meterTopic="house/solaredge/meter/state"

# Initialise Influxdb data object
influxUser = 'telegraf'
influxPassword = 'critchet'
influxDBAll = 'home_assistant'
influxDBPower = 'powerlogging'
influxDBuser = 'telegraf'
influxDBuser_password='critchet'
influxHost = 'localhost'
influxPort=8086
influxDomain='solaredge'
influxEntity='meters'

# Initialise syslog settings

_id = 'getsolar v1.1'
logStr = 'info'
logFacilityLocalN = 1
sleepTime=10
waitTime=1

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
        except Exception as err:
            try:
                syslog.openlog(syslog.LOG_PID, self.FACILITY[n])
            except Exception as err:
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

def getSolaredge(sd=None,cycle=None,debug=False):

    global lastProductionEnergy,lastImportEnergy,lastExportEnergy
    maxRetries=10

    if sd is not None:

        # read all models in the device
        retry=5
        while retry > 0:
            logging.debug("Trying. Cycle={}, Retry={}".format(cycle,retry))
            try:
                iData=sd.read_all()
                meter1=sd.meters()["Meter1"]
                mData=meter1.read_all()

                powerProduction=float(iData['power_ac']*10**iData['power_ac_scale'])
                if mData['power'] > 0:
                    powerExport=float(mData['power']*10**mData['power_scale'])
                    powerImport=0.0
                else:
                    powerImport=float(-1.0*mData['power']*10**mData['power_scale'])
                    powerExport=0.0
            except Exception as err:
                # Retry on read exception
                logging.debug(err)
                logging.info("Register read error - retrying")
                retry-=1
                time.sleep(waitTime)
            else:
                retry=0
                timeStamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                logging.debug( 'Timestamp: %s' % (timeStamp))
                if cycle == 0:
                    deltaProductionEnergy=float(iData['energy_total']*10**iData['energy_total_scale']-lastProductionEnergy)
                    lastProductionEnergy=float(iData['energy_total']*10**iData['energy_total_scale'])
                    deltaImportEnergy=float(mData['import_energy_active']*10**mData['energy_active_scale']-lastImportEnergy)
                    lastImportEnergy=float(mData['import_energy_active']*10**mData['energy_active_scale'])
                    deltaExportEnergy=float(mData['export_energy_active']*10**mData['energy_active_scale']-lastExportEnergy)
                    lastExportEnergy=float(mData['export_energy_active']*10**mData['energy_active_scale'])
                # Derive values
                powerLoad=float(powerProduction-powerExport+powerImport)

                if cycle == 0:
                    deltaConsumptionEnergy=float(deltaProductionEnergy-deltaExportEnergy+deltaImportEnergy)
                    deltaSelfConsumptionEnergy=float(deltaProductionEnergy-deltaExportEnergy)
                    # Write energy values to influx
                    influxMeasure='Wh'
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
                    if (not debug):

                        # Decode inverter status
                        iData['status']=solaredge_modbus.INVERTER_STATUS_MAP[iData['status']]
                        logging.debug("Publishing data")
                        md.publish(powerTopic,powerProduction/1000)
                        md.publish(exportTopic,powerExport/1000)
                        md.publish(importTopic,powerImport/1000)
                        md.publish(loadTopic,powerLoad/1000)
                        md.publish(inverterTopic,json.dumps(iData))
                        md.publish(meterTopic,json.dumps(mData))

                        logging.debug(deltaProductionEnergy)
                        logging.debug(deltaImportEnergy)
                        logging.debug(deltaExportEnergy)
                        logging.debug(deltaConsumptionEnergy)
                        logging.debug(deltaSelfConsumptionEnergy)
                        dd.write_points(influx_metric,time_precision='s')

                        # Print published values

                        logging.info("Power Production: {}".format(powerProduction))
                        logging.info("Power Export: {}".format(powerExport))
                        logging.info("Power Import: {}".format(powerImport))
                        logging.info("Power Load: {}".format(powerLoad))
                        logging.info("Current Energy Production: {}".format(deltaProductionEnergy))
                        logging.info("Current Energy Export: {}".format(deltaExportEnergy))
                        logging.info("Current Energy Import: {}".format(deltaImportEnergy))
                        logging.info("Current Energy Consumption: {}".format(deltaConsumptionEnergy))
                        logging.info("Current Energy Self Consumption: {}".format(deltaSelfConsumptionEnergy))
                    else:
                        logging.debug("Power Production: {}".format(powerProduction))
                        logging.debug("Power Export: {}".format(powerExport))
                        logging.debug("Power Import: {}".format(powerImport))
                        logging.debug("Power Load: {}".format(powerLoad))
                        logging.debug("Current Energy Production: {}".format(deltaProductionEnergy))
                        logging.debug("Current Energy Export: {}".format(deltaExportEnergy))
                        logging.debug("Current Energy Import: {}".format(deltaImportEnergy))
                        logging.debug("Current Energy Consumption: {}".format(deltaConsumptionEnergy))
                        logging.debug("Current Energy Self Consumption: {}".format(deltaSelfConsumptionEnergy))
                # Write power values to influx
                influxMeasure='W'
                influx_metric = [{
                    'measurement': influxMeasure,
                    'time': timeStamp,
                    'tags': {
                        'domain': influxDomain,
                        'entity_id':influxEntity 
                    },
                    'fields': {
                         'Production': powerProduction,
                         'Import': powerImport,
                         'Export': powerExport,
                         'Load': powerLoad
                    }
                }]
                if (not debug):
                    pass
                    logging.debug("Writing power points")
                    dp.write_points(influx_metric,time_precision='s')
                


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

    if args.D:
        logStr='debug'
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

    md = mqtt.Client(mqttClientName)
    md.username_pw_set(mqttUsername,mqttPassword)
    md.connect(mqttHost,int(mqttPort))
    md.loop_start()

    dd = InfluxDBClient(influxHost,influxPort,influxUser,influxPassword,influxDBAll)
    dp = InfluxDBClient(influxHost,influxPort,influxUser,influxPassword,influxDBPower)

    # Setup 'last' energy counters

    result=dd.query('select sum(Production) as Production,sum(Export) as Export ,sum(Import) as Import from Wh')
    logging.debug("Result: {}".format(result.raw))
    points=result.get_points()
    for point in points:
        lastProductionEnergy=point['Production']
        lastExportEnergy=point['Export']
        lastImportEnergy=point['Import']

    cycle=0
    retry=5
    logging.info("Modbus: Opening client")
    if args.t == 'tcp':
        try:
            sd = solaredge_modbus.Inverter(host=args.i, port=args.P)
        except Exception as err:
            logging.debug("Opening")
            logging.debug(Exception)
            logging.debug(err)
    elif args.t == 'rtu':
        sd = solaredge_modbus.Inverter(device=args.p, baud=args.b)
    else:
        logging.critical('Unknown -t option: %s' % (args.t))
        rmPidFile()
        sys.exit(1)
    while retry != 0:
        if sd.connected():
            retry=5
            if args.D:
                # Read registers
                logging.debug("Running in debug mode, not writing data")
                logging.debug("Reading data - cycle {}".format(cycle))
                data=getSolaredge(sd,cycle,True)
            else:
                # Read registers
                logging.debug("Reading data - cycle {}".format(cycle))
                data=getSolaredge(sd,cycle)
            cycle+=1
            if cycle > 5:
                cycle=0
            #logging.info("Sunspec: Closing client")
            # Close the connection (TODO)
            #sd.close()
            time.sleep(sleepTime)
        else:
            logging.info('Connect failed - retrying')
            retry-=1
            time.sleep(waitTime)
            logging.info("Modbus: Opening client")
            if args.t == 'tcp':
                try:
                    sd = solaredge_modbus.Inverter(host=args.i, port=args.P)
                except Exception as err:
                    logging.debug("Opening")
                    logging.debug(Exception)
                    logging.debug(err)
            elif args.t == 'rtu':
                sd = solaredge_modbus.Inverter(device=args.p, baud=args.b)
            else:
                logging.critical('Unknown -t option: %s' % (args.t))
                rmPidFile()
                sys.exit(1)
    logging.error("Too many retries")
    rmPidFile()
    sys.exit(2)

