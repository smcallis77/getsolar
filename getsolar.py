#!/usr/bin/env python3

# getsolar.py v1.2 30-July-2020

"""
  Copyright (c) 2018, Steve McAllister
  All Rights Reserved

Connects to a solaredge inverter extracts data and writes to two influx databases.

Database 1 - homeassistant(HA) DB writes important sensor data for homeassistant
             approximately once per minute
Database 2 - powerlogging DB - writes a smaller amount of data more frequently
             for analysis of 'AC Load'

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

Solaredge Reister Details
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

Changelog.

v1.2 - update code to comply with pylint coding standards

"""

import sys
import os
import time
import logging
import syslog
import json
import argparse
import solaredge_modbus
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import keyring.backend
from keyrings.alt.file import PlaintextKeyring


MQTT_CLIENT_NAME = "getsolar"
MQTT_HOST = "ha.smcallister.org"
MQTT_PORT = "1883"
MQTT_USER = "homecontrol"
POWER_TOPIC = "house/solaredge/power/production"
EXPORT_TOPIC = "house/solaredge/power/export"
IMPORT_TOPIC = "house/solaredge/power/import"
LOAD_TOPIC = "house/solaredge/power/load"
INVERTER_TOPIC = "house/solaredge/inverter/state"
METER_TOPIC = "house/solaredge/meter/state"

# Initialise Influxdb data object
INFLUX_USER = 'telegraf'
INFLUX_DB_ALL = 'home_assistant'
INFLUX_DB_POWER = 'powerlogging'
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
INFLUX_DOMAIN = 'solaredge'
INFLUX_ENTITY = 'meters'

# Initialise syslog settings

_ID = 'getsolar v1.1'
LOG_FACILITY_LOCAL_N = 1
SLEEP_TIME = 10
WAIT_TIME = 1
MAX_RETRIES = 5

# Initialise other settings
PID_FILE = '/var/run/getsolar/getsolar.pid'

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
                    syslog.openlog('my_IDent', syslog.LOG_PID, self.FACILITY[n])
                except:
                    raise
        # We got it
        logging.Handler.__init__(self)

    def emit(self, record):
        syslog.syslog(self.format(record))


def write_pid_file():
    """
    Writes a file containing the current process id
    """
    pid = str(os.getpid())
    _f = open(PID_FILE, 'w')
    _f.write(pid)
    _f.close()

def rm_pid_file():
    """
    Deletes the file containing the current process id
    """
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

def get_solaredge(s_d, m_d, d_d, d_p, cycle, energy_last, debug=False):

    """
    Reads data from the inverter and decodes the required values
    """


    # read all models in the device
    retry = MAX_RETRIES
    while retry > 0:
        logging.debug("Trying. Cycle= %s, Retry= %s", cycle, retry)
        try:
            inv_data = s_d.read_all()
            meter1 = s_d.meters()["Meter1"]
            meter_data = meter1.read_all()

            power_prod = float(inv_data['power_ac']*10**inv_data['power_ac_scale'])
            if meter_data['power'] > 0:
                power_exp = float(meter_data['power']*10**meter_data['power_scale'])
                power_imp = 0.0
            else:
                power_imp = float(-1.0*meter_data['power']*10**meter_data['power_scale'])
                power_exp = 0.0
        except Exception as err:
            # Retry on read exception
            logging.debug(err)
            logging.info("Register read error - retrying")
            retry -= 1
            time.sleep(WAIT_TIME)
        else:
            retry = 0
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            logging.debug('Timestamp: %s', timestamp)
            if cycle == 0:
                prod_energy_delta = \
                    float(inv_data['energy_total']*10**inv_data['energy_total_scale']- energy_last['production'])
                imp_energy_delta = \
                    float(meter_data['import_energy_active']*10**meter_data['energy_active_scale'] \
                          - energy_last['import'])
                exp_energy_delta = \
                    float(meter_data['export_energy_active']*10**meter_data['energy_active_scale'] \
                          -energy_last['export'])
                # Derive values
            power_load = float(power_prod-power_exp+power_imp)

            if cycle == 0:
                cons_energy_delta = float(prod_energy_delta-exp_energy_delta+imp_energy_delta)
                s_cons_energy_delta = float(prod_energy_delta-exp_energy_delta)
                # Write energy values to influx
                influx_measure = 'Wh'
                influx_metric = [{
                    'measurement': influx_measure,
                    'time': timestamp,
                    'tags': {
                        'domain': INFLUX_DOMAIN,
                        'entity_ID':INFLUX_ENTITY
                    },
                    'fields': {
                        'Production': prod_energy_delta,
                        'Import': imp_energy_delta,
                        'Export': exp_energy_delta,
                        'Consumption': cons_energy_delta,
                        'Self-Consumption': s_cons_energy_delta
                    }
                }]
                # Decode inverter status
                inv_data['status'] = solaredge_modbus.INVERTER_STATUS_MAP[inv_data['status']]
                if not debug:
                    m_d.publish(POWER_TOPIC, power_prod/1000)
                    m_d.publish(EXPORT_TOPIC, power_exp/1000)
                    m_d.publish(IMPORT_TOPIC, power_imp/1000)
                    m_d.publish(LOAD_TOPIC, power_load/1000)
                    m_d.publish(INVERTER_TOPIC, json.dumps(inv_data))
                    m_d.publish(METER_TOPIC, json.dumps(meter_data))

                    d_d.write_points(influx_metric, time_precision='s')

                else:
                        # Print published values

                    logging.debug("Publishing data")
                    logging.debug("Power - Production: %s, Export: %s, Import: %s, Load: %s", \
                        power_prod, power_exp, power_imp, power_load)
                    logging.debug( \
                        "Energy  - Production: %s, Export: %s, Import: %s, Consumption: %s, Self Consumption: %s", \
                        prod_energy_delta, \
                        exp_energy_delta, \
                        imp_energy_delta, \
                        cons_energy_delta, \
                        s_cons_energy_delta)
            # Write power values to influx
            influx_measure = 'W'
            influx_metric = [{
                'measurement': influx_measure,
                'time': timestamp,
                'tags': {
                    'domain': INFLUX_DOMAIN,
                    'entity_ID':INFLUX_ENTITY
                },
                'fields': {
                    'Production': power_prod,
                    'Import': power_imp,
                    'Export': power_exp,
                    'Load': power_load
                }
            }]
            if not debug:
                logging.debug("Writing power points")
                d_p.write_points(influx_metric, time_precision='s')
            return float(inv_data['energy_total']*10**inv_data['energy_total_scale']), \
                   float(meter_data['import_energy_active']*10**meter_data['energy_active_scale']), \
                   float(meter_data['export_energy_active']*10**meter_data['energy_active_scale'])


def parse_args():
    """
        configure valid arguments
    """

    parser = argparse.ArgumentParser(description='Get solar performance data from a solaredge inverter')
    args = parser.parse_args()
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
    parser.add_argument('-D', action="store_true",
                        help='run in debug mode')
    print(args)
    return args

def set_logging(log_str):
    # Defines a logging level and logging format based on a given string key.
    log_attr = {'debug': (logging.DEBUG,
                          _ID + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'),
                'info': (logging.INFO,
                         _ID + ' %(levelname)-9s %(message)s'),
                'warning': (logging.WARNING,
                            _ID + ' %(levelname)-9s %(message)s'),
                'error': (logging.ERROR,
                          _ID + ' %(levelname)-9s %(message)s'),
                'critical': (logging.CRITICAL,
                             _ID + ' %(levelname)-9s %(message)s')}
    loglevel, logformat = log_attr[log_str]

    # Configuring the logger
    logger = logging.getLogger()
    logger.setLevel(loglevel)

    # Clearing previous logs
    logger.handlers = []

    # Setting formaters and adding handlers.
    formatter = logging.Formatter(logformat)
    handlers = []
    handlers.append(SysLogLibHandler(LOG_FACILITY_LOCAL_N))
    for handle in handlers:
        handle.setFormatter(formatter)
        logger.addHandler(handle)


def main():
    """
    Main processing loop
    """

    # Get the passwords from the plain text keyring
    keyring.set_keyring(PlaintextKeyring())
    mqtt_password = keyring.get_password(MQTT_HOST, MQTT_USER)
    influx_password = keyring.get_password(INFLUX_HOST, INFLUX_USER)

    # Initialise last published energy counters
    energy_last = {'production': 0.0, 'import': 0.0, 'export': 0.0}

    args = parse_args()

    print(args)
    if args.D:
        set_logging('debug')
        debug = True
    else:
        set_logging('info')
        debug = False
    # Setup logging

    # if running as a daemon, record the PID
    try:
        write_pid_file()
    except Exception as err:
        print(err)
#    if os.path.isfile(PID_FILE):
#        logging.error("PID already exists. Is getsolar already running? Remove %s if it is not", \
#            PID_FILE)
#        rm_pid_file()
#        sys.exit(1)

    m_d = mqtt.Client(MQTT_CLIENT_NAME)
    m_d.username_pw_set(MQTT_USER, mqtt_password)
    m_d.connect(MQTT_HOST, int(MQTT_PORT))
    m_d.loop_start()

    d_d = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, INFLUX_USER, influx_password, INFLUX_DB_ALL)
    d_p = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, INFLUX_USER, influx_password, INFLUX_DB_POWER)

    # Setup 'last' energy counters

    result = d_d.query('select sum(Production) as Production,sum(Export) as Export ,sum(Import) as Import from Wh')
    logging.debug("Result: %s", result.raw)
    points = result.get_points()
    for point in points:
        energy_last['production'] = point['Production']
        energy_last['export'] = point['Export']
        energy_last['import'] = point['Import']

    cycle = 0
    retry = MAX_RETRIES
    logging.info("Modbus: Opening client")
    if args.t == 'tcp':
        try:
            s_d = solaredge_modbus.Inverter(host=args.i, port=args.P)
        except Exception as err:
            logging.debug("Opening")
            logging.debug(Exception)
            logging.debug(err)
    elif args.t == 'rtu':
        s_d = solaredge_modbus.Inverter(device=args.p, baud=args.b)
    else:
        logging.critical('Unknown -t option: %s', args.t)
        rm_pid_file()
        sys.exit(1)
    while retry != 0:
        if s_d.connected():
            retry = MAX_RETRIES
            # Read registers
            logging.debug("Running in debug mode, not writing data")
            logging.debug("Reading data - cycle %s", cycle)
#            energy_last['production'], energy_last['export'], energy_last['import'] = \
#                get_solaredge(s_d, m_d, d_d, d_p, cycle, energy_last, debug)
            cycle += 1
            if cycle > 5:
                cycle = 0
            #logging.info("Sunspec: Closing client")
            # Close the connection (TODO)
            #s_d.close()
            time.sleep(SLEEP_TIME)
        else:
            logging.info('Connect failed - retrying')
            retry -= 1
            time.sleep(WAIT_TIME)
            logging.info("Modbus: Opening client")
            if args.t == 'tcp':
                try:
                    s_d = solaredge_modbus.Inverter(host=args.i, port=args.P)
                except Exception as err:
                    logging.debug("Opening")
                    logging.debug(Exception)
                    logging.debug(err)
            elif args.t == 'rtu':
                s_d = solaredge_modbus.Inverter(device=args.p, baud=args.b)
            else:
                logging.critical('Unknown -t option: %s', args.t)
                rm_pid_file()
                sys.exit(1)
    logging.error("Too many retries")
    rm_pid_file()
    sys.exit(2)


if __name__ == "__main__":
    main()
