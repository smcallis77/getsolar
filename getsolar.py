#!/usr/bin/env python3

# getsolar.py v1.2.1 30-July-2020

from keyrings.alt.file import PlaintextKeyring
import keyring.backend
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
import solaredge_modbus
import argparse
import json
import syslog
import logging
import time
import datetime
import os
import sys
VERSION = 'v1.3.0'

"""
  Copyright (c) 2018, Steve McAllister
  All Rights Reserved

Connects to a solaredge inverter extracts data and writes to two influx databases.

Database 1 - homeassistant(HA) DB writes important sensor ENERGY data for homeassistant
             approximately once per minute
Database 2 - powerlogging DB - writes a smaller amount of POWER data more frequently
             for analysis of 'AC Load'

Changelog:

V1.3.0
    Prior to v1.3.0 energy was written to the database as energy generated between this data point and the last data point. Energy is now written as
    total lifetime energy. 

v1.2 - update code to comply with pylint coding standards

  options:
      -t: transport type: tcp or rtu (default: tcp)
      -i: ip address to use for modbus tcp (default: localhost)
      -P: port number for modbus tcp (default: 502)
      -p: serial port for modbus rtu (default: /dev/ttyUSB0)
      -b: baud rate for modbus rtu (default: 9600)
      -D: debug mode (do not read any data)

Solaredge Register Details
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

"""


MQTT_CLIENT_NAME = "getsolar.192.168.20.2"
MQTT_HOST = "ha.smcallister.org"
MQTT_PORT = "1883"
MQTT_USER = "homecontrol"
AUTODISCOVERY_PREFIX = "homeassistant"
POWER_TOPIC = "house/solaredge/power/production"
EXPORT_TOPIC = "house/solaredge/power/export"
IMPORT_TOPIC = "house/solaredge/power/import"
LOAD_TOPIC = "house/solaredge/power/load"
INVERTER_TOPIC = "house/solaredge/inverter/state"
METER_TOPIC = "house/solaredge/meter/state"

# Initialise Influxdb data object
INFLUX_USER = 'telegraf'
INFLUX_DB_ALL = 'solar'
INFLUX_DB_POWER = 'solar'
INFLUX_HOST = 'ha.smcallister.org'
INFLUX_PORT = 8086
INFLUX_DOMAIN = 'solaredge'
INFLUX_ENTITY = 'meters'
INFLUX_PASSWORD = ''

# Initialise syslog settings

_ID = 'getsolar ' + VERSION
LOG_FACILITY_LOCAL_N = 1

# Initialise globals

SLEEP_TIME = 10
WAIT_TIME = 1
MAX_RETRIES = 5
MAX_COUNTER = 5
#PID_FILE = '/var/run/getsolar/getsolar.pid'
DEBUG = False


class SysLogLibHandler(logging.Handler):
    """A logging handler that emits messages to syslog.syslog."""

    # pylint: disable=broad-except
    # broad exception is reasonable in this case as

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
        except Exception:
            try:
                syslog.openlog(syslog.LOG_PID, self.FACILITY[n])
            except Exception:
                try:
                    syslog.openlog('my_IDent', syslog.LOG_PID,
                                   self.FACILITY[n])
                except:
                    raise
        # We got it
        logging.Handler.__init__(self)

    def emit(self, record):
        syslog.syslog(self.format(record))


class InverterData():
    """
    This class is used to hold data read from the inverter
    """
    # pylint: disable=too-many-instance-attributes
    # Eleven is reasonable in this case.

    def __init__(self):

        self.new = True
        self.timestamp = ""
        self.power = {
            "prod": 0.0,
            "imp": 0.0,
            "exp": 0.0,
            "load": 0.0
        }
#        self.power_prod = 0.0
#        self.power_imp = 0.0
#        self.power_exp = 0.0
#        self.power_load = 0.0
        self.energy = {
            "prod": 0.0,
            "imp": 0.0,
            "exp": 0.0,
            "cons": 0.0,
            "s-cons": 0.0
        }
#        self.energy_prod_delta = 0.0
#        self.energy_imp_delta = 0.0
#        self.energy_exp_delta = 0.0

        self.inv_data = {}
        self.meter_data = {}
        d_d = InfluxDBClient(INFLUX_HOST, INFLUX_PORT,
                             INFLUX_USER, INFLUX_PASSWORD, INFLUX_DB_ALL)
        # Setup 'last' energy counters
        # Energy data over an interval = current data - last recorded data

#        result = d_d.query(
#            'select sum(Production) as Production,sum(Export) as Export ,sum(Import) as Import from Wh')
#        logging.debug("Result: %s", result.raw)
#        points = result.get_points()
#        for point in points:
#            self.energy_prod = point['Production']
#            self.energy_exp = point['Export']
#            self.energy_imp = point['Import']

    def update(self, s_d):
        """
        Read inverter registers
        """
        # pylint: disable=broad-except
        # broad exception is reasonable in this case as exceptions are not inherited from the class pymodbus
        # by solaredge_modbus

        retry = MAX_RETRIES

        while retry > 0:
            logging.debug("Trying. Retry= %s", retry)
            try:
                self.inv_data = s_d.read_all()
                meter1 = s_d.meters()["Meter1"]
                self.meter_data = meter1.read_all()

            except Exception:
                # Retry on read exception
                logging.warning("Register read error - retrying")
                retry -= 1
                time.sleep(WAIT_TIME)
            else:
                retry = 0

                # Update power data

                self.power["prod"] = float(
                    self.inv_data['power_ac']*10**self.inv_data['power_ac_scale'])
                if self.meter_data['power'] > 0:
                    self.power["exp"] = float(
                        self.meter_data['power']*10**self.meter_data['power_scale'])
                    self.power["imp"] = 0.0
                else:
                    self.power["imp"] = float(
                        -1.0*self.meter_data['power']*10**self.meter_data['power_scale'])
                    self.power["exp"] = 0.0
                self.power["load"] = float(
                    self.power["prod"]-self.power["exp"]+self.power["imp"])
                self.timestamp = time.strftime(
                    '%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                logging.debug('Timestamp: %s', self.timestamp)

                # Update energy data

                self.energy["prod"] = \
                    float(self.inv_data['energy_total']*10 **
                          self.inv_data['energy_total_scale'])
                self.energy["imp"] =  \
                    float(self.meter_data['import_energy_active']
                          * 10**self.meter_data['energy_active_scale'])
                self.energy["exp"] =  \
                    float(self.meter_data['export_energy_active']
                          * 10**self.meter_data['energy_active_scale'])

    def write_ha(self, mqtt_ha, influx_ha):
        """
        Writes power and energy utilisation data to the Home Assistant database
        """
        self.energy["cons"] = float(
            self.energy["prod"]-self.energy["exp"]+self.energy["imp"])
        self.energy["s-cons"] = float(
            self.energy["prod"]-self.energy["exp"])
        # Write energy values to influx
        influx_measure = 'Wh'
        influx_metric = [{
            'measurement': influx_measure,
            'time': self.timestamp,
            'tags': {
                'domain': INFLUX_DOMAIN,
                'entity_id': INFLUX_ENTITY
            },
            'fields': {
                'Production': self.energy["prod"],
                'Import': self.energy["imp"],
                'Export': self.energy["exp"],
                'Consumption': self.energy["cons"],
                'Self-Consumption': self.energy["s-cons"]
            }
        }]
        # Decode inverter status
        self.inv_data['status'] = solaredge_modbus.INVERTER_STATUS_MAP[self.inv_data['status']]
        if not DEBUG:

            if self.new == True:
                #       Write inverter discovery topic to Home Assistant
                inverterUniqueIDPrefix = self.inv_data["c_model"] + \
                    "-" + self.inv_data["c_serialnumber"]

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_cur_A" + "/" + "config"
                inverterPayload = {
                    "device": {
                        "identifiers": [inverterUniqueIDPrefix],
                        "manufacturer": "Solaredge",
                        "model": self.inv_data["c_model"],
                        "name": "Solaredge Inverter",
                        "sw_version": self.inv_data["c_version"]
                    },
                    "icon": "mdi:signal",
                    "name": "AC Cur A",
                    "state_topic": INVERTER_TOPIC,
                    "unique_id": inverterUniqueIDPrefix + "_AC_cur_A",
                    "value_template": "{{ (value_json.l1_current * 10 ** value_json.current_scale)|round(2) }}",
                    "unit_of_measurement": "A",
                    "icon": "mdi:current-ac",
                    "platform": "mqtt"
                }
                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_cur_B" + "/" + "config"
                inverterPayload["name"] = "AC Cur B"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + "_AC_cur_B"
                inverterPayload["value_template"] = "{{ (value_json.l2_current * 10 ** value_json.current_scale)|round(2) }}"
                inverterPayload["unit_of_measurement"] = "A"
                inverterPayload["icon"] = "mdi:current-ac"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_cur_C" + "/" + "config"
                inverterPayload["name"] = "AC Cur C"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + "_AC_cur_C"
                inverterPayload["value_template"] = "{{ (value_json.l3_current * 10 ** value_json.current_scale)|round(2) }}"
                inverterPayload["unit_of_measurement"] = "A"
                inverterPayload["icon"] = "mdi:current-ac"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_cur" + "/" + "config"
                inverterPayload["name"] = "AC Cur"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + "_AC_cur"
                inverterPayload["value_template"] = "{{ (value_json.current * 10 ** value_json.current_scale)|round(2) }}"
                inverterPayload["unit_of_measurement"] = "A"
                inverterPayload["icon"] = "mdi:current-ac"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_Energy" + "/" + "config"
                inverterPayload["name"] = "AC Energy"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_Energy"
                inverterPayload["value_template"] = "{{ (value_json.energy_total * 10 ** (value_json.energy_total_scale-6))|round(3) }}"
                inverterPayload["unit_of_measurement"] = "MWh"
                inverterPayload["icon"] = "mdi:electron-framework"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_Freq" + "/" + "config"
                inverterPayload["name"] = "AC Frequency"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_Freq"
                inverterPayload["value_template"] = "{{ (value_json.frequency * 10 ** value_json.frequency_scale)|round(2) }}"
                inverterPayload["unit_of_measurement"] = "Hz"
                inverterPayload["icon"] = "mdi:sine-wave"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_PF" + "/" + "config"
                inverterPayload["name"] = "AC Power Factor"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_PF"
                inverterPayload["value_template"] = "{{ (value_json.power_factor * 10 ** value_json.power_factor_scale)|round(2) }}"
                inverterPayload["unit_of_measurement"] = "%"
                inverterPayload["icon"] = "mdi:percent"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_Power" + "/" + "config"
                inverterPayload["name"] = "AC Power"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_Power"
                inverterPayload["value_template"] = "{{ (value_json.power_ac * 10 ** (value_json.power_ac_scale-3))|round(3) }}"
                inverterPayload["unit_of_measurement"] = "KW"
                inverterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_VA" + "/" + "config"
                inverterPayload["name"] = "AC VA"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_VA"
                inverterPayload["value_template"] = "{{ (value_json.power_apparent * 10 ** (value_json.power_apparent_scale-3))|round(3) }}"
                inverterPayload["unit_of_measurement"] = "KVA"
                inverterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_VAR" + "/" + "config"
                inverterPayload["name"] = "AC VAR"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_VAR"
                inverterPayload["value_template"] = "{{ (value_json.power_reactive * 10 ** (value_json.power_reactive_scale-3))|round(3) }}"
                inverterPayload["unit_of_measurement"] = "KVA"
                inverterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_AC_Voltage" + "/" + "config"
                inverterPayload["name"] = "AC Voltage"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_AC_Voltage"
                inverterPayload["value_template"] = "{{ (value_json.l1_voltage * 10 ** (value_json.voltage_scale))|round(2) }}"
                inverterPayload["unit_of_measurement"] = "V"
                inverterPayload["icon"] = "mdi:power-socket-au"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_DC_Current" + "/" + "config"
                inverterPayload["name"] = "DC Current"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_DC_Current"
                inverterPayload["value_template"] = "{{ (value_json.current_dc * 10 ** (value_json.current_dc_scale))|round(2) }}"
                inverterPayload["unit_of_measurement"] = "A"
                inverterPayload["icon"] = "mdi:current-dc"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_DC_Power" + "/" + "config"
                inverterPayload["name"] = "DC Power"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_DC_Power"
                inverterPayload["value_template"] = "{{ (value_json.power_dc * 10 ** (value_json.power_dc_scale-3))|round(2) }}"
                inverterPayload["unit_of_measurement"] = "KW"
                inverterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_DC_Voltage" + "/" + "config"
                inverterPayload["name"] = "DC Voltage"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_DC_Voltage"
                inverterPayload["value_template"] = "{{ (value_json.voltage_dc * 10 ** (value_json.voltage_dc_scale))|round(2) }}"
                inverterPayload["unit_of_measurement"] = "V"
                inverterPayload["icon"] = "mdi:power-socket-au"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_Temperature" + "/" + "config"
                inverterPayload["name"] = "Inverter Temperature"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_Temperature"
                inverterPayload["value_template"] = "{{ (value_json.temperature * 10 ** (value_json.temperature_scale))|round(2) }}"
                inverterPayload["unit_of_measurement"] = "°C"
                inverterPayload["icon"] = "mdi:thermometer"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_Status" + "/" + "config"
                inverterPayload["name"] = "Inverter Status"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_Status"
                inverterPayload["value_template"] = "{{ (value_json.status) }}"
                inverterPayload["unit_of_measurement"] = ""
                inverterPayload["icon"] = "mdi:star-three-points"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + inverterUniqueIDPrefix + "_Vendor_Status" + "/" + "config"
                inverterPayload["name"] = "Inverter Vendor Status"
                inverterPayload["unique_id"] = inverterUniqueIDPrefix + \
                    "_Vendor_Status"
                inverterPayload["value_template"] = "{{ (value_json.vendor_status) }}"
                inverterPayload["unit_of_measurement"] = ""
                inverterPayload["icon"] = "mdi:star-three-points"

                mqtt_ha.publish(discoveryTopic, json.dumps(inverterPayload))

                #       Write meter discovery topic to Home Assistant
                meterUniqueIDPrefix = self.meter_data["c_model"] + \
                    "-" + self.meter_data["c_serialnumber"]

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_load" + "/" + "config"

                meterPayload = {
                    "device": {
                        "identifiers": [meterUniqueIDPrefix],
                        "manufacturer": "Solaredge",
                        "model": self.meter_data["c_model"],
                        "name": "Solaredge Meter",
                        "sw_version": self.meter_data["c_version"]
                    },
                    "icon": "mdi:solar-power",
                    "name": "Meter Load",
                    "state_topic": LOAD_TOPIC,
                    "unique_id": meterUniqueIDPrefix + "_load",
                    "unit_of_measurement": "kW",
                    "platform": "mqtt"
                }

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_production" + "/" + "config"
                meterPayload["name"] = "Meter Production"
                meterPayload["state_topic"] = POWER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + "_production"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_import" + "/" + "config"
                meterPayload["name"] = "Meter Import"
                meterPayload["state_topic"] = IMPORT_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + "_import"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_export" + "/" + "config"
                meterPayload["name"] = "Meter Export"
                meterPayload["state_topic"] = EXPORT_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + "_export"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_current" + "/" + "config"
                meterPayload["name"] = "Meter Current"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + "_current"
                meterPayload["value_template"] = "{{ (value_json.current * 10 ** value_json.current_scale)|round(2) }}"
                meterPayload["unit_of_measurement"] = "A"
                meterPayload["icon"] = "mdi:current-ac"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_line_voltage" + "/" + "config"
                meterPayload["name"] = "Meter Line Voltage"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_line_voltage"
                meterPayload["value_template"] = "{{ (value_json.voltage_ln * 10 ** (value_json.voltage_scale))|round(2) }}"
                meterPayload["unit_of_measurement"] = "V"
                meterPayload["icon"] = "mdi:power-socket-au"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_frequency" + "/" + "config"
                meterPayload["name"] = "Meter Frequency"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_frequency"
                meterPayload["value_template"] = "{{ (value_json.frequency * 10 ** value_json.frequency_scale)|round(2) }}"
                meterPayload["unit_of_measurement"] = "Hz"
                meterPayload["icon"] = "mdi:sine-wave"

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_real_power" + "/" + "config"
                meterPayload["name"] = "Meter Real Power"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_frequency"
                meterPayload["value_template"] = "{{ (value_json.power * 10 ** (value_json.power_scale))|round(3) }}"
                meterPayload["unit_of_measurement"] = "W"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_power_apparent" + "/" + "config"
                meterPayload["name"] = "Meter Apparent Power"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_power_apparent"
                meterPayload["value_template"] = "{{ (value_json.power_apparent * 10 ** (value_json.power_apparent_scale))|round(3) }}"
                meterPayload["unit_of_measurement"] = "VA"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_power_reactive" + "/" + "config"
                meterPayload["name"] = "Meter Reactive Power"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_power_reactive"
                meterPayload["value_template"] = "{{ (value_json.power_reactive * 10 ** (value_json.power_reactive_scale))|round(3) }}"
                meterPayload["unit_of_measurement"] = "VA"
                meterPayload["icon"] = "mdi:solar-power"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_power_factor" + "/" + "config"
                meterPayload["name"] = "Meter Power Factor"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_power_factor"
                meterPayload["value_template"] = "{{ (value_json.power_factor * 10 ** value_json.power_factor_scale)|round(2) }}"
                meterPayload["unit_of_measurement"] = "%"
                meterPayload["icon"] = "mdi:percent"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_lifetime_energy_export" + "/" + "config"
                meterPayload["name"] = "Meter Lifetime Energy Export"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_lifetime_energy_export"
                meterPayload["value_template"] = "{{ (value_json.export_energy_active * 10 ** (value_json.energy_active_scale-6))|round(3) }}"
                meterPayload["unit_of_measurement"] = "MWh"
                meterPayload["icon"] = "mdi:electron-framework"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                discoveryTopic = AUTODISCOVERY_PREFIX + "/" + \
                    "sensor" + "/" + meterUniqueIDPrefix + "_lifetime_energy_import" + "/" + "config"
                meterPayload["name"] = "Meter Lifetime Energy Import"
                meterPayload["state_topic"] = METER_TOPIC
                meterPayload["unique_id"] = meterUniqueIDPrefix + \
                    "_lifetime_energy_import"
                meterPayload["value_template"] = "{{ (value_json.import_energy_active * 10 ** (value_json.energy_active_scale-6))|round(3) }}"
                meterPayload["unit_of_measurement"] = "MWh"
                meterPayload["icon"] = "mdi:electron-framework"

                mqtt_ha.publish(discoveryTopic, json.dumps(meterPayload))

                self.new = False

            logging.debug("Writing energy points")
            mqtt_ha.publish(POWER_TOPIC, self.power["prod"]/1000)
            mqtt_ha.publish(EXPORT_TOPIC, self.power["exp"]/1000)
            mqtt_ha.publish(IMPORT_TOPIC, self.power["imp"]/1000)
            mqtt_ha.publish(LOAD_TOPIC, self.power["load"]/1000)
            mqtt_ha.publish(INVERTER_TOPIC, json.dumps(self.inv_data))
            mqtt_ha.publish(METER_TOPIC, json.dumps(self.meter_data))

            influx_ha.write_points(influx_metric, time_precision='s')

        else:
            logging.debug(
                "Energy  - Production: %s, Export: %s, Import: %s, Consumption: %s, Self Consumption: %s",
                self.energy["prod"],
                self.energy["exp"],
                self.energy["imp"],
                self.energy["cons"],
                self.energy["s-cons"])
        # reset energy delta
#        self.energy_prod = self.energy_prod+self.energy_prod_delta
#        self.energy_imp = self.energy_imp+self.energy_imp_delta
#        self.energy_exp = self.energy_exp+self.energy_exp_delta

    def write_power(self, influx_pw):
        """
        Writes power utilisation data to the powerlogging database
        """
        # Write power values to influx
        influx_measure = 'W'
        influx_metric = [{
            'measurement': influx_measure,
            'time': self.timestamp,
            'tags': {
                'domain': INFLUX_DOMAIN,
                'entity_id': INFLUX_ENTITY
            },
            'fields': {
                'Production': self.power["prod"],
                'Import': self.power["imp"],
                'Export': self.power["exp"],
                'Load': self.power["load"]
            }
        }]
        if not DEBUG:
            logging.debug("Writing power points")
            influx_pw.write_points(influx_metric, time_precision='s')
        else:
            # Print published values to log
            logging.debug("Power - Production: %s, Export: %s, Import: %s, Load: %s",
                          self.power["prod"], self.power["exp"], self.power["imp"], self.power["load"])


def write_pid_file(pid_f):
    """
    Writes a file containing the current process id
    """
    pid = str(os.getpid())
    _f = open(pid_f, 'w')
    _f.write(pid)
    _f.close()


def rm_pid_file(pid_f):
    """
    Deletes the file containing the current process id
    """
    if not DEBUG:
        if os.path.exists(pid_f):
            os.remove(pid_f)


def parse_args():
    """
        configure valid arguments
    """

    parser = argparse.ArgumentParser(
        description='Get solar performance data from a solaredge inverter')
    parser.add_argument('-i', metavar=' ',
                        default='localhost',
                        help='ip address to use for modbus tcp [default: localhost]')
    parser.add_argument('-p', metavar=' ', type=int,
                        default=502,
                        help='port number for modbus tcp [default: 502]')
    parser.add_argument('-t', metavar=' ', type=int,
                        default=1,
                        help='connection timeout [default: 1]')
    parser.add_argument('-u', metavar=' ', type=int,
                        default=1,
                        help='modbus unit [default: 1]')
    parser.add_argument('-D', action="store_true",
                        help='run in debug mode')
    return parser.parse_args()


def set_logging(log_str):
    """
    Configures the log level, and log format and sets up the logging handlers
    """

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
    if not DEBUG:
        handlers.append(SysLogLibHandler(LOG_FACILITY_LOCAL_N))
        for handle in handlers:
            handle.setFormatter(formatter)
            logger.addHandler(handle)


def on_connect(client, userdata, flags, rc):
    # client.error_code = rc
    if rc == 0:
        client.connected_flag = True  # set flag
    else:
        client.connected_flag = False


def on_disconnect(client, userdata, rc):
    logging.info("disconnecting reason  " + str(rc))
    client.connected_flag = False
    client.disconnect_flag = True


def on_log(client, userdata, level, buf):
    logging.debug("log: " + buf)


def main():
    """
    Main processing loop
    """

    # pylint: disable=global-statement
    # use of global statement here is required to allow main() to set the value based on passed arguments to the program

    global DEBUG, INFLUX_PASSWORD

    # Get the passwords from the plain text keyring
    keyring.set_keyring(PlaintextKeyring())
    mqtt_password = keyring.get_password(MQTT_HOST, MQTT_USER)
    INFLUX_PASSWORD = keyring.get_password(INFLUX_HOST, INFLUX_USER)

    try:
        pid_file = os.environ['PIDFILE']
    except:
        pid_file = "null"

    args = parse_args()

    # Setup logging

    if args.D:
        DEBUG = True
        set_logging('debug')
        logging.debug("Running in debug mode, not writing data")
    else:
        DEBUG = False
        set_logging('info')
        if os.path.exists(pid_file):
            logging.error("PID already exists. Is getsolar already running?")
            logging.error(
                "Either, stop the running process or remove %s or run with the debug flag set (-D)", pid_file)
            sys.exit(2)
        else:
            write_pid_file(pid_file)

    # Connect to MQTT

    m_d = mqtt.Client(MQTT_CLIENT_NAME)
    m_d.connected_flag = False
    m_d.error_code = 0
    m_d.on_connect = on_connect  # bind call back function
    m_d.on_disconnect = on_disconnect
    m_d.on_log = on_log
    m_d.username_pw_set(MQTT_USER, mqtt_password)
    m_d.connect(MQTT_HOST, int(MQTT_PORT))
    m_d.loop_start()

    retry = MAX_RETRIES
    while not m_d.connected_flag:
        if retry == 0:
            # wait in loop for MAX_RETRIES
            sys.exit("Connect failed with error", m_d.error_code)
        else:
            if m_d.error_code == 5:
                sys.exit("Authorisation Failure" + mqtt_password)
            time.sleep(1)
            retry -= 1

    # Connect to two InfluxDB databases
    #   DB 1 = Home Assistant database for one minute logging of power and energy data
    #   DB 2 = Powerlogging for 10s logging of power only

    d_d = InfluxDBClient(INFLUX_HOST, INFLUX_PORT,
                         INFLUX_USER, INFLUX_PASSWORD, INFLUX_DB_ALL)
    d_p = InfluxDBClient(INFLUX_HOST, INFLUX_PORT,
                         INFLUX_USER, INFLUX_PASSWORD, INFLUX_DB_POWER)

    inv_data = InverterData()

    # Initialise cycle counter and number of retries

    counter = MAX_COUNTER
    retry = MAX_RETRIES

    # Connect to solaredge modbus inverter

    logging.debug("Connect to device. Host " + args.i + " Port " +
                  str(args.p) + " Timeout " + str(args.t) + " Unit " + str(args.u))
    s_d = solaredge_modbus.Inverter(
        host=args.i, port=args.p, timeout=args.t, unit=args.u)
    # s_d.connect()

    # Try up to MAX_RETRIES times to read data from the inverter

    while retry != 0:
        if not s_d.connect():
            retry -= 1
            time.sleep(WAIT_TIME)
            logging.debug("Retry. Connect to device. Host " +
                          args.i + " Port " + str(args.p) + " Timeout " + str(args.t) + " Unit " + str(args.u))
            s_d = solaredge_modbus.Inverter(
                host=args.i, port=args.p, timeout=args.t, unit=args.u)
        else:
            waitSeconds = SLEEP_TIME - \
                (datetime.datetime.now().second % SLEEP_TIME)
            energyTime = int(datetime.datetime.now().second / SLEEP_TIME) + 1
            # logging.info("Sleeping for " + str(waitSeconds))
            time.sleep(waitSeconds)

            retry = MAX_RETRIES
            # Read registers
            logging.debug("Reading data - cycle %s", counter)
            inv_data.update(s_d)
            inv_data.write_power(d_p)
            if energyTime == 6:
                inv_data.write_ha(m_d, d_d)
    logging.error("Too many retries")
    rm_pid_file(pid_file)
    sys.exit(2)


if __name__ == "__main__":
    main()
