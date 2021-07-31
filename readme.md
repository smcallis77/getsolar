This version replaces the sunspec library with the python module solaredge_modbus.

https://github.com/nmakel/solaredge_modbus

Installation instructions

as root

cp getsolar.service /lib/systemd/system/getsolar.service
cp getsolar /usr/local/sbin/getsolar
cp getsolar.py /usr/local/bin/getsolar.py

to run at startup

sudo systemctl enable getsolar

if 
