#!/bin/bash
beempy updatenodes
cd /home/uncommon/project/nc/nc_backend
/usr/local/bin/python3.12 -u /home/uncommon/project/nc/nc_backend/process_transaction.py
