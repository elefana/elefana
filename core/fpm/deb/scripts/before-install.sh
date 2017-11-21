#!/bin/bash

EF_CONF_DIRECTORY="/etc/elefana"
EF_DATA_DIRECTORY="/var/elefana"
EF_LOG_DIRECTORY="/var/log/elefana"

getent group elefana || groupadd elefana
id -u elefana &>/dev/null || useradd -g elefana elefana

if [ ! -d $EF_CONF_DIRECTORY ]; then
    mkdir -p $EF_CONF_DIRECTORY
fi
chown elefana:elefana $EF_CONF_DIRECTORY
chmod 755 $EF_CONF_DIRECTORY

if [ ! -d $EF_DATA_DIRECTORY ]; then
    mkdir -p $EF_DATA_DIRECTORY
fi
chown elefana:elefana $EF_DATA_DIRECTORY
chmod 755 $EF_DATA_DIRECTORY

if [ ! -d $EF_LOG_DIRECTORY ]; then
    mkdir -p $EF_LOG_DIRECTORY
fi
chown elefana:elefana $EF_LOG_DIRECTORY
chmod 755 $EF_LOG_DIRECTORY
