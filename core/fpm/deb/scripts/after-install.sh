#!/bin/bash

EF_BIN_DIRECTORY="/usr/share/elefana"
EF_CONF_DIRECTORY="/etc/elefana"
EF_CONF_FILE="$EF_CONF_DIRECTORY/application.properties"
EF_CONF_SAMPLE_FILE="$EF_CONF_DIRECTORY/application.properties.sample"
EF_DEFAULTS_FILE="/etc/default/elefana"
EF_LOG_DIRECTORY="/var/log/elefana"

cp /usr/share/elefana/fpm/deb/init.d/elefana /etc/init.d/elefana
chown root:root /etc/init.d/elefana
chmod +x /etc/init.d/elefana

if [ ! -f $EF_DEFAULTS_FILE ]; then
    cp /usr/share/elefana/fpm/deb/default/elefana $EF_DEFAULTS_FILE
    chown root:root $EF_DEFAULTS_FILE
fi

if [ ! -f $EF_CONF_FILE ]; then
    cp /usr/share/elefana/src/main/resources/application.properties $EF_CONF_FILE
else
    cp /usr/share/elefana/src/main/resources/application.properties $EF_CONF_SAMPLE_FILE
fi

mv $EF_BIN_DIRECTORY/build/release/* $EF_BIN_DIRECTORY/
rm -rf $EF_BIN_DIRECTORY/build

chown -R elefana:elefana $EF_BIN_DIRECTORY
chown -R elefana:elefana $EF_CONF_DIRECTORY
chown -R elefana:elefana $EF_LOG_DIRECTORY

systemctl daemon-reload
update-rc.d elefana defaults
