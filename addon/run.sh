#!/usr/bin/with-contenv bashio

export PV_STATE_FILE=/data/pview-state.json
export RUST_BACKTRACE=full
export RUST_LOG_STYLE=always

if bashio::services.available mqtt ; then
  export PV_MQTT_HOST="$(bashio::services mqtt 'host')"
  export PV_MQTT_PORT="$(bashio::services mqtt 'port')"
  export PV_MQTT_USER="$(bashio::services mqtt 'username')"
  export PV_MQTT_PASSWORD="$(bashio::services mqtt 'password')"
fi

if bashio::config.has_value hub_ip ; then
  export PV_HUB_IP="$(bashio::config hub_ip)"
fi

if bashio::config.has_value mqtt_host ; then
  export PV_MQTT_HOST="$(bashio::config mqtt_host)"
fi


if bashio::config.has_value mqtt_port ; then
  export PV_MQTT_PORT="$(bashio::config mqtt_port)"
fi

if bashio::config.has_value mqtt_username ; then
  export PV_MQTT_USER="$(bashio::config mqtt_username)"
fi

if bashio::config.has_value mqtt_password ; then
  export PV_MQTT_PASSWORD="$(bashio::config mqtt_password)"
fi

if bashio::config.has_value debug_level ; then
  # Accept a bare level ("trace") or a full filter ("pview=trace").
  level="$(bashio::config debug_level)"
  case "$level" in
    *=*) export RUST_LOG="$level" ;;
    *) export RUST_LOG="pview=$level" ;;
  esac
fi

env | grep PV_ | sed -r 's/_(EMAIL|KEY|PASSWORD)=.*/_\1=REDACTED/'
set -x

exec /pview serve-mqtt
