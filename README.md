[![Deb build Status](https://travis-ci.org/asteny/rabbitmq-policymaker.svg?branch=master)](https://travis-ci.org/asteny/rabbitmq-policymaker)[![Download](https://api.bintray.com/packages/asten/rabbitmq-tools/rabbitmq_policymaker/images/download.svg)](https://bintray.com/asten/rabbitmq-tools/rabbitmq-policymaker/_latestVersion)


rabbitmq-policymaker
===========

TODO: Write description

INSTALLATION
------------

pip
---
```bash
pip install git+https://github.com/asteny/rabbitmq-policymaker
```

Deb package for Ubuntu (16.04 - 18.04)
--------------------------------------

```bash
apt-get update
apt-get install gnupg2 apt-transport-https ca-certificates -y
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 379CE192D401AB61
echo "deb https://dl.bintray.com/asten/rabbitmq-tools xenial main" | tee -a /etc/apt/sources.list.d/rabbitmq-tools.list
apt-get update
apt-get install rabbitmq-policymaker -y

```
