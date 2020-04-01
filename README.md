rabbitmq-policymaker
===========
![Run tests](https://github.com/asteny/rabbitmq-policymaker/workflows/Run%20tests/badge.svg)

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
