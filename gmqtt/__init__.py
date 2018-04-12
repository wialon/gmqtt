import datetime

from .client import Client
from gmqtt.mqtt.protocol import BaseMQTTProtocol

__author__ = "Mikhail Turchunovich"
__email__ = 'mitu@gurtam.com'
__copyright__ = ("Copyright 2013-%d, Gurtam; " % datetime.datetime.now().year,)

__credits__ = [
    "Mikhail Turchunovich",
    "Elena Nikolaichik"
]
__version__ = "0.0.13"


__all__ = [
    'Client', 'BaseMQTTProtocol'
]
