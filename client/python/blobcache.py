# Blobcache client library for Python
#
# This library provides a Python client for the Blobcache API.

import requests

class VolumeSpec:
    def __init__(self, hashAlgo: str):
        self.hashAlgo = hashAlgo

class Handle:
    def __init__(self, oid: str, secret: str):
        self.oid = oid
        self.secret = secret

# Client is used to connect to a Blobcache service..
class Client:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
 