#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

import os
import sys
import errno
import argparse
import time
import tempfile
import json
import stat

sys.path.append('/Users/cwood/PARC/Distillery/build/lib/python2.7/site-packages')
from CCNx import *

class CCNxClient(object):
    def __init__(self, async = False):
        print "Async? %d" % (async)
        self.portal = self.openAsyncPortal() if async else self.openPortal()
        print "Done."

    def setupIdentity(self):
        global IDENTITY_FILE
        IDENTITY_FILE = tempfile.NamedTemporaryFile(suffix=".p12")
        identity = create_pkcs12_keystore(IDENTITY_FILE.name, "foobar", "bletch", 1024, 10)
        return identity

    def openPortal(self):
        identity = self.setupIdentity()
        factory = PortalFactory(identity)
        portal = factory.create_portal()
        return portal

    def openAsyncPortal(self):
        identity = self.setupIdentity()
        factory = PortalFactory(identity)
        portal = factory.create_portal(transport=TransportType_RTA_Message, \
            attributes=PortalAttributes_NonBlocking)
        return portal

    def get(self, name, data):
        interest = Interest(Name(name))
        if data != None:
            interest.setPayload(data)

        self.portal.send(interest)
        response = self.portal.receive()

        if isinstance(response, ContentObject):
            return response.getPayload()
        else:
            return None

    def get_async(self, name, data, timeout_seconds):
        interest = None
        if data == None:
            interest = Interest(Name(name))
        else:
            interest = Interest(Name(name), payload=data)

        for i in range(timeout_seconds):
            try:
                self.portal.send(interest)
                response = self.portal.receive()
                if response and isinstance(response, ContentObject):
                    return response.getPayload()
            except Portal.CommunicationsError as x:
                if x.errno == errno.EAGAIN:
                    time.sleep(1)
                else:
                    raise
        return None

    def push(self, name, data):
        interest = Interest(Name(name), payload=data)
        try:
            self.portal.send(interest)
        except Portal.CommunicationsError as x:
            sys.stderr.write("ccnxPortal_Write failed: %d\n" % (x.errno,))
        pass

    def listen(self, prefix):
        try:
            self.portal.listen(Name(prefix))
        except Portal.CommunicationsError as x:
            sys.stderr.write("CCNxClient: comm error attempting to listen: %s\n" % (x.errno,))
        return True

    def receive(self):
        request = self.portal.receive()
        if isinstance(request, Interest):
            return str(request.name), request.getPayload()
        else:
            pass
        return None, None

    def receive_raw(self):
        request = self.portal.receive()
        if isinstance(request, Interest):
            return request.name, request.getPayload()
        else:
            pass
        return None, None

    def reply(self, name, data):
        try:
            self.portal.send(ContentObject(Name(name), data))
        except Portal.CommunicationsError as x:
            sys.stderr.write("reply failed: %d\n" % (x.errno,))

if __name__ == "__main__":
    client = CCNxClient()
