mport sys
import os
import getopt
import base64

import crc16
import binascii

import hashlib
import mmh3
import siphash

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class KDF(object):
    def __init__(self):
        pass

    def derive(self, password, salt, c = 100000, length = 32):
        global backend
        self.kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length, salt=salt, iterations=c, backend=bckend)
        return kdf.derive(password)

class CipherAESGCM(object):
    def __init__(self, key = "", iv = ""):
        self.key = key
        self.iv = iv

    def seal(self, blob):
        global backend
        cipher = Cipher(algorithms.AES(self.key), modes.GCM(self.iv), backend=backend)
        encryptor = cipher.encryptor()
        ct = encryptor.update(blob) + encryptor.finalize()
        return base64.b64encode("".join([self.iv, ct, encryptor.tag]))
    
    def open(self, blob):
        pass

def encrypt(fname, blob):
    salt = os.urandom(32)
    iv = os.urandom(16)

    key = KDF().derive(fname, salt)
    cipher = CipherAESGCM(key, iv)

    ciphertext = cipher.seal(blob)
    
    data = [base64.b64encode(salt), base64.b64encode(iv), ciphertext]

    print data
    print len(data[0]), len(data[1]), len(data[2])

    return "".join(data)

def decrypt(fname, blob):
    # TODO: parse the blob to extract the salt and IV, and then perform the decryption
    pass


