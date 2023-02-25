import random
import binascii

class Crypt:
    
    def __init__(self, akey = "Yd#s%jdfsdfds@"):
        self.rng = random.Random(akey)

    def crypt(self, astring):
        rand = self.rng.randrange
        crypted = [chr(ord(elem)^rand(256)) for elem in astring]
        return ''.join(crypted)


def strtohex(astring):
        hexlist = ["%02X " % ord(x) for x in astring]
        return ''.join(hexlist)
    
if __name__ == "__main__":
    key = "Yd#s%jdfsdfds@"
    #test_string = raw_input("Enter a string: ")

    #testStr = Crypt(key).crypt(test_string)
    #print "Encrypted string: '%s'"%testStr
    #print "Hex    : '%s'"%binascii.hexlify(testStr)
    #print binascii.hexlify(testStr)
    #a= binascii.hexlify(testStr)
#     test_Str = Crypt(key).crypt(testStr)
#     print "Decrypted string: '%s'"% test_Str
#    print "Decrypted string: '%s'"% Crypt(key).crypt(binascii.unhexlify(a))
