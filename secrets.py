### Universal secrets file
#Python2


#https://www.reddit.com/r/Python/comments/390z4z/is_it_possible_to_securely_store_actual_passwords/

import ConfigParser, os,base64

#Because encryption is too cool for SBCC
#~ from cryptography.fernet import Fernet

#~ config1 = ConfigParser.ConfigParser()
#~ config1.read(os.path.expanduser('~/.sbcc_keyfile'))
#~ config2 = ConfigParser.ConfigParser()
#~ config2.read(os.path.expanduser('~/config.ini'))

#~ crypto = Fernet(config1.get("key","key1"))

#~ def decrypt(cipher_text):
#~ return crypto.decrypt(cipher_text)
# Call using: 
# from secrets import banHOST,banUSER,banPASS,banPORT,banSID

config = ConfigParser.ConfigParser()

config.read(os.path.expanduser('~/config.ini'))
banHOST = config.get('banprod','banhost')
banUSER = base64.b64decode(config.get('banprod','banuser'))
banPASS = base64.b64decode(config.get('banprod','banpass'))
banPORT = config.get('banprod','banport')
banSID = config.get('banprod','bansid')
