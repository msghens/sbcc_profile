#!/home/mghens/google-env/bin/python
# -*- coding: utf-8 -*-

import pickle
import json
import sys,os
import cx_Oracle

import random
import copy
import oauth2client
from oauth2client import client
from oauth2client import tools
import httplib2
from apiclient import discovery,errors
from apiclient.http import BatchHttpRequest
from secrets import banHOST,banUSER,banPASS,banPORT,banSID
import log
import time
import timeit
from pprint import pprint

try:
       import argparse
       flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
       flags = None


logger = log.setup_custom_logger('root')


__author__ = "Michael Ghens"
__copyright__ = "Copyright 2013"
__credits__ = ["Michael Ghens"]
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Michael Ghens"
__email__ = "mghens@sbcc.edu"
__status__ = "Production"

#===============================================================================
# Python code to add some information to users profiles such as phone
# Department, Room
#===============================================================================

# Initializations


batchtosend = 150

# Quota is 1,500 per 100 seconds. Should 10 seconds, for cushion.
quotaDelay = 15.0 

start_time = timeit.default_timer()

def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]
    
    
def get_credentials():

		"""Gets valid user credentials from storage.

		If nothing has been stored, or if the stored credentials are invalid,
		the OAuth2 flow is completed to obtain the new credentials.

		Returns:
				Credentials, the obtained credential.

		"""
		#https://developers.google.com/admin-sdk/directory/v1/quickstart/python

		try:
				import argparse
				flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
				flags.noauth_local_webserver = True
		except ImportError:
				flags = None

		SCOPES = ['https://www.googleapis.com/auth/admin.directory.user',
				 'https://www.googleapis.com/auth/apps.groups.settings',
				 'https://www.googleapis.com/auth/admin.directory.group']




		
		APPLICATION_NAME = 'Santa Barbarbara City College Profile Update'

		home_dir = os.path.expanduser('~')
		credential_dir = os.path.join(home_dir, '.credentials')
		if not os.path.exists(credential_dir):
				os.makedirs(credential_dir)
		credential_path = os.path.join(credential_dir,'sbcc_profile.dat')
		CLIENT_SECRET_FILE = os.path.join(credential_dir,'sbcc_profile.json')
		store = oauth2client.file.Storage(credential_path)
		credentials = store.get()
		if not credentials or credentials.invalid:
				flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
				flow.user_agent = APPLICATION_NAME
				if flags:
						credentials = tools.run_flow(flow, store, flags)
				else:
						credentials = tools.run(flow, store)
		return credentials


def get_users(directory_service):
	all_users = []
	page_token = None
	params = {'customer': 'my_customer'}

	while True:
		try:
			if page_token:
				params['pageToken'] = page_token
			current_page = directory_service.users().list(**params).execute()
			all_users.extend(current_page['users'])
			page_token = current_page.get('nextPageToken')
				
			if not page_token:
					break
					
		except errors.HttpError as error:
			logger.error('An error occurred: %s' % str(error))
			break
	return all_users





class Oracle(object):

    def connect(self, username, password, hostname, port, servicename):
        """ Connect to the database. """

        try:
            dsn_tns = cx_Oracle.makedsn(hostname, port, servicename)
            self.db = cx_Oracle.connect(username, password, dsn_tns)
                                
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                logger.error('Please check your credentials.')
            else:
                logger.error('Database connection error: %s'.format(e))
                logger.error(dsn_tns)
            # Very important part!
            raise

        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.curs = self.db.cursor()

    def disconnect(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist we don't really care.
        """

        try:
            self.curs.close()
            self.db.close()
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=[], commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """
        
        try:
            self.curs.execute(sql, bindvars)
            return rows_to_dict_list(self.curs)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                logger.error('Table already exists')
            elif error.code == 1031:
                logger.error("Insufficient privileges")
            logger.error(error.code)
            logger.error(error.message)
            logger.error(error.context)

            # Raise the exception.
            raise

        # Only commit if it-s necessary.
        if commit:
            self.db.commit()
 

logger.info("Starting profile update process")

credentials = get_credentials()
http = credentials.authorize(httplib2.Http())


service = discovery.build('admin', 'directory_v1', http=http)
#~ print service._baseUrl
logger.info("Getting users from gmail")
lstartime = timeit.default_timer()


gmusers = get_users(service)
elapsed =  timeit.default_timer() - lstartime
logger.info("Time to get Google Users %f", elapsed)


with open("gmcache.p","wb") as f:
	pickle.dump(gmusers,f)
	


#~ with open("gmcache.p","rb") as f:
	#~ gmusers = pickle.load(f)

#~ for u in gmusers:
	#~ print u["primaryEmail"]



#~ sys.exit('Stop, we can work with google')




addressDictEmployee = {"country" : "UNITED STATES",
               "countryCode" : "US",
               "locality":"Santa Barbara",
               "postalCode": "93109",
               "primary":"true",
               "region":"CA",
               "streetAddress":"721 Cliff Drive",
               "type":"work"}

addressDictBlank = {"country" : None,
               "countryCode" : None,
               "locality": None,
               "postalCode": None,
               "primary": None,
               "region": None,
               "streetAddress": None,
               "type":"work"}
addressBlank = [{'type': 'home'}]




organizationDictionary = {"department": "Administrative Systems",
                 "primary": True,
                 "title": "Network Specialist III",
                  "name": "Santa Barbara City College",
                 "location": "Administration Building 210M",
                 "type": "work"}


organizationDictionaryBlank = [{'customType': '', 'name': ''}]

organizationBlank = [{'customType': '', 'name': ''}]

phoneBlank = [{'customType': '', 'type': 'custom', 'value': ''}]


updatecontactBlank = {"addresses":addressBlank,
                 "organizations": organizationBlank,
                 "phones": phoneBlank
                 }


# Batch actions
def update_contact(request_id, response, exception):
    if exception is not None:
        #Just pass it. Don't care if error
        logger.error( 'ERROR: %s %s',request_id, exception)
        pass
    else:
        #Will get to it
        logger.info('SUCCESS: %s %s',request_id, response['primaryEmail'])
        #~ sys.stdout.flush()
        pass
    




# Prep for batch
#~ batch = BatchHttpRequest(callback=update_contact)


sql = """
SELECT DISTINCT gobtpac_external_user              AS pluser,
  nbbposn_title                                    AS job_title,
  ftvorgn_title                                    AS department,
  room.addr_line3                                  AS room,
  sbcc_lib.gzf_get_tele (gobtpac_pidm,'SBCC' ,'P') AS phone
FROM NBBPOSN,
  NBRJOBS J,
  NBRBJOB,
  PEBEMPL,
  GOBTPAC,
  FTVORGN,
  TABLE ( CAST ( gzf_contact_info( gobtpac_pidm , 'SB','SBCC' ) AS sbcc_lib.gzyb_contact ) ) room
WHERE gobtpac_pidm                                = nbrjobs_pidm
AND nbbposn_title                                <> 'No Pay'
AND nbbposn_title                                <> '?'
AND nbrjobs_pidm                                  = nbrbjob_pidm
AND nbrjobs_pidm                                  = pebempl_pidm
AND nbbposn_posn                                  = nbrjobs_posn
AND nbrjobs_posn                                  = nbrbjob_posn
AND NBRJOBS_SUFF                                  = NBRBJOB_SUFF
AND pebempl_orgn_code_home                        = ftvorgn.ftvorgn_orgn_code(+)
AND ftvorgn_nchg_date                             > sysdate
AND nbrjobs_status                                = 'A'
AND NBRBJOB.NBRBJOB_CONTRACT_TYPE                 = 'P'
AND pebempl_empl_status                          <> 'T'
AND nbrjobs_status                               <> 'T'
AND NVL(TRUNC(nbrbjob_end_date), TRUNC(SYSDATE)) >= TRUNC(SYSDATE)
AND nbrjobs_effective_date                        =
  (SELECT MAX(nbrjobs_effective_date)
  FROM nbrjobs m
  WHERE m.nbrjobs_pidm                 = j.nbrjobs_pidm
  AND m.nbrjobs_posn                   = j.nbrjobs_posn
  AND m.nbrjobs_suff                   = j.nbrjobs_suff
  AND TRUNC(M.NBRJOBS_EFFECTIVE_DATE) <= TRUNC(SYSDATE)
  )
"""

logger.info("Getting users from banner")
try:
    oracle = Oracle()
    oracle.connect(banUSER,banPASS,banHOST,banPORT,banSID)

    # No commit as you don-t need to commit DDL.
    
    directList = oracle.execute(sql)
    

# Ensure that we always disconnect from the database to avoid
# ORA-00018: Maximum number of sessions exceeded.
except Exception, e:
    logger.error('%s', str(e))
    sys.exit("Oracle Problems") 
finally:
    oracle.disconnect()



directDict = dict()
for i in directList:
    directDict[i['PLUSER']] = i

#Memory constraints
directList = None
batchcount = batchtosend

#Initialize quota delay

startBatchRunTime = timeit.default_timer()
batch = service.new_batch_http_request(callback=update_contact)
for gmu in gmusers:
	#~ print gmu["primaryEmail"]
	pluser = gmu["primaryEmail"].split('@')[0]
	userKey = gmu["primaryEmail"]
	logger.debug("Checking: %s", userKey)
	if pluser in directDict:
		gmcontact = copy.deepcopy(updatecontactBlank)
		#print directDict[i]
		#print "GMCONTACT: ",gmcontact
		gmcontact['organizations'][0]['name'] = ''
		gmcontact['addresses'] = [addressDictEmployee]
		if directDict[pluser]['ROOM'] is not None:
			gmcontact['organizations'][0]['location'] = directDict[pluser]['ROOM']
		if directDict[pluser]['PHONE'] is not None:
			gmcontact['phones'][0]['value'] = directDict[pluser]['PHONE']
		else:
			gmcontact['phones']=[{'customType': '', 'type': 'custom', 'value': '' }]
	#     print i+'@pipeline.sbcc.edu',directDict[i]
		if directDict[pluser]['DEPARTMENT'] is not None:
			gmcontact['organizations'][0]['department'] = directDict[pluser]['DEPARTMENT']
		if directDict[pluser]['JOB_TITLE'] is not None:
			gmcontact['organizations'][0]['title'] = directDict[pluser]['JOB_TITLE']
		 #print gmcontact
		try:
			logger.info('Adding: %s to batch for profile update',userKey)
			batch.add(service.users().patch(userKey=userKey,body=gmcontact))
			batchcount -= 1
		except Exception as e:
			logger.error('%s', str(e))
			sys.exit("bad batch")
			pass
	else:
		gmcontact = copy.deepcopy(updatecontactBlank)
		needClear = False
		#Search list of dictionaries for key
		#~ pprint(gmu)
		#~ print(type(gmu))
		#~ if any('primary' in d for d in gmu["addresses"]):
		if 'addresses' in gmu:
			if any('primary' in d for d in gmu["addresses"]):
				needClear = True
		if 'organizations' in gmu:
			if any('primary' in d for d in gmu["organizations"]):
				needClear = True
		if 'phones' in gmu:
			if any('primary' in d for d in gmu["phones"]):
				needClear = True

		if needClear:
			#~ print("Clear Contact")
			try:
				logger.info('Adding: %s to batch for profile removal',userKey)
				batch.add(service.users().patch(userKey=userKey,body=gmcontact))
				batchcount -= 1
			except Exception as e:
				logger.error('%s', str(e))
				sys.exit("bad batch")
				pass
			
    
	if batchcount == 0:
		batchcount = batchtosend
		logger.info("Sending batch to Google")
		batch.execute(http=http)
		#Logic to prevent quota per 100 seconds overrun
		batchElapse =   timeit.default_timer() - startBatchRunTime
		if quotaDelay > batchElapse:
			logger.info("Quota Pause: %f seconds", quotaDelay - batchElapse)
			time.sleep(quotaDelay - batchElapse)
		else:
			logger.info("Quota Pause: 1 seconds")
			time.sleep(1)
		startBatchRunTime = timeit.default_timer()
		batch = service.new_batch_http_request(callback=update_contact)
        
        
if batchcount > 0:
    logger.info("Sending batch to Google")
    batch.execute(http=http)
    
    
logger.info("Time to process: %f",  timeit.default_timer() - start_time)    
logger.info("All done!")
