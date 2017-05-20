#!/home/mghens/google-env/bin/python
# -*- coding: utf-8 -*-


import json
import sys
#import cx_Oracle
#~ import redis
import random
import copy
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.tools import run
from apiclient.discovery import build
import httplib2
from apiclient.http import BatchHttpRequest
from secrets import banHOST,banUSER,banPASS,banPORT,banSID
import log

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

def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]
    
    
def get_credentials(self):

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


def get_users():
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
                print('Please check your credentials.')
            else:
                print('Database connection error: %s'.format(e))
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
                print('Table already exists')
            elif error.code == 1031:
                print("Insufficient privileges")
            print(error.code)
            print(error.message)
            print(error.context)

            # Raise the exception.
            raise

        # Only commit if it-s necessary.
        if commit:
            self.db.commit()
 




credentials = get_credentials()
http = credentials.authorize(httplib2.Http())
service = build('admin','directory_v1', http=http)
service = discovery.build('admin', 'directory_v1', http=http)
print service._baseUrl

os.exit('Stop, we can work with google')


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
addressBlank = [addressDictBlank]




organizationDictionary = {"department": "Administrative Systems",
                 "primary": True,
                 "title": "Network Specialist III",
                  "name": "Santa Barbara City College",
                 "location": "Administration Building 210M",
                 "type": "work"}


organizationDictionaryBlank = {"department": None,
                               "primary": True,
                               "title": None,
                               "name": None,
                               "location": None,
                               "type": "work"}

organizationBlank = [organizationDictionaryBlank]

phoneBlank = [{'primary': True,
               'type': 'work',
               'value': None},
              ]


updatecontactBlank = {"addresses":addressBlank,
                 "organizations": organizationBlank,
                 "phones": phoneBlank
                 }


# Batch actions
def update_contact(request_id, response, exception):
    if exception is not None:
        #Just pass it. Don't care if error
        print 'ERROR: ',request_id, exception
        sys.stdout.flush()
        pass
    else:
        #Will get to it
        print 'SUCCESS: ',request_id, response['primaryEmail']
        sys.stdout.flush()
        pass
    

# gmailUsers = list(r_s.smembers('gusers'))


# Prep for batch
batch = BatchHttpRequest(callback=update_contact)


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

try:
    oracle = Oracle()
    oracle.connect(banUSER, banPASS, dsn_tns
                           , '1521', '')

    # No commit as you don-t need to commit DDL.
    
    directList = oracle.execute(sql)
    

# Ensure that we always disconnect from the database to avoid
# ORA-00018: Maximum number of sessions exceeded.
except Exception, e:
    print e
    sys.exit("Oracle Problems") 
finally:
    oracle.disconnect()

#get set of gmail users
#~ gmusers = list(r_s.smembers('gusers'))

#get gmail users who have profiles
#~ gmu_profile = list(r_s.smembers('gusers:profile'))


directDict = dict()
for i in directList:
    directDict[i['PLUSER']] = i

#print len(gmusers)  
directList = None
batchcount = 100
for i  in  directDict:
    userKey = i+'@pipeline.sbcc.edu'
    gmcontact = copy.deepcopy(updatecontactBlank)
    #print directDict[i]
    #print "GMCONTACT: ",gmcontact
    gmcontact['organizations'][0]['name'] = ''
    gmcontact['addresses'] = [addressDictEmployee]
    if directDict[i]['ROOM'] is not None:
        gmcontact['organizations'][0]['location'] = directDict[i]['ROOM']
    if directDict[i]['PHONE'] is not None:
        gmcontact['phones'][0]['value'] = directDict[i]['PHONE']
    else:
        gmcontact['phones']=[{'customType': '', 'type': 'custom', 'value': '' }]
#     print i+'@pipeline.sbcc.edu',directDict[i]
    if directDict[i]['DEPARTMENT'] is not None:
        gmcontact['organizations'][0]['department'] = directDict[i]['DEPARTMENT']
    if directDict[i]['JOB_TITLE'] is not None:
        gmcontact['organizations'][0]['title'] = directDict[i]['JOB_TITLE']
     #print gmcontact
    try:
        #~ gmusers.remove(i)
        batch.add(gmuser_service.patch(userKey=userKey,body=gmcontact))
        #~ r_s.sadd('gusers:profile',i)
        #~ gmu_profile.remove(i)
        batchcount -= 1
    except:
        pass
    
    print userKey,json.dumps(gmcontact)
 
    
    if batchcount == 0:
        batchcount = 100
        print "Send to Google"
        sys.stdout.flush()
        batch.execute(http=http)
        

    
#print len(gmusers)
#~ random.shuffle(gmusers)

#Remove profiles
#~ print "Removing: ", len(gmu_profile), "profiles"
#~ for i in gmu_profile:
    #~ userKey = i+'@pipeline.sbcc.edu'
    #~ gmcontact = {'addresses': [ {"type": "home"} ], 'organizations': [ None ], 'phones': [{'customType': '', 'type': 'custom', 'value': '' }],'organizations': [{'name': '', u'title': None, 'customType': '', 'location': None, 'department': None}]}
    #~ batch.add(gmuser_service.patch(userKey=userKey,body=gmcontact))
    #~ batchcount -= 1
    #~ r_s.srem('gusers:profile',i)
    #~ if batchcount == 0:
        #~ batchcount = 100
        #~ print "Send to Google"
        #~ sys.stdout.flush()
        #~ batch.execute(http=http)
        
if batchcount <> 0  and batchcount <> 100:
    print "Send to Google"
    sys.stdout.flush()
    batch.execute(http=http)   
    
print
#print directDict
print "All done!"
