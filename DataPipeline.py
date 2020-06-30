import sys
import os
from datetime import date, datetime
import time
import pyTigerGraph as tg
import requests
import json
from configparser import ConfigParser
import lxml.html as lh
from lxml.html import fromstring
from kafka import KafkaProducer

import logging
# logging.basicConfig(level=logging.DEBUG)


# read the application ini file
Config = ConfigParser()
Config.read("DataPipeline.ini")

# read the application configuration items. ideally these are stored in an encrypted key-value store.
config_metadata_load_on_startup = Config.get('config_metadata', 'config_metadata_load_on_startup')
config_metadata_file_path = Config.get('config_metadata', 'config_metadata_file_path')
config_database_name = Config.get('config_database', 'config_database_name')
config_database_secret = Config.get('config_database', 'config_database_secret')
config_database_apitoken = Config.get('config_database', 'config_database_apitoken') or ""
config_database_apitoken_force_new = Config.get('config_database', 'config_database_apitoken_force_new') or ""
config_database_apitoken_port = Config.get('config_database', 'config_database_apitoken_port')
config_database_apitoken_expire_date = Config.get('config_database', 'config_database_apitoken_expire_date')
config_database_apitoken_lifetime = Config.get('config_database', 'config_database_apitoken_lifetime')
config_database_ip = Config.get('config_database', 'config_database_ip')
umls_api_key = Config.get('umls', 'umls_api_key')
umls_api_fetch_page_size = Config.get('umls', 'umls_api_fetch_page_size')
umls_api_fetch_page_count = Config.get('umls', 'umls_api_fetch_page_count')
umls_subsets_current_url = Config.get('umls', 'umls_subsets_current_url')
umls_auth_login_url = Config.get('umls', 'umls_auth_login_url')
umls_auth_service_url = Config.get('umls', 'umls_auth_service_url')
umls_auth_endpoint_url = Config.get('umls', 'umls_auth_endpoint_url')
kafka_bootstrap_servers = Config.get('kafka', 'kafka_bootstrap_servers')
kafka_api_version = Config.get('kafka', 'kafka_api_version')


# global variables
getconfig_api_endpoint = "{ip}:{port}/query/{graph}/GetConfig?c={ontology}"
datasets_to_load = ["SNOMEDCT_US"]

# the UmlsAuthentication class encapsulates the login needed to authenticate with the UMLS service.
# the following steps are necessary to consume UMLS data via REST API call:
# step 1: open a UMLS account and from your account profile section, obtain a UMLS API KEY.
# step 2: using an http POST to https://utslogin.nlm.nih.gov/cas/v1/api-key, Get a Ticket-Granting Ticket (TGT).
# You should include the key names and key values within the url-encoded body of the form in your POST call. for example:
# curl -X POST https://utslogin.nlm.nih.gov/cas/v1/api-key -H 'content-type: application/x-www-form-urlencoded' -d apikey={your_api_key_here}
# The TGT is valid for 8 hours. You do not need a new TGT for each REST API call.
# step 3: Get a Service Ticket. A Service Ticket expires after one use or five minutes from the time of generation, whichever comes first.
# Each REST API call requires a new Service Ticket.
# You should include the key names and key values within the url-encoded body of the form in your POST call. for example:
# curl -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/{your_TGT_here} -H 'content-type: application/x-www-form-urlencoded' -d service=http%3A%2F%2Fumlsks.nlm.nih.gov
class UmlsAuthentication(object):

    def __init__(self, apikey):
        if not "".__eq__(apikey):
            self.apikey = apikey
            self.service = umls_auth_service_url
            self.login_uri = umls_auth_login_url
            self.auth_endpoint = umls_auth_endpoint_url

            self.header = {
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "text/plain", "User-Agent": "python"
            }
            
            self.ticket_granting_ticket = self.get_tgt()
            self.service_ticket = self.get_st(self.ticket_granting_ticket)

            

    def get_tgt(self) -> str:
        params = { 'apikey' : self.apikey }

        r = requests.post(self.login_uri + self.auth_endpoint, data=params, headers=self.header)

        response = fromstring(r.text)

        # extract the entire URL needed from the HTML form (action attribute) returned
        # looks similar to https://utslogin.nlm.nih.gov/cas/v1/tickets/TGT-36471-aYqNLN2rFIJPXKzxwdTNC5ZT7z3B3cTAKfSc5ndHQcUxeaDOLN-cas
        # we make a POST call to this URL in the getst method
        tgt = response.xpath('//form/@action')[0]

        return tgt

    def get_st(self, tgt) -> str:

        params = { 'service' : self.service }

        r = requests.post(tgt, data=params, headers=self.header)
        st = r.text

        return st
  


# this class encapsulates all the logic needed to obtain the information needed
# to load a data set from a given online service like the UMLS. the configuration is stored in
# table load_config in TigerGraph Cloud. this class also provides methods for renewing the authentication
# token required by the REST++ service.
class DataSetConfig(object):

    def __init__(self, data_set_name):
        if not "".__eq__(data_set_name):
            self.data_set_name = data_set_name
            self.config_database_name = config_database_name
            self.config_database_apitoken = config_database_apitoken
            self.config_database_apitoken_port = config_database_apitoken_port
            self.config_database_apitoken_expire_date = config_database_apitoken_expire_date
            self.config_database_apitoken_lifetime = config_database_apitoken_lifetime
            self.config_database_ip = config_database_ip
            self.config_database_secret = config_database_secret
            self.config_metadata_load_on_startup = config_metadata_load_on_startup
            self.config_metadata_file_path = config_metadata_file_path

            # use a current api token or get a new one when needed
            self.api_token = self.get_tg_auth_token()

            # define the header for the rest api request and specify the api token
            self.header = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(self.api_token)
            }
            
            # reload the load_config table from a local file if needed
            if "true".__eq__(self.config_metadata_load_on_startup):
                self.load_data_pipeline_config()

            # get the configuration data for this data set            
            self.get_data_pipeline_config()

    def __str__(self):
        return str(self.__class__) + '\n' + '\n'.join((str(item) + ' = ' + 
            str(self.__dict__[item]) for item in sorted(self.__dict__)))

    """ this method will get the contents of the load_config table data from a local file """
    def file_get_contents(self, filename):
        if os.path.exists(filename):
            fp = open(filename, "r")
            content = fp.read()
            fp.close()

        return content

    """ this method will get a new api token from TigerGraph """
    def get_tg_auth_token(self) -> str:
        try:
            if (("".__eq__(self.config_database_apitoken) or datetime.fromtimestamp(int(self.config_database_apitoken_expire_date)).date() < date.today()) or config_database_apitoken_force_new == "true"):

                response = requests.get("{ip}:{port}/requesttoken?secret={secret}&lifetime={lt}".format(
                    ip=self.config_database_ip,
                    port=self.config_database_apitoken_port,
                    secret=self.config_database_secret,
                    lt=self.config_database_apitoken_lifetime
                ))

                response_dict = response.json()

                # get the token and expiration
                self.config_database_apitoken = response_dict.get('token')
                self.config_database_apitoken_expire_date = response_dict.get('expiration')

                # save the token and expiration to the configuration file. only do this for proof of concept.
                # ideally these are stored in an encrypted key-value store.
                Config.set('config_database', 'config_database_apitoken', self.config_database_apitoken)
                Config.set('config_database', 'config_database_apitoken_expire_date', str(self.config_database_apitoken_expire_date))

                return response_dict['token']
            else:
                return self.config_database_apitoken

        except Exception as e:
            print('Error getting authentication token from TigerGraph on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


    """ this method will load the data_pipeline_config table from a local file """
    def load_data_pipeline_config(self):
        try:       
            payload = self.file_get_contents(self.config_metadata_file_path)
            
            url = "{ip}:{port}/graph/{graph}".format(
                ip=self.config_database_ip, 
                port=self.config_database_apitoken_port,
                graph=self.config_database_name
            )

            response = requests.request("POST", url, headers=self.header, data=payload)

            if response.status_code == 200:
                print("Data pipeline configuration metadata reloaded successfully")
            else:
                print("UMLS did not return a valid reponse: {0}".format(response.status_code))
        
        except Exception as e:
            print('Error loading load_config from local file source on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


    """ this method will get the load configuration for a given data set. relies on query GetConfig() """
    def get_data_pipeline_config(self):
        try:

            # get the configuration information for this data set
            url = getconfig_api_endpoint.format(
                ip=self.config_database_ip,
                port=self.config_database_apitoken_port,
                graph=self.config_database_name,
                ontology=self.data_set_name)

            # print(url)

            response = requests.get(url, headers=self.header)

            # set the load configuration within the object
            if response.status_code == 200:
                config = json.loads(response.content.decode('utf-8')).get('results')[0]['start'][0]

                # print(json.dumps(config, indent=4))

                if config:
                    self.source_url = config.get('attributes').get('source_url')
                    self.update_type = config.get('attributes').get('update_type')
                    self.load_type = config.get('attributes').get('load_type')
                    self.topic_name = config.get('attributes').get('topic_name')
                    self.download_type = config.get('attributes').get('download_type')
                    self.authentication_req = config.get('attributes').get('authentication_req')
                    self.first_run_date = datetime.strptime(config.get('attributes').get('first_run_date'), '%Y-%m-%d %H:%M:%S')
                    self.next_run_date = datetime.strptime(config.get('attributes').get('next_run_date'), '%Y-%m-%d %H:%M:%S')
            else:
                print("TigerGraph did not return a valid reponse: {0}".format(response.status_code))

        except Exception as e:
            print('Error getting configuration item from TigerGraph on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


class DataSetDownload(object):
    def __init__(self, data_set_name, url, umls_service_ticket):
        if not "".__eq__(data_set_name):
            self.data_set_name = data_set_name
            self.umls_service_ticket = umls_service_ticket

    def __str__(self):
        return str(self.__class__) + '\n' + '\n'.join((str(item) + ' = ' +
                str(self.__dict__[item]) for item in sorted(self.__dict__)))

    """ this method will find all valid subsets for a given umls data set. for example it is not possible to simply
    download all of the snomedct values directly from the rest api. you have to choose a subset of the snomedct data
    set (e.g.: "Anatomy structure and part association reference set" or "US English"). so this method will show the
    appropriate options to choose from. once found, the appropriate url for that subset can be added to load_config"""
    def get_umls_subsets(self):
        try:
            url = "{base}/{ds}&ticket={service_ticket}".format(
                base=umls_subsets_current_url,
                ds=self.data_set_name,
                service_ticket=self.umls_service_ticket
            )

            response = requests.get(url)

            if response.status_code == 200:
                data = json.loads(response.content.decode('utf-8')).get('result')
                print(json.dumps(data, indent=4, sort_keys=True))
            else:
                print("UMLS did not return a valid reponse: {0}".format(response.status_code))

        except Exception as e:
            print('Error getting configuration item from TigerGraph on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == "__main__":

    # record the start time
    current_time = time.time()
    current_date = datetime.today()

    # connect to the kafka service. there is a bug in the python-kafka library so we must include
    # the api version or kafka cannot connect to any broker.
    # https://blog.stigok.com/2017/12/25/kafkapython-no-broker-available-with-kafka-on-localhost.html
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, api_version=kafka_api_version)

    for data_set_name in datasets_to_load:
        try:

            # get the configuration for this data set
            ds_config = DataSetConfig(data_set_name)

            if ds_config:

                # load the data if appropriate
                if ds_config.next_run_date <= current_date:

                    print("Starting data set {}".format(data_set_name))

                    if data_set_name == "SNOMEDCT_US":

                        try:
                            for i in range(1, int(umls_api_fetch_page_count)):

                                # authenticate with the umls rest api service and generate a service ticket for each page
                                umls_auth = UmlsAuthentication(umls_api_key)

                                # define the api url based on configuration settings
                                url = "{source_url}?ticket={service_ticket}&pageSize={page_size}&pageNumber={page_no}".format(
                                    source_url=ds_config.source_url,
                                    service_ticket=umls_auth.service_ticket,
                                    page_size=umls_api_fetch_page_size,
                                    page_no=i
                                )

                                # call the api and get data
                                response = requests.get(url)

                                if response.status_code == 200:                                    
                                    data = json.loads(response.content.decode('utf-8')).get('result')

                                    # add the data to a kafka record
                                    for d in data:
                                        rec = "{0},{1},{2}".format(d.get("ui"), d.get("name"), d.get("sourceConcept"))

                                        kafka_producer.send(ds_config.topic_name, json.dumps(rec).encode('utf-8'))                                        
                                else:
                                    print("UMLS did not return a valid reponse: {0}".format(response.status_code))

                        except Exception as e:
                            print('Error posting records to Kafka for {0} on line {1}'.format(data_set_name, sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                        else:
                            print("All {0} records loaded successfully".format(data_set_name))        
                        finally:
                            kafka_producer.flush()

                    else:
                        None

                    # transform data and build edges if necessary

                    # load data for this ontology
                    None

        except Exception as e:
            print('Error getting data pipeline configuration for {0} on line {1}'.format(data_set_name, sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    print("\nTotal elapsed time {0} min".format(round((time.time()-current_time)/60, 2)))
