import json
from lxml import etree
import requests
from requests.exceptions import ConnectionError
import urllib
import urlparse

#TODO: this will go away
from smartsandbox.refs import SYSTEM_FIELDS

NS = {
        "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
        "urn"    : "urn:partner.soap.sforce.com"
    }

class SalesforceClient(object):

    def __init__(self, username, password, token=None, prod=True, api=31.0):
        url = "https://login.salesforce.com/services/oauth2/token"
        params = {
                    'grant_type'    : 'password',
                    'client_id'     : '3MVG9xOCXq4ID1uEEA_ToSIsz_mLRtePeC_NvTfFy1Djcj0T1GGBgtVpdVgDKxeej2u95jucqvNNXdGPPnm71',
                    'client_secret' : '2067262900177930870',
                    #'client_id'     : '3MVG9sLbBxQYwWqs6uqP55D9UpCuY6wTs.fN8BVGQwSSoUv98JLJ8wo4Cw5jNrRHovTG92D38KIbHEW3XfSNe',
                    #'client_secret' : '9071109493259885377',
                    'username'      : username,
                    'password'      : password
                }

        url_parts = list(urlparse.urlparse(url))
        query = dict(urlparse.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urllib.urlencode(query)

        url =  urlparse.urlunparse(url_parts)
        response = requests.post(url, headers={'content-type': 'application/x-www-form-urlencoded'})
        login_response = json.loads(response.content)
        print login_response
        response = requests.get(login_response.get('id'), headers={'Authorization': 'Bearer %s' % login_response.get('access_token')})
        id_response = json.loads(response.content)

        self.instance_url = login_response.get('instance_url')
        self.token = login_response.get('access_token')
        self.api = api
        #this will be removed with update to use oauth web server flow
        self.username = username
        self.password = password
        self.org_id = id_response.get('organization_id')

    #this will be replaced by the oauth refresh token flow
    def _keep_alive(self):
        url = "https://login.salesforce.com/services/oauth2/token"
        params = {
                    'grant_type'    : 'password',
                    #'client_id'     : '3MVG9xOCXq4ID1uEEA_ToSIsz_mLRtePeC_NvTfFy1Djcj0T1GGBgtVpdVgDKxeej2u95jucqvNNXdGPPnm71',
                    #'client_secret' : '2067262900177930870',
                    'client_id'     : '3MVG9sLbBxQYwWqs6uqP55D9UpCuY6wTs.fN8BVGQwSSoUv98JLJ8wo4Cw5jNrRHovTG92D38KIbHEW3XfSNe',
                    'client_secret' : '9071109493259885377',
                    'username'      : self.username,
                    'password'      : self.password
                }

        url_parts = list(urlparse.urlparse(url))
        query = dict(urlparse.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urllib.urlencode(query)

        url =  urlparse.urlunparse(url_parts)
        response = requests.post(url, headers={'content-type': 'application/x-www-form-urlencoded'})
        login_response = json.loads(response.content)
        self.instance_url = login_response.get('instance_url')
        self.token = login_response.get('access_token')

    def _execute(self, url, method):
        headers = {'Authorization' : 'Bearer ' + self.token}
        
        try:
            if method == 'POST':
                response = requests.post(url, headers=headers)
            elif method == 'GET':
                response = requests.get(url, headers=headers)
        except ConnectionError:
            print "Reconnecting to Salesforce"
            self._keep_alive()
            #TODO: could probably do this recursively and cleaner in the future
            if method == 'POST':
                response = requests.post(url, headers=headers)
            elif method == 'GET':
                response = requests.get(url, headers=headers)

        return json.loads(response.content)

    def get_sobjects(self):
        url = '%s/services/data/v%s/sobjects' % (self.instance_url, self.api)
        content = self._execute(url, 'GET')

        return content.get('sobjects')

    def sobject_describe(self, name):
        url = '%s/services/data/v%s/sobjects/%s/describe' % (self.instance_url, self.api, name)


        return self._execute(url, 'GET')

    def query(self, query):
        url = '%s/services/data/v%s/query/?q=%s' % (self.instance_url, self.api, query)
        content = self._execute(url, 'GET')

        return content.get('records')

    def count(self, obj):
        query = "SELECT count() FROM %s" % (obj)
        url = '%s/services/data/v%s/query/?q=%s' % (self.instance_url, self.api, query)
        content = self._execute(url, 'GET')

        return content.get('totalSize')

    def count_group(self, obj, group_by):
        query = "SELECT count(id) amt, %s id FROM %s GROUP BY %s" % (group_by, obj, group_by)
        url = '%s/services/data/v%s/query/?q=%s' % (self.instance_url, self.api, query)
        content = self._execute(url, 'GET')
        response = {}
        for ar in content.get('records'):
            if group_by=='RecordTypeId':
                group_name = 'None' if ar.get('id') is not None else ar.get('id')
            else:
                group_name = ar.get('id')
            response[group_name] = ar.get('amt')

        return response

    #TODO: make this decoupled from the business logic of the load method that is forthcoming
    def insert(self, table, data, cols):
        rows = []
        for d in data:
            rows.append(zip(cols, d))
        url = self.instance_url + "/services/Soap/u/31.0/" + self.org_id
        ins_xml = etree.parse('smartsandbox/api/soap/insert.xml')
        ins = ins_xml.getroot()
        ins.xpath("soapenv:Header/urn:SessionHeader/urn:sessionId", namespaces=NS)[0].text = self.token
        create = ins.xpath("soapenv:Body/urn:create", namespaces=NS)[0]

        for row in rows:
            sobject = etree.SubElement(create, '{urn:partner.soap.sforce.com}sobjects', nsmap=NS)
            sobject_type = etree.SubElement(sobject, '{urn:partner.soap.sforce.com}type', nsmap=NS)
            sobject_type.text = table
            for k, v in row:
                #TODO: this should be removed and moved to the decoupled load method
                if k not in ('insert_row', 'inserted') and k not in SYSTEM_FIELDS:
                    field = etree.SubElement(sobject, k)
                    v = v.replace('\'', '&apos;') if v is not None else v
                    field.text = None if v == 'None' else v

        create_xml = '<?xml version="1.0" encoding="UTF-8"?>' + etree.tostring(ins)
        response = requests.post(url, headers={"content-type": "text/xml", "SOAPAction": '""'}, data=create_xml)
        status_response = etree.fromstring(response.content)

        f = open('response.xml', 'w+')
        f.write(response.content)
        f.close()

        f = open('test.xml', 'w+')
        f.write(etree.tostring(ins_xml))
        f.close()