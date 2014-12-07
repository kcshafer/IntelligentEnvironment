import json
import requests
from requests.exceptions import ConnectionError
import urllib
import urlparse

class SalesforceClient(object):

    def __init__(self, username, password, token=None, prod=True, api=31.0):
        url = "https://test.salesforce.com/services/oauth2/token"
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
        self.instance_url = login_response.get('instance_url')
        self.token = login_response.get('access_token')
        self.api = api
        #this will be removed with update to use oauth web server flow
        self.username = username
        self.password = password

    #this will be replaced by the oauth refresh token flow
    def _keep_alive(self):
        url = "https://test.salesforce.com/services/oauth2/token"
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