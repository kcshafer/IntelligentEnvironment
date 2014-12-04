import json
import requests
import urllib

class SalesforceClient(object):

    def __init__(self, username, password, token=None, prod=True, api=31.0):
        import urllib
        import urlparse

        url = "https://login.salesforce.com/services/oauth2/token"
        params = {
                    'grant_type'    : 'password',
                    'client_id'     : '3MVG9xOCXq4ID1uEEA_ToSIsz_mLRtePeC_NvTfFy1Djcj0T1GGBgtVpdVgDKxeej2u95jucqvNNXdGPPnm71',
                    'client_secret' : '2067262900177930870',
                    'username'      : 'kc@analyticscloud.com',
                    'password'      : 'Rubygem14'
                }

        url_parts = list(urlparse.urlparse(url))
        query = dict(urlparse.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urllib.urlencode(query)

        url =  urlparse.urlunparse(url_parts)
        response = requests.post(url, headers={'content-type': 'application/x-www-form-urlencoded'})
        login_response = json.loads(response.content)
        print "login response"
        print login_response
        self.instance_url = login_response.get('instance_url')
        self.token = login_response.get('access_token')
        self.api = api

    
    def get_sobjects(self):
        url = '%s/services/data/v%s/sobjects' % (self.instance_url, self.api)
        headers = {'Authorization' : 'Bearer ' + self.token}
        response = requests.get(url, headers=headers)

        content = json.loads(response.content)

        return content.get('sobjects')

    def sobject_describe(self, name):
        url = '%s/services/data/v%s/sobjects/%s/describe' % (self.instance_url, self.api, name)
        headers = {'Authorization' : 'Bearer ' + self.token}
        response = requests.get(url, headers=headers)

        return json.loads(response.content)

    def query(self, query):
        url = '%s/services/data/v%s/query/?q=%s' % (self.instance_url, self.api, query)
        headers = {'Authorization' : 'Bearer ' + self.token}
        response = requests.get(url, headers=headers)

        content = json.loads(response.content)

        return content.get('records')
