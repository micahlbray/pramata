# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:36:59 2018

@author: mbray201
"""

import json
import collections
import requests

from urllib import request, parse
from urllib.error import HTTPError

##### test sites credentials
#clientId = ''
#clientSecret = ''
#base_url = ''
clientId = ''
clientSecret = ''
base_url = ''
oauth_url = '/auth/oauth/v2/token'
pramata_number_url = '/services/data/v1/documents/{pramata_number}/details'
keydates_url = '/services/data/v1/documents/modified?start_date_timestamp={start_date_timestamp}&end_date_timestamp={end_date_timestamp}'
keyterms_url = ''


class ServerError(Exception): pass

def getOAuthToken(base_url, oauth_url, clientId, clientSecret):
    url = base_url + oauth_url
    json_input = {
            "client_id": clientId,
            "client_secret": clientSecret,
            "grant_type": "client_credentials",
            #"scope": "esl:location"
            }
    headers = {'Content-type': 'application/x-www-form-urlencoded', 'Accept': '*/*'}
    response = requests.post(url, json_input, headers=headers)
    if not response.ok:
        raise ServerError(response.text)

    response = response.json()
    return response["access_token"]

def pramata_key_dates_req(token, start_date, end_date):
    token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
    url = base_url + keydates_url
    url = url.replace('{' + 'start_date_timestamp' + '}',parse.quote_plus(start_date))
    url = url.replace('{' + 'end_date_timestamp' + '}',parse.quote_plus(end_date))
    headers = { 'Authorization':'Bearer ' + token  }
    req = request.Request(url, headers=headers)
    req.get_method = lambda: 'GET'
    try:
        response_body = request.urlopen(req).read()
        response = json.loads(response_body.decode("utf-8"))
    except HTTPError as e:
        response_body = e.read()
        response = json.loads(response_body.decode("utf-8"))
    return response 
   
def pramata_number_req(token, pramata_number):
    #token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
    url = base_url + pramata_number_url
    url = url.replace('{' + 'pramata_number' + '}',parse.quote_plus(pramata_number))
    headers = { 'Authorization':'Bearer ' + token  }
    req = request.Request(url, headers=headers)
    req.get_method = lambda: 'GET'
    try:
        response_body = request.urlopen(req).read()
        response = json.loads(response_body.decode("utf-8"))
    except HTTPError as e:
        response_body = e.read()
        response = json.loads(response_body.decode("utf-8"))
    return response   

def pramata_keyterms_req():
    token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
    headers = { 'Authorization':'Bearer ' + token  }
    #headers = {'Content-type': 'application/json', 'Accept': '*/*', 'Authorization': 'Bearer ' + token}
    req = request.Request(keyterms_url, headers=headers)
    req.get_method = lambda: 'GET'
    response_body = request.urlopen(req).read()
    response = json.loads(response_body.decode("utf-8"))
    return response
