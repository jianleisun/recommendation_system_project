# Project 2 Recommendation System

import requests
import json

with open('app_id.txt', 'rb') as f:
    lst_appid = f.readlines()

c = 0
with open('app_details.txt', 'a') as f:
    url = 'http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002'
    for i in lst_appid:
        dic_temp = {}
        id = i.strip()
        params = {'key': 'XXXXXX', 'appid': id, 'count': 3,
                  'maxlength': 300, 'format': 'json'}
        r = requests.get(url, params)
        dic_temp[str(int(id))] = r.json()
        f.write(json.dumps(dic_temp))
        f.write('\n')

        c += 1
        if c % 10 == 0:
            print('Finished {:5d} game ids'.format(c))


