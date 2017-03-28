# Project 2 Recommendation System

import requests
import json


with open('steam_user_id.txt', 'rb') as f:
    lst_steamid = f.readlines()

c = 0
with open('steam_user_inventory.txt', 'a') as f:
    url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001'
    for i in lst_steamid:
        dic_temp = {}
        id = i.strip()
        params = {'key': 'XXXXXX', 'steamid': id, 'format': 'json'}
        r = requests.get(url, params)
        dic_temp[str(int(id))] = r.json()
        f.write(json.dumps(dic_temp))
        f.write('\n')

        c += 1
        if c % 10 == 0:
            print('Finished {:5d} steam ids'.format(c))


