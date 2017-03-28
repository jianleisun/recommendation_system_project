from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext
import sqlalchemy
import json
import pandas as pd


sc = SparkContext()

path_user_inventory = 'steam_user_inventory.txt'


# parse the crawled data set
def parse_new_string(raw_string):
    user_inventory = json.loads(raw_string)
    user_id = list(user_inventory.keys())[0]
    if user_inventory[user_id]['response'] != {} and user_inventory[user_id]['response']['game_count'] != 0:
        user_inventory = user_inventory[user_id]['response']['games']
    else:
        user_inventory = {}
    return user_id, user_inventory

# represent 'steam_appid' with index
user_inventory_rdd = sc.textFile(path_user_inventory).map(parse_new_string).zipWithIndex()


# build relationship between 'steam_appid' and 'index'
def id_index(x):
    ((user_id, lst_inventory), index) = x
    return index, user_id

dict_id_index = user_inventory_rdd.map(id_index).collectAsMap()


# create training rdd => format in (index, appid, playtime_forever)
def create_tuple(x):
    ((user_id, lst_inventory), index) = x
    if lst_inventory != {}:
        return index, [(i.get('appid'), i.get('playtime_forever'))
                       for i in lst_inventory if i.get('playtime_forever') > 0]
    else:
        return index, []

training_rdd = user_inventory_rdd.map(create_tuple).flatMapValues(lambda x: x).map(lambda x: (x[0], x[1][0], x[1][1]))


# '5' stands for feature dimension; default value is '3'
model = ALS.train(training_rdd, 5)


# make the recommendation
dic_recommend = {'g0':{}, 'g1':{}, 'g2':{},'g3':{}, 'g4':{}, 'g5':{},'g6':{}, 'g7':{}, 'g8':{}, 'g9':{}}
for index in dict_id_index.keys():
    try:
        if index % 200 == 0:
            print('working on ', index)
        lst_recommend = [i.product for i in model.recommendProducts(index, 10)]
        user_id = dict_id_index.get(index)
        rank = 0
        for app_id in lst_recommend:
            dic_recommend['g%s'%rank].update({user_id:app_id})
            rank+=1
    except:
        pass


# output to the mySQL database
engine = sqlalchemy.create_engine('mysql+pymysql://@127.0.0.1/game_recommendation?charset=utf8mb4')
df = pd.DataFrame(dic_recommend)
df.index.name = 'user_id'
df = df.reset_index()
df.to_sql('tbl_recommend_games', engine, if_exists='replace')









































