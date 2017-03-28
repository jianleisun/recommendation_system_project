import json, os, re
from datetime import datetime
import sqlalchemy
import pandas as pd

# create file paths

path_app_info = 'app_details.txt'
path_steam_app_info = 'steam_app_info.csv'

# extract the selected app features from the web crawled raw data set

if os.path.exists(path_steam_app_info):
    # if the output file exits, avoid running the code again
    print(path_steam_app_info, 'already exists')
else:
    with open(path_app_info) as f:
        dict_steam_app = {'initial_price':{}, 'name':{}, 'score':{}, 'windows':{}, 'mac':{}, 'linux':{},
                          'type':{}, 'release_date':{}, 'recommendation':{}, 'header_image':{} }
        lst_raw_string = f.readlines()
        for raw_string in lst_raw_string:
            # retrieve dic values for 'steam_appid'
            app_data = json.loads(raw_string)
            key = list(app_data.keys())[0]
            app_data = app_data[key]

            if app_data !={}:
                steam_id = app_data.get('steam_appid')

                initial_price = app_data.get('price_overview', {}).get('initial')
                if app_data.get('is_free') == True:
                    initial_price = 0

                app_name = app_data.get('name')
                critic_score = app_data.get('metacritic', {}).get('score')
                app_type = app_data.get('type')

                for (platform, is_supported) in app_data.get('platforms', {}).items():
                    if is_supported == True:
                        dict_steam_app[platform].update({steam_id:1})

                # if game will 'come soon', just ignore it and don't recommend
                if app_data.get('release_date',{}).get('coming_soon') == False:
                    release_date = app_data.get('release_date', {}).get('date')
                    if not release_date == '':
                        if re.search(',', release_date) == None:
                            release_date = datetime.strptime(release_date, '%b %Y')
                        else:
                            try:
                                release_date = datetime.strptime(release_date, '%b %d, %Y')
                            except:
                                release_date = datetime.strptime(release_date, '%d %b, %Y')

                recommendation = app_data.get('recommendations', {}).get('total')
                header_image = app_data.get('header_image')

                # update the dictionary for 'steam_appid'
                dict_steam_app['initial_price'].update({steam_id:initial_price})
                dict_steam_app['name'].update({steam_id:app_name})
                dict_steam_app['score'].update({steam_id:critic_score})
                dict_steam_app['type'].update({steam_id:app_type})
                dict_steam_app['release_date'].update({steam_id:release_date})
                dict_steam_app['recommendation'].update({steam_id:recommendation})
                dict_steam_app['header_image'].update({steam_id:header_image})

    # output to dataframe

    df_steam_app = pd.DataFrame(dict_steam_app)
    df_steam_app['initial_price'] = df_steam_app['initial_price'].map(lambda x: x/100.0)
    df_steam_app.index.name = 'steam_appid'
    df_steam_app['windows'] = df_steam_app['windows'].fillna(0)
    df_steam_app['mac'] = df_steam_app['mac'].fillna(0)
    df_steam_app['linux'] = df_steam_app['linux'].fillna(0)

    df_steam_app = df_steam_app[['name', 'type', 'initial_price', 'release_date', 'score',
                                'recommendation', 'windows', 'mac', 'linux', 'header_image']]

    df_steam_app.reset_index(inplace=True)
    df_steam_app.to_csv(path_steam_app_info, encoding='utf8', index=False)



# save to MySql

engine = sqlalchemy.create_engine('mysql+pymysql://@127.0.0.1/game_recommendation?charset=utf8mb4&local_infile=1')

engine.execute('''
    CREATE TABLE IF NOT EXISTS `tbl_steam_app`(
        `steam_appid` INT,
        `name` VARCHAR(500) CHARACTER SET utf8mb4,
        `type` VARCHAR(15),
        `initial_price` FLOAT,
        `release_date` VARCHAR(20),
        `score` INT,
        `recommendation` INT,
        `windows` BOOLEAN,
        `mac` BOOLEAN,
        `linux` BOOLEAN,
        `header_image` VARCHAR(100)
        );
    ''')


engine.execute('''
    LOAD DATA LOCAL INFILE '%s' INTO TABLE `tbl_steam_app`
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@steam_appid, @name, @type, @initial_price, @release_date, @score, @recommendation,
    @windows, @mac, @linux, @header_image)
    SET
    steam_appid = nullif(@steam_appid, ''),
    name = nullif(@name, ''),
    type = nullif(@type, ''),
    initial_price = nullif(@initial_price, ''),
    release_date = nullif(@release_date, ''),
    score = nullif(@score, ''),
    recommendation = nullif(@recommendation, ''),
    windows = nullif(@windows, ''),
    mac = nullif(@mac, ''),
    linux = nullif(@linux, ''),
    header_image = nullif(@header_image, '');
    ''' % (path_steam_app_info))











