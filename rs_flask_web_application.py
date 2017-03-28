from flask import Flask, render_template
import sqlalchemy

app = Flask(__name__)

engine = sqlalchemy.create_engine('mysql+pymysql://@127.0.0.1/game_recommendation?charset=utf8mb4')


@app.route('/')
@app.route('/index')
def index():
    return "Hello, World !\n\nAppend /recommendation/<userid> to the current " \
           "url\n\nSome available userids 76561197960355015, 76561197960385706"


@app.route('/recommendation/<user_id>')
def recommendation(user_id):
    # retrieve recommendation for 'user_id'
    results = engine.execute('''
        SELECT g0, g1, g2, g3, g4, g5, g6, g7, g8, g9 FROM tbl_recommendation_games WHERE user_id=%s;
    ''' % user_id).first()

    lst_recommend_games = []
    for app_id in list(results):
        app_data = engine.execute('''
            SELECT name, initial_price, header_image FROM tbl_steam_app WHERE steam_appid=%s;
        ''' % app_id).first()
        if app_data != None:
            lst_recommend_games.append(app_data)

    return render_template('recomendation.html', user_id=user_id, lst_recommend_games=lst_recommend_games)


if __name__ == '__main__':
    app.run(debug=True)
