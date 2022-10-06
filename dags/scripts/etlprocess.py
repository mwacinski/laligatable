import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from pandasql import sqldf
from google.cloud import storage

def get_laliga_tags():
    url = 'https://understat.com/league/la_liga'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    soup_scripts = soup.find_all("script")
    return soup_scripts


def get_laliga_data():
    soup_scripts = get_laliga_tags()
    strings = soup_scripts[1].string
    index_start = strings.index("('") + 2
    index_end = strings.index("')")
    json_data = strings[index_start:index_end]
    json_data = json_data.encode('utf-8').decode('unicode_escape')
    data = json.loads(json_data)
    return data


def create_table():
    data = []
    data_json = get_laliga_data()
    i = 0
    for _ in data_json:
        if data_json[i]['isResult'] == False:
            break
        data.append([
            data_json[i]['a']['title'],
            data_json[i]['a']['short_title'],
            data_json[i]['h']['title'],
            data_json[i]['h']['short_title'],
            data_json[i]['datetime'],
            data_json[i]['forecast']['d'],
            data_json[i]['forecast']['l'],
            data_json[i]['forecast']['w'],
            data_json[i]['goals']['a'],
            data_json[i]['goals']['h']
        ]
        )
        i += 1
    df = pd.DataFrame(data, columns=['Title of Away Team', 'Short Title of Away Team', 'Title of Home Team',
                                     'Short Title of Home Team',
                                     'DateTime', 'Chances for draw', 'Chances for away team', 'Chances for home team',
                                     'Away team goals', 'Home team goals'])

    df = df.astype({"Title of Away Team": str, "Short Title of Away Team": str,
                    "Title of Home Team": str, "Short Title of Home Team": str,
                    "Chances for draw": float, "Chances for away team": float,
                    "Chances for home team": float, "Away team goals": int, "Home team goals": int})
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    return df


def export_table(today, path):
    df_weekly = create_table()
    filename_weekly = path + today + '.csv'
    df_weekly.to_csv(filename_weekly, index=False)


def get_extracted_data(today, path):
    data = pd.read_csv(path + today + '.csv', sep=',')
    return data


def transform_data():
    df_helper = pd.DataFrame(create_table())
    if not df_helper.empty:
        new_table = sqldf(
            'WITH goals_scored_away AS '
            '( '
            '  SELECT "Title of Away Team" as teams, SUM("Away team goals") as goals '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'goals_scored_home AS '
            '( '
            '  SELECT "Title of Home Team" as teams, SUM("Home team goals") as goals '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'goals_scored AS '  # GOALS SCORED
            '( '
            'SELECT gsh.teams as teams, gsa.goals+gsh.goals as goals '
            'FROM goals_scored_away gsa '
            'JOIN goals_scored_home gsh '
            'ON gsa.teams = gsh.teams '
            '), '
            'games_played_home AS '
            '( '
            '  SELECT "Title of Home Team" as teams, COUNT("Title of Home Team") as games_played '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'games_played_away AS '
            '( '
            '  SELECT "Title of Away Team" as teams, COUNT("Title of Away Team") as games_played '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'games_played AS '  # GAMES PLAYED
            '( '
            'SELECT gpa.teams as teams, gpa.games_played+gph.games_played as games_played '
            'FROM games_played_away gpa '
            'JOIN games_played_home gph '
            'ON gpa.teams = gph.teams '
            '), '
            'goals_lost_away AS '
            '( '
            '  SELECT "Title of Away Team" as teams, SUM("Home team goals") as goals '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'goals_lost_home AS '
            '( '
            '  SELECT "Title of Home Team" as teams, SUM("Away team goals") as goals '
            '  FROM df_helper '
            '  GROUP BY 1 '
            '), '
            'goals_lost AS '  # GOALS LOST
            '( '
            'SELECT glsh.teams as teams, glsa.goals+glsh.goals as goals_lost '
            'FROM goals_lost_away glsa '
            'JOIN goals_lost_home glsh '
            'ON glsa.teams = glsh.teams '
            '), '
            'goal_balance AS '  # GOAL BALANCE
            '( '
            'SELECT gls.teams as teams, goals-goals_lost as goal_balance '
            'FROM goals_lost gls '
            'JOIN goals_scored gs '
            'ON gls.teams = gs.teams '
            '), '
            'games_won_home AS '
            '( '
            'SELECT "Title of Home Team" as teams, SUM( '
            'CASE '
            '    WHEN "Home team goals" > "Away team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as won_home '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_won_away AS '
            '( '
            'SELECT "Title of Away Team" as teams, SUM( '
            'CASE '
            '    WHEN "Away team goals" > "Home team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as won_away '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_won AS '  # GAMES WON
            '( '
            'SELECT gwh.teams, won_away+won_home as games_won '
            'FROM games_won_home gwh '
            'JOIN games_won_away gwa '
            'ON gwh.teams = gwa.teams '
            '), '
            'games_lost_away AS '
            '( '
            'SELECT "Title of Away Team" as teams, SUM( '
            'CASE '
            '    WHEN "Away team goals" < "Home team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as lost_away '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_lost_home AS '
            '( '
            'SELECT "Title of Home Team" as teams, SUM( '
            'CASE '
            '    WHEN "Away team goals" > "Home team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as lost_home '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_lost AS '  # GAMES LOST
            '( '
            'SELECT glh.teams, lost_away+lost_home as games_lost '
            'FROM games_lost_home glh '
            'JOIN games_lost_away gla '
            'ON glh.teams = gla.teams '
            '), '
            'games_draw_away AS '
            '( '
            'SELECT "Title of Away Team" as teams, SUM( '
            'CASE '
            '    WHEN "Away team goals" = "Home team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as draw_away '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_draw_home AS '
            '( '
            'SELECT "Title of Home Team" as teams, SUM( '
            'CASE '
            '    WHEN "Away team goals" = "Home team goals" THEN 1 '
            '    ELSE 0 '
            '    END '
            ') as draw_home '
            'FROM df_helper '
            'GROUP BY 1 '
            '), '
            'games_draw AS '  # GAMES DRAW
            '( '
            'SELECT gdh.teams, draw_away+draw_home as games_draw '
            'FROM games_draw_home gdh '
            'JOIN games_draw_away gda '
            'ON gdh.teams = gda.teams '
            '), '
            'points AS '  # POINTS
            '( '
            'SELECT gd.teams as teams, ((games_won * 3) + (games_draw * 1) + (games_lost * 0)) as points '
            'FROM games_won gw '
            'JOIN games_draw gd '
            'ON gw.teams = gd.teams '
            'JOIN games_lost gls '
            'ON gw.teams = gls.teams '
            'ORDER BY 2 DESC '
            ') '  # RESULT
            'SELECT gp.teams, goals , goals_lost, games_played, games_won, games_draw, games_lost, goal_balance, points '
            'FROM games_played gp '
            'JOIN goals_scored gs '
            'ON gp.teams = gs.teams '
            'JOIN goals_lost gls '
            'ON gp.teams = gls.teams '
            'JOIN games_won gw '
            'ON gp.teams = gw.teams '
            'JOIN games_lost gl '
            'ON gp.teams = gl.teams '
            'JOIN games_draw gd '
            'ON gp.teams = gd.teams '
            'JOIN goal_balance gb '
            'ON gp.teams = gb.teams '
            'JOIN points pts '
            'ON gp.teams = pts.teams '
        )

    new_table = new_table.astype({"goals": int, "goals_lost": int, "games_played": int, "games_won": int,
                                  "games_draw": int, "goal_balance": int, "points": int})

    return new_table


def export_new_table(today, path):
    df_weekly_new_table = transform_data()
    filename_weekly_new_table = path + today + 'T.csv'
    if len(df_weekly_new_table.columns) == 9:
        df_weekly_new_table.to_csv(filename_weekly_new_table, index=False)


def upload_to_gcs(bucket, object_name, local_file):

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
