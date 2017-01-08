'''
Created on Jan 6, 2017

@author: Bryan Lu
'''
import sqlite3, requests as r
from lxml import html

db_name = 'soFifaStats.db'
version_string = ''

def update_teams_fifa(cursor):
    pass

def update_players(cursor):
    pass

def main():
    res = r.get('http://sofifa.com')
    tree = html.fromstring(res.content)
    new_version = tree.xpath('//a[@class="choose-version"]/text()')[0]
    if version_string == new_version:
        print("Fifa stats up to date.")
    else:
        version_string = new_version
        print('soFIFA update: ' + version_string)
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        update_teams_fifa(c)
        update_players(c)
        conn.commit()
        conn.close()

if __name__ == '__main__':
    main()