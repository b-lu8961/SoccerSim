'''
Created on Jan 6, 2017

@author: Bryan Lu
'''
import sqlite3

db_name = 'soFifaStats.db'
column_info = {
        'id': 'integer PRIMARY KEY', 
        'name': 'text', 
        'overall': 'integer', 
        'attack': 'integer', 
        'midfield': 'integer', 
        'defence': 'integer', 
        'elo': 'real'
    }

def setup_team_table(cursor):
    for k, v in column_info.items():
        try:
            cursor.execute('SELECT {} FROM teams'.format(k))
        except sqlite3.Error:
            print('Creating column with name {} and data type {}'.format(k, v))
            cursor.execute('ALTER TABLE teams ADD COLUMN {} {}'.format(k, v))
        else:
            continue
        
    for i in range(40):
        col_name = 'player_' + str(i+1)
        data_type = 'integer'
        try:
            cursor.execute('SELECT {} FROM teams'.format(col_name))
        except sqlite3.Error:
            print('Creating column with name {} and data type {}'.format(col_name, data_type))
            cursor.execute('ALTER TABLE teams ADD COLUMN {} {}'.format(col_name, data_type))
        else:
            continue
    
def main():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    setup_team_table(c)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()