'''
Created on Jan 6, 2017
Module to setup the tables in the sqlite database.
@author: Bryan Lu
'''
import sqlite3

db_name = 'soFifaStats.db'
team_info = { 
        'name': 'text', 
        'overall': 'integer', 
        'attack': 'integer', 
        'midfield': 'integer', 
        'defence': 'integer', 
    }
player_info = {
        'name': 'text',
        'position': 'text',
        'overall': 'integer'
    }

def setup_team_table(cursor):
    """Create the table that holds team stats in the database."""
    #Ensure team table exists
    cursor.execute('CREATE TABLE IF NOT EXISTS teams (teamId integer PRIMARY KEY)')
    
    #Add columns from the team_info dictionary
    for k, v in team_info.items():
        try:
            cursor.execute('SELECT {} FROM teams'.format(k))
        except sqlite3.Error:
            print('Creating {} column in teams with data type {}'.format(k, v))
            cursor.execute('ALTER TABLE teams ADD COLUMN {} {}'.format(k, v))
        
def setup_squad_table(cursor):
    """Create the table the holds squad lists."""
    #setup_team_table must be run before this function
    cursor.execute('CREATE TABLE IF NOT EXISTS squads (teamId integer PRIMARY KEY, FOREIGN KEY(teamId) REFERENCES teams(teamId))')
    
    for i in range(40):
        col_name = 'player_' + str(i+1)
        data_type = 'integer'
        try:
            cursor.execute('SELECT {} FROM teams'.format(col_name))
        except sqlite3.Error:
            print('Creating {} column in squads with data type {}'.format(col_name, data_type))
            cursor.execute('ALTER TABLE squads ADD COLUMN {} {}'.format(col_name, data_type))
    
def setup_player_table(cursor):
    """Create the table that holds player stats."""
    cursor.execute('CREATE TABLE IF NOT EXISTS players (playerId integer PRIMARY KEY)')
        
    for k, v in player_info.items():
        try:
            cursor.execute('SELECT {} FROM players'.format(k))
        except sqlite3.Error:
            print('Creating {} column with data type {}'.format(k, v))
            cursor.execute('ALTER TABLE players ADD COLUMN {} {}'.format(k, v))
        
def main():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    setup_team_table(c)
    conn.commit()
    
    setup_squad_table(c)
    conn.commit()
    
    setup_player_table(c)
    conn.commit()
    
    conn.close()

if __name__ == '__main__':
    main()