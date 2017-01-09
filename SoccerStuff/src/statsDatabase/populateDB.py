'''
Created on Jan 7, 2017
Module to initialize the stats database with team and player info.
@author: Bryan Lu
'''
import queue, threading, sqlite3, requests as r
from lxml import html
from timeit import default_timer as timer

db_name = 'soFifaStats.db'
base_url = 'http://sofifa.com'

def get_ids(list_type, start_num, thr_num):
    """Return a list of team/player id numbers from sofifa.com."""
    ids = []
    num = start_num
    
    if list_type == 'player': 
        offset = '/players?offset='
        mult = 1000
    else:
        offset = '/teams?offset='
        mult = 180
    
    while int(num) < (start_num + mult):
        #Use requests to get html from sofifa.
        res = r.get(base_url + offset + str(num))
        res.raise_for_status()
        tree = html.fromstring(res.content)
        
        #Keep track of which pages have been looked at.
        if thr_num == 0:
            if list_type == 'team' or int(num) % 100 == 0:
                print('Offset: ' + str(num))
        
        #Select the html tag that contains the id and add it to the list.
        team_tags = tree.xpath('//td[@class="nowrap"]/a')
        for tag in team_tags:
            ids.append(int(tag.get('href').split('/')[2]))
        #    print('Getting link for ' + tag.text)
        
        #Find the next button and get the link
        next_link = tree.xpath('//li[@class="page-item"]/a')[1].get('href')
        
        #Check if there is a next page
        if next_link == '#':
            break
        else:
            num = next_link.split('=')[1]
    
    #set() in case of duplicates 
    return set(ids)
    
def run_id_threads(list_type):
    """Return a list of all team or player ids."""
    final_list = []
    que = queue.Queue()     #Use a queue to store the result of each thread
    threads = []
    
    #Create a thread for every 10 pages in player list, 100 players per page
    if list_type == 'player':
        num_threads = 18
        mult = 1000
    #Create a thread for every 3 pages in team list, 60 teams per page
    elif list_type == 'team':
        num_threads = 4
        mult = 180
    else:
        print('Bad input for type of list.')
        return
    
    #Create threads
    for i in range(num_threads):
        list_thread = threading.Thread(
            target=lambda q, list_type, offset, thr_num: q.put(get_ids(list_type, offset, thr_num)), 
            args=(que, list_type, i*mult, i)
        )
        threads.append(list_thread)
        list_thread.start()
        
    #Wait for all threads to finish    
    for list_thread in threads:
        list_thread.join()
    print('Done. Length of result:', end=' ')

    #Combine the contents of the queue into one list
    while not que.empty():
        result = que.get()
        final_list.extend(result)
          
    print(len(final_list))
    return final_list

def generate_sql(table_name, id_list, thr_num):
    """Use sofifa.com/TYPE/id# to add data to the stats database."""
    count = 0
    command_list = []
    if table_name == 'players':
        page_type = '/player/'
    else:
        page_type = '/team/'
    
    for id_num in id_list:
        #Dictionary that holds data for the database.
        if table_name == 'players':
            info = {
                'playerId': id_num, 
                'name': None, 
                'position': None,
                'overall': None
            }
        else:
            info = {
                'teamId': id_num, 
                'name': None, 
                'overall': None, 
                'attack': None, 
                'midfield': None, 
                'defence': None
            }
        #print('Getting info for ' + table_name[:-1] + ' #' + str(id_num) + '... ', end=' ')
        
        #Use requests to get html from a specific player/team page.
        res = r.get(base_url + page_type + str(id_num))
        res.raise_for_status()
        tree = html.fromstring(res.content)
        
        #Select the html header that contains the full player/team name.
        header_string = tree.xpath('//h1/text()')[0]
        info['name'] = header_string.split('(')[0][:-1]
        
        if table_name == 'players':
            #Select the html element that has the player's position
            position = tree.xpath('//div[@class="meta"]//span/text()')
            info['position'] = str(position[1])
        
        #Select the html elements that contain the numeric stats.
        stats = tree.xpath('//div[@class="stats"]//span/text()')
        info['overall'] = int(stats[0])
        
        if table_name == 'teams':
            info['attack'] = int(stats[1])
            info['midfield'] = int(stats[2])
            info['defence'] = int(stats[3])
            #Create squad info for team
            squad_info = get_squad_info(tree, id_num)
        
        #Keep track of progress    
        if thr_num == 0 and count % 10 == 0:
            print(table_name, count)
        count += 1
        
        #Build the SQLite command that updates the database.
        sql_command = build_sql_command(table_name, info)
        command_list.append(sql_command)
        #print(sql_command)
        
        if table_name == 'teams':
            squad_sql_command = build_sql_command('squads', squad_info)
            command_list.append(squad_sql_command)
            #print(squad_sql_command)
            
    return command_list

def get_squad_info(tree, team_id):
    """Return a dictionary with the player id for each player on a team."""
    squad_info = {}
    player_ids = []
    
    #Get list of player ids for the team
    tag_list = tree.xpath('(//table[@class="table table-striped table-hover no-footer"])[1]//td[@class="nowrap"]/a')
    for tag in tag_list:
        player_ids.append(int(tag.get('href').split('/')[2]))
    
    #Add player: playerId pairs to dictionary
    for i in range(len(player_ids)):
        key = 'player_' + str(i+1)
        squad_info[key] = player_ids[i]
    
    #Associate squad with team
    squad_info['teamId'] = team_id
    
    return squad_info
    
def run_sql_threads(table_name, id_list):
    """Return a list of all SQLite commands needed for each table."""
    command_list = []
    que = queue.Queue()    #Store result of each thread in a queue
    threads = []
    
    #Create a thread for every 600 players, every 30 teams
    if table_name == 'players':
        step = 600
    elif table_name == 'teams' or table_name == 'squads':
        step = 30
    else:
        print('Bad input for table name.')
        return
    
    #Create threads
    for i in range(0, len(id_list), step):
        id_slice = id_list[i:(i+step)]
        sql_thread = threading.Thread(
            target=lambda q, t_name, ids, thr_num: q.put(generate_sql(t_name, ids, thr_num)),
            args=(que, table_name, id_slice, i)
        )
        threads.append(sql_thread)
        sql_thread.start()
         
    #Wait for all threads to finish
    for sql_thread in threads:
        sql_thread.join()
    print('Done. Length of result:', end=' ')
    
    #Put contents of queue into one list
    while not que.empty():
        result = que.get()
        command_list.extend(result)
    
    print(len(command_list))

    return command_list

def main():
    """Open connection with database and call methods to insert data."""
    
    start = timer()
    
    #Open connection to database
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    #Get team and player ids
    team_ids = run_id_threads('team')
    player_ids = run_id_threads('player')
    
    #Get SQLite commands for the ids
    sql_list = []
    sql_list.extend(run_sql_threads('teams', team_ids))
    sql_list.extend(run_sql_threads('players', player_ids))
    print('Number of sql commands generated: ' + str(len(sql_list)))
    
    #Execute the commands
    for command in sql_list:
        c.execute(command)
    
    #Save changes and close connection
    conn.commit()
    conn.close()
    
    end = timer()
    print(end - start)

def build_sql_command(table_name, info):
    """Return a string of the sql command needed to insert data in the database."""
    key_list = list(info.keys())
    val_list = list(info.values())
    
    sql_command = 'INSERT INTO ' + table_name + ' ('
    
    #Add column names to command
    for i in range(len(key_list)):
        if i != len(key_list) - 1:
            sql_command += key_list[i] + ', '
        else:
            sql_command += key_list[i] + ') VALUES ('
            
    #Add values to command     
    for i in range(len(val_list)):
        #Text values in SQLite need surrounding quotes
        if type(val_list[i]) == str:
            data = "'" + val_list[i] + "'"
        else:
            data = str(val_list[i])
            
        if i != len(val_list) - 1:
            sql_command += data + ', '
        else:
            sql_command += data + ');'
        
    return sql_command    

if __name__ == '__main__':
    main()