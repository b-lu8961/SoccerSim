'''
Created on Jan 7, 2017
Module to initialize the stats database with team and player info.
@author: Bryan Lu
'''
import queue, threading, sqlite3, requests as r
from lxml import html

db_name = 'soFifaStats.db'
base_url = 'http://sofifa.com'

def get_team_list():
    """Return a dictionary of team names and url extensions from sofifa.com."""
    page_num = 1
    next_page = True
    team_links = {}
    offset = '/teams'
    
    while next_page:
        #Use requests to get html from the sofifa team list.
        res = r.get(base_url + offset)
        res.raise_for_status()
        tree = html.fromstring(res.content)
        
        #Keeping track of how many pages have been looked at.
        print('Getting links for page ' + str(page_num) + ' of team list.')
        page_num += 1
        
        #Selects the html tag that contains the url extension and adds it to the dictionary.
        team_tags = tree.xpath('//td[@class="nowrap"]/a')
        for tag in team_tags:
            team_links[tag.text] = tag.get('href')
        #    print('Getting link for ' + tag.text)
        
        #Finding the next button and getting the link
        next_link = tree.xpath('//li[@class="page-item"]/a')[1].get('href')
        
        #Checking if there is a next page
        if next_link == '#':
            next_page = False
        else:
            offset = next_link
            next_page = True
        
    #for k, v in team_links.items():
    #    print("{}: {}".format(k, v))
    #print('There are ' + str(len(team_links)) + ' teams in the list.')
    #print()
    return team_links
    
def populate_teams(links, cursor):
    """Use sofifa.com/team/id# to add data to the stats database."""
    count = 0
    
    for k, v in links.items():
        #Dictionary that holds data for the database.
        team_info = {
            'id': None, 
            'name': None, 
            'overall': None, 
            'attack': None, 
            'midfield': None, 
            'defence': None
        }
        print('Getting info for ' + k + '... ', end=' ')
        
        #Use requests to get html from a specific team page.
        res = r.get(base_url + v)
        res.raise_for_status()
        tree = html.fromstring(res.content)
        
        #Selects the html header that contains the team name and id #.
        #Header text has format TEAM NAME (ID: ####).
        #Thus, header text is split at the '('
        header_string = tree.xpath('//h1/text()')[0]
        split_list = header_string.split('(')
        
        #One team has a '(' in their name.
        if len(split_list) > 2:
            name_string = split_list[0] + '(' + split_list[1]
            name_string = name_string[:-1]
            team_info['name'] = name_string
            id_num = int(split_list[2][4:-1])
            team_info['id'] = id_num
        else:
            team_info['name'] = split_list[0][:-1]
            team_info['id'] = int(split_list[1][4:-1])
        
        #Selects the html elements that contain the numeric stats for the team.
        stats = tree.xpath('//div[@class="stats"]//span/text()')
        team_info['overall'] = int(stats[0])
        team_info['attack'] = int(stats[1])
        team_info['midfield'] = int(stats[2])
        team_info['defence'] = int(stats[3])
        
        print('Complete.')
        #print(team_info)
        
        #Building the SQLite command that updates the database.
        sql_command = 'INSERT INTO teams ('
        key_list = list(team_info.keys())
        val_list = list(team_info.values())
        for i in range(len(key_list)):
            if i == len(key_list) - 1:
                sql_command += key_list[i] + ') VALUES ('
            else:
                sql_command += key_list[i] + ', '
        for i in range(len(val_list)):
            if type(val_list[i]) == str:
                data = "'" + val_list[i] + "'"
            else:
                data = str(val_list[i])
            
            if i == len(val_list) - 1:
                sql_command += data + ');'
            else:
                sql_command += data + ', '
        
        #print(sql_command)
        count += 1
        #cursor.execute(sql_command)
    

    print(count)

def get_player_list(offset_num, thread_num):
    """Return a dictionary of player names and url extensions from sofifa.com."""
    player_links = {}
    offset = '/players?offset=' + str(offset_num)
    
    for i in range(10):
        #Use requests to get html from the sofifa team list.
        res = r.get(base_url + offset)
        res.raise_for_status()
        tree = html.fromstring(res.content)
        
        #Keeping track of how many pages have been looked at.
        print('Getting player links for page ' + str(i + 1) + ' in thread ' + str(thread_num) + '.')
        
        #Selects the html tag that contains the url extension and adds it to the dictionary.
        team_tags = tree.xpath('//td[@class="nowrap"]/a')
        for tag in team_tags:
            player_links[tag.text] = tag.get('href')
        #    print('Getting link for ' + tag.text)
        
        #Finding the next button and getting the link
        next_link = tree.xpath('//li[@class="page-item"]/a')[1].get('href')
        
        #Checking if there is a next page
        if next_link == '#':
            break
        else:
            offset = next_link
        
    #for k, v in players_links.items():
    #    print("{}: {}".format(k, v))
    #print('There are ' + str(len(player_links)) + ' players in the list.')
    #print()
    return player_links

def start_pList_threads():
    final_player_list = {}
    que = queue.Queue()
    threads = []
    for i in range(18):
        player_thread = threading.Thread(target=lambda q, offset, thread_num: q.put(get_player_list(offset, thread_num)), args=(que, i*1000, i))
        threads.append(player_thread)
        player_thread.start()
        
    for player_thread in threads:
        player_thread.join()
    print('Done.')
    count = 0
    while not que.empty():
        result = que.get()
        print(type(result), len(result))
        count += len(result)
    print(count)
def populate_players(links, cursor):
    """Use sofifa.com/player/id# to add data to the stats database."""
    pass

def main():
    """Open connection with database and call methods to insert data."""
    
    #conn = sqlite3.connect(db_name)
    #c = conn.cursor()
    
    #team_links = get_team_list()
    #populate_teams(team_links, c)
    
    start_pList_threads()
    #player_links = get_player_list()
    #populate_players(player_links, c)
    #conn.commit()
    #conn.close()
    

if __name__ == '__main__':
    main()