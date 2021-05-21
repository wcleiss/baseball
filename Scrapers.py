#This file is essential to scraping information relative to baseball.
#Past box scores, past weather data, future schedules, projected weather, and projected lineups are among the information scraped.

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

#Creates Box Score URLs from Start Day to End Day as Specified in Dates.Csv
def weather_past_ids(sday, eday):
    
    dates_file = pd.read_csv('config\\dates.csv')
    
    date_begin = sday - 1
    date_end = eday

    dates_filter = dates_file.iloc[date_begin:date_end, :]
    dates_filter = dates_filter.reset_index()
    dates_filter.drop('index', axis='columns', inplace=True)
    
    yr = dates_filter.loc[0, 'Year']
    open('config\\wpid.txt', 'w').close()
    a = 0
    g = len(dates_filter)
    for a in range(g):
        
        mo = dates_filter.loc[a, 'Month']
        da = dates_filter.loc[a, 'Day']
        url = 'https://www.baseball-reference.com/boxes/?year=' + str(yr) + '&month=' + str(mo) + '&day=' + str(da)
        with open('config\\wpid.txt', 'a') as date_write:
            date_write.write(str(url) + '\n')
            
    open('config\\wbox.txt', 'w').close()
    
    # Using readline()
    w_file = open('config\\wpid.txt', 'r')
    count = 0
     
    while True:
        count += 1
     
        url = w_file.readline()
        url = url.strip()
        print(url)
     
        if not url:
            break
        
        r = requests.get(url)
        html_doc = r.text
        soup = BeautifulSoup(html_doc)
        pretty_soup = soup.prettify()
        
        for link in soup.find_all('a'):
            link_get = link.get('href')
            link_type = type(link_get)
            if link_get != None:
                if ('/boxes/' in link_get) & ('shtml' in link_get):
                    with open('config\\wbox.txt', 'a') as date_write:
                        date_write.write(str(link_get) + '\n')
                    
     
    w_file.close()

#Function that Grabs Weather, Scores, F5 Scores from BR
def weather_get(url):
        
    r = requests.get(url)
    html_doc = r.text
    soup = BeautifulSoup(html_doc)
    
    open('config\\whtml.txt', 'w').close()
    with open('config\whtml.txt', 'w') as html_write:
        html_write.write(str(soup))
    
    w_file = open('config\\whtml.txt', 'r')
    count = 0
    score_count = 0
    
    line_switch = 0
    line_a_ct = 0
    line_a_5 = 0
    line_h_ct = 0
    line_h_5 = 0
                    
     
    while True:
        count += 1
     
        h_line = w_file.readline()
        h_line = h_line.strip()
        
        if ('<td class="center">' in h_line) & ('</td>' in h_line):
            if line_switch == 2:
                if line_h_ct <= 4:
                    
                    strip_score = BeautifulSoup(h_line)
                    strip_score = strip_score.get_text()
                    line_h_5 += int(strip_score)
                    line_h_ct += 1
                if line_h_ct > 4:
                    
                    line_switch = 999
                    
                
            if line_switch == 1:
                if line_a_ct <= 4:
                    
                    strip_score = BeautifulSoup(h_line)
                    strip_score = strip_score.get_text()
                    line_a_5 += int(strip_score)
                    line_a_ct += 1
                if line_a_ct > 4:
                    line_switch = 99
            
                
     
        if '<div><strong>Start Time Weather' in h_line:
            h_list = h_line.split(" ")
            break
        if '<div class="score">' in h_line:
            strip_score = BeautifulSoup(h_line)
            strip_score = strip_score.get_text()
            if score_count == 0:
                score_away = strip_score
                score_count += 1
            else:
                score_home = strip_score
        
        if '<div class="linescore_wrap">' in h_line:
            line_switch = 1
        
        if (line_switch == 99) & ('.shtml' in h_line):
            line_switch = 2
        
        
    w_file.close()
    
    a = 0
    g = len(h_list)
    for a in range(g):
        
        list_line = h_list[a]
        if 'deg' in list_line:
            temp = list_line.split('&')
            temp = int(temp[0])
        if 'Wind' in list_line:
            windspd = h_list[a + 1]
            windspd = windspd.split('mph')
            windspd = int(windspd[0])
        if ('from' in list_line) | ('out' in list_line):
            marker = a
            chk = 0
            ct = 0
            while chk == 0:
                
                next_line = h_list[marker]
                
                if ct == 0:
                    wind_list = next_line
                else:
                    wind_list = str(wind_list) + ' ' + str(next_line)
                
                gg = len(next_line) - 1
                comma_check = next_line[gg]
                
                if (comma_check == ','):
                    chk = 1
                    break
                
                ct += 1
                marker += 1
        if 'Dome' in list_line:
            wind_list = 'unknown,'
        if 'direction' in list_line:
            wind_list = 'unknown,'
        if '0mph' in list_line:
            wind_list = 'unknown'
    
    winddir = wind_list[:-1]
    return([temp, windspd, winddir, score_away, score_home, line_a_5, line_h_5])
    
#Loops Through Box Ids Grabbed Above and Outputs to Files
def weather_box_cycle():
    
    # Using readline()
    w_file = open('config\\wbox.txt', 'r')
    team_file = pd.read_csv('config\\teamnames.csv')
    
    act_day = 0
    act_mon = 0
    
    ct2 = 0
     
    while True:

        t_l = w_file.readline()
        t_l = t_l.strip()
        
        if not t_l:
            break
        
        tm_id = t_l[7:10]
        yr_id = t_l[14:18]
        mo_id = t_l[18:20]
        dy_id = t_l[20:22]
        
        if (mo_id != act_mon) | (dy_id != act_day):
            print("HI")
            ct = 0
            if ct2 > 0:
                file_name = 'scores\\score_data_' + str(act_mon) + '_' + str(act_day) + '_' + str(yr_id) + '.csv'
                pd_df.to_csv(file_name, index=False, header=True, encoding='utf-8-sig')
            act_mon = mo_id
            act_day = dy_id
        
        url = 'https://www.baseball-reference.com/' + str(t_l)
        print(url)
        
        a = 0
        g = len(team_file)
        for a in range(g):
            c = team_file.loc[a, 'BR']
            if c == tm_id:
                act_team = team_file.loc[a, 'Abb']
        
        
        wg = weather_get(url)
        
        temp_get = wg[0]
        windspd_get = wg[1]
        winddir_get = wg[2]
        score_away_get = wg[3]
        score_home_get = wg[4]
        f5_score_away_get = wg[5]
        f5_score_home_get = wg[6]
        
        print(wg)
        
        if winddir_get == 'out to Centerfield':
            winddir_get = 'tocf'
        elif winddir_get == 'from Left to Right':
            winddir_get = 'ltor'
        elif winddir_get == 'from Right to Left':
            winddir_get = 'rtol'
        elif winddir_get == 'from Rightfield':
            winddir_get = 'fromrf'
        elif winddir_get == 'out to Rightfield':
            winddir_get = 'torf'
        elif winddir_get == 'out to Leftfield':
            winddir_get = 'tolf'
        elif winddir_get == 'from Leftfield':
            winddir_get = 'fromlf'
        elif winddir_get == 'from Centerfield':
            winddir_get = 'fromcf'
        else:
            winddir_get == 'unknown'
        
        if ct == 0:
            pd_df = pd.DataFrame({'act_team':str(act_team), 'month':mo_id, 'day':dy_id, 'year':yr_id,
                                  'temp':temp_get, 'windspd':windspd_get, 'winddir':winddir_get,
                                  'score_away':score_away_get, 'score_home':score_home_get,
                                  'f5score_away':f5_score_away_get, 'f5score_home':f5_score_home_get}, index=[0])
        else:
            pd_df2 = pd.DataFrame({'act_team':str(act_team), 'month':mo_id, 'day':dy_id, 'year':yr_id,
                                  'temp':temp_get, 'windspd':windspd_get, 'winddir':winddir_get,
                                  'score_away':score_away_get, 'score_home':score_home_get,
                                  'f5score_away':f5_score_away_get, 'f5score_home':f5_score_home_get}, index=[0])
            
            pd_df = pd.concat([pd_df, pd_df2], axis='rows')
            
        
        ct += 1
        ct2 += 1
    
    file_name = 'scores\\score_data_' + str(mo_id) + '_' + str(dy_id) + '_' + str(yr_id) + '.csv'
    pd_df.to_csv(file_name, index=False, header=True, encoding='utf-8-sig')
        
def roto_lineup_get():
    
    url = 'https://www.rotowire.com/baseball/daily-lineups.php'
    r = requests.get(url)
    html_doc = r.text
    soup = BeautifulSoup(html_doc)
    
    open('config\\whtml.txt', 'w').close()
    with open('config\whtml.txt', 'w') as html_write:
        html_write.write(str(soup))
    
    
def roto_lineup_seek():
    
    w_file = open('config\\whtml.txt', 'r')
    
    count = 0
    t_count = 0
    p_count = 0
    b_count = 0
    s_count = 0
    h_count = 0
    w_count = 0
    
    while True:
        count += 1
     
        h_line = w_file.readline()
        h_line = h_line.strip()
        
        if 'footer' in h_line:
            h_list = h_line.split(" ")
            break
        
        if 'lineup__abbr' in h_line:
            h_soup = BeautifulSoup(h_line)
            h_text = h_soup.get_text()
            if h_text == "WAS":
                h_text = "WSH"
            t_count += 1
            if t_count == 1:
                team_list = [h_text]
            else:
                team_list = team_list + [h_text]
        
        if ('a href' in h_line) & ('title' in h_line) & ('player' in h_line):
            p_count += 1
            p_name = h_line.split('"')[3]
            if p_count == 1:
                player_list = [p_name]
            else:
                player_list = player_list + [p_name]
        
        if ('game has been postponed' in h_line):
            
            team_list = team_list[0:(len(team_list) - 2)]
            print(team_list)
        
        if ('lineup__bats' in h_line):
            h_soup = BeautifulSoup(h_line)
            h_text = h_soup.get_text()
            b_count += 1
            if b_count == 1:
                bat_list = [h_text]
            else:
                bat_list = bat_list + [h_text]
        
        if ('a href="/baseball/player.php' in h_line) & ('title' not in h_line):
            h_soup = BeautifulSoup(h_line)
            h_text = h_soup.get_text()
            s_count += 1
            if s_count == 1:
                start_list = [h_text]
            else:
                start_list = start_list + [h_text]
        
        if ('lineup__throws' in h_line):
            h_soup = BeautifulSoup(h_line)
            h_text = h_soup.get_text()
            h_count += 1
            if h_count == 1:
                hand_list = [h_text]
            else:
                hand_list = hand_list + [h_text]
        
        if ('°' in h_line) | ('In Dome' in h_line):
            
            w_count += 1
            
            if ('Wind' in h_line):
                h_text = h_line.split(" ")
                h_temp = int(h_text[0].split('°')[0])
                h_windspd = int(h_text[1])
                h_winddir = h_text[3]
                
            if ('Dome' in h_line):
                h_temp = 72
                h_winddir = 'unknown'
                h_windspd = 0
            
            if h_winddir == 'R-L':
                h_winddir = 'rtol'
            if h_winddir == 'L-R':
                h_winddir = 'ltor'
            if h_winddir == 'In':
                h_winddir = 'fromcf'
            if h_winddir == 'Out':
                h_winddir = 'tocf'
            
            
            ww_list = [h_temp, h_winddir, h_windspd]
            if w_count == 1:
                w_list = [ww_list]
            else:
                w_list = w_list + [ww_list]
        
        
            
    print(team_list)
    print(start_list)
    print(hand_list)
    print(w_list)
    
    a = 0
    g = len(team_list) - 1
    c = 0
    while (a <= g):
        
        print(a)
        h = a + 1
        
        away_team = team_list[a]
        home_team = team_list[h]
        
        away_starter = start_list[a]
        home_starter = start_list[h]
        
        away_hand = hand_list[a]
        home_hand = hand_list[h]
        
        temp_use = w_list[c][0]
        winddir_use = w_list[c][1]
        windspd_use = w_list[c][2]
        
        d = c + 1
        
        if (c == 0):
            
            stage_frame = pd.DataFrame(data = {'Away':away_team, 'Home':home_team, 'AwayStart':away_starter, 'HomeStart':home_starter,
                                               'AwayHand':away_hand, 'HomeHand':home_hand, 'Temp':temp_use,
                                               'WindDir':winddir_use, 'WindSpeed':windspd_use}, index=[c])
        else:
            
            stage_frame2 = pd.DataFrame(data = {'Away':away_team, 'Home':home_team, 'AwayStart':away_starter, 'HomeStart':home_starter,
                                               'AwayHand':away_hand, 'HomeHand':home_hand, 'Temp':temp_use,
                                               'WindDir':winddir_use, 'WindSpeed':windspd_use}, index=[c])
            stage_frame = pd.concat([stage_frame, stage_frame2], axis='rows')
            
        
        a = a + 2
        c += 1
    
    a = 0
    g = len(stage_frame)
    stage_frame['GameNo'] = 0
    
    for a in range(g):
        
        at = stage_frame.iloc[a, 0]
        
        team_sum = sum(stage_frame['Away'] == at)
        
        if team_sum == 1:
            stage_frame.loc[a, 'GameNo'] = 1
        else:
            ct = 0
            b = 0
            for b in range(g):
                
                at_c = stage_frame.iloc[b, 0]
                if (at_c == at):
                    
                    ct = ct + 1
                    stage_frame.loc[b, 'GameNo'] = ct
        
    print(stage_frame)
    w_file.close()
    
    a = 0
    g = len(stage_frame)
    ct = 0
    
    print(player_list)
    print(bat_list)
    print(len(player_list))
    print(len(bat_list))

    for a in range(g):
        
        away_team = stage_frame.iloc[a, 0]
        home_team = stage_frame.iloc[a, 1]
        game_no = stage_frame.iloc[a, 9]
        
        b = 1
        
        while (b <= 9):
            
            player_id = player_list[ct]
            stand_id = bat_list[ct]
            
            if ct == 0:
                
                lineup_frame = pd.DataFrame({'Team':away_team, 'GameNo':game_no, 'Bats':stand_id, 'Player':player_id}, index=[ct])
            
            else:
                
                lineup_frame2 = pd.DataFrame({'Team':away_team, 'GameNo':game_no, 'Bats':stand_id, 'Player':player_id}, index=[ct])
                lineup_frame = pd.concat([lineup_frame, lineup_frame2], axis='rows')
                
            ct += 1
            b += 1
        
        b = 1

        while (b <= 9):
            
            if (ct >= len(player_list)):
                ct = len(player_list) - 1
            
            
            player_id = player_list[ct]
            stand_id = bat_list[ct]

            lineup_frame2 = pd.DataFrame({'Team':home_team, 'GameNo':game_no, 'Bats':stand_id, 'Player':player_id}, index=[ct])
            lineup_frame = pd.concat([lineup_frame, lineup_frame2], axis='rows')
                
            ct += 1
            b += 1
        
            
        
        stage_frame.to_csv('config\\stage.csv', index=False, header=True, encoding='utf-8-sig')
        lineup_frame.to_csv('config\\lineups.csv', index=False, header=True, encoding='utf-8-sig')
        
        

     
        
        
        
    



    
                
                
