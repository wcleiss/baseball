import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import pickle

#Load Raw Savant File and Remove Uneeded Columns and Place Outcomes

#-------------------Current Year Loads---------------

def current_weather(yr):
    savant_wx_load(yr)
    weather_file_create(yr)
    savant_wx_parse(yr)
    savant_wx_assign(yr)
    wx_neutralize(yr)
    impute_parkfac(yr)
    park_neutralize(yr)
    
def weather_file_create(yr):
    
    d_file = pd.read_csv('config\\dates.csv')
    d_frame = d_file[d_file['Played'] == 1]
    t_file = pd.read_csv('config\\teamnames.csv')
    
    a = 0
    g = len(d_frame)
    ct = 0
    
    for a in range(g):
        
        m = str(d_frame.loc[a, 'Month'])
        d = str(d_frame.loc[a, 'Day'])
        
        if len(m) < 2:
            m = '0' + str(m)
        if len(d) < 2:
            d = '0' + str(d)
            
        w_file = pd.read_csv('scores\\score_data_' + str(m) + '_' + str(d) + '_' + str(yr) + '.csv')
        w_merge = w_file.merge(t_file, left_on = 'act_team', right_on = 'Abb')
        
        w_slice = w_merge[['Full', 'year', 'month', 'day', 'temp', 'winddir', 'windspd']]
        w_slice.rename(columns={'Full':'hometeam'}, inplace=True)
        w_slice.rename(columns={'windspd':'windspeed'}, inplace=True)
        
        if ct == 0:
            
            w_bind = w_slice
        else:
            w_bind = pd.concat([w_bind, w_slice], axis='rows')
        
        ct += 1
    
    w_bind.to_csv('weather\\weather_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
def impute_parkfac(yr):
    
    this_year_file = pd.read_csv('stats\\wn_parkfactors_' + str(yr) + '.csv')
    pps_file = pd.read_csv('pps\\wnparkfac_' + str(yr) + '.csv')
    wx_file = pd.read_csv("savant\\savant_wx_" + str(yr) + ".csv")
    wx_line = len(wx_file)
    out_file = pd.read_csv('pps\\wnparkfac_' + str(yr) + '.csv')
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    print(out_file)
    
    if wx_line < 100000:
        actual_pct = wx_line / 100000
        impute_pct = 1 - actual_pct
        print(wx_line)
        
        a = 0
        g = len(pps_file)
        
        for a in range(g):
            
            st = pps_file.iloc[a, 0]
            tm = pps_file.iloc[a, 1]
            
            ty_line = this_year_file[(this_year_file['stand'] == st) & (this_year_file['team'] == tm)]
            
            if len(ty_line) > 0:
                
                dbl_check = float(ty_line['double'])
                if np.isnan(dbl_check) == False:
                    
                    ty_calc = ty_line.loc[:, 'double':'walk']
                    pp_calc = pps_file.loc[a, 'double':'walk']
                    ty_actual = ty_calc * actual_pct
                    ty_impute = pp_calc * impute_pct
                    
                    ty_comb = ty_actual + ty_impute
                    
                    out_file.at[a, at_list] = ty_comb.iloc[0, :]
                    
                    if (tm == 'WSH'):
                        print(st)
                        print(ty_comb)
                
    
    out_file.to_csv('stats\\wn_parkfactors_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')

                
                
        
    

#---------------------------PAST LOADS------------------------
        
def savant_wx_load(yr):
    
    savant_file = pd.read_csv('savant\\savant_' + str(yr) + '.csv')
    savant_file = savant_file.iloc[:,[1, 5, 6, 7, 8, 9, 15, 17, 18, 19, 20, 23, 35, 36, 43, 58, 76, 77]]
    savant_plays = pd.read_csv('savant\\play_types.csv')
    savant_use = savant_file
    #Join Savant Event Types to Savant Frame
    savant_outcome = savant_use.merge(savant_plays, on='events')
    #Remove Rows assigned "Remove"
    savant_outcome_filt = savant_outcome[savant_outcome['actual_type'] != 'remove']
    #Assign rows assigned "Check" Various out values
    savant_outcome_filt.loc[savant_outcome_filt.actual_type == 'check', 'actual_type'] = savant_outcome_filt[savant_outcome_filt.actual_type == 'check']['bb_type']
    savant_outcome_filt.to_csv('savant\\savant_tmp2.csv', index = False, header = True, encoding='utf-8-sig')

#Merge Weather to Savant File, Drop 2nd Game of Double Headers to prevent One Too Many Join
def savant_wx_parse(yr):
    
    savant_use = pd.read_csv('savant\\savant_tmp2.csv')
    team_names = pd.read_csv('config\\teamnames.csv')
    team_names = team_names[['Full', 'Abb']]
    weather_file = pd.read_csv('weather\\weather_' + str(yr) + '.csv')
    weather_file['counter'] = 1
    weather_group = weather_file.groupby(['hometeam', 'year', 'month', 'day'])['counter'].agg('count')
    weather_group = weather_group.reset_index()
    double_headers = weather_group[weather_group['counter'] > 1]
    double_headers = double_headers.reset_index()
    
    a = 0
    g = len(double_headers)
    
    for a in range(g):
        ht = double_headers.loc[a, 'hometeam']
        ye = double_headers.loc[a, 'year']
        mo = double_headers.loc[a, 'month']
        da = double_headers.loc[a, 'day']
        dh_find = weather_file[(weather_file['hometeam'] == ht) & (weather_file['year'] == ye) & (weather_file['day'] == da) & (weather_file['month'] == mo)]
        index_2 = dh_find.index[1]
        weather_file.drop(index_2, inplace=True)
    
    weather_file.reset_index()

    if (yr >= 2016) & (yr <= 2019):
        new = savant_use["game_date"].str.split("/", n = 2, expand = True)
        savant_use['year'] = new[2].astype(int)
        savant_use['month'] = new[0].astype(int)
        savant_use['day'] = new[1].astype(int)
    elif yr == 2021:
        new = savant_use["game_date"].str.split("/", n = 2, expand = True)
        savant_use['year'] = new[2].astype(int)
        savant_use['month'] = new[0].astype(int)
        savant_use['day'] = new[1].astype(int)
    else:
        new = savant_use["game_date"].str.split("-", n = 2, expand = True)
        savant_use['year'] = new[0].astype(int)
        savant_use['month'] = new[1].astype(int)
        savant_use['day'] = new[2].astype(int)
    print(new)


    savant_merge = savant_use.merge(team_names, left_on = 'home_team', right_on = 'Abb', how = 'inner')
    
    savant_wx_merge = savant_merge.merge(weather_file, left_on = ['Full', 'year', 'month', 'day'], right_on = ['hometeam', 'year', 'month', 'day'], how = 'inner')
    savant_wx_merge.drop(['hometeam', 'Full', 'Abb'], axis = 'columns', inplace=True)
    
    file_n = "savant\\savant_wx_" + str(yr) + ".csv"
    
    savant_wx_merge.to_csv(file_n, index = False, header = True, encoding='utf-8-sig')

#Assign Wind In and Wind Out Columns, Then Group by Game Pack to Generate Stat Average Per Game, Subtract League Averages to Get Over Average Values
def savant_wx_assign(yr):
    
    wx_use = "savant\\savant_wx_" + str(yr) + ".csv"
    wx_use = pd.read_csv(wx_use)
    wx_use['out'] = 0
    wx_use['in'] = 0
    
    wx_out = (wx_use['winddir'] == 'tolf') | (wx_use['winddir'] == 'torf') | (wx_use['winddir'] == 'tocf')
    wx_use.loc[wx_out, 'out'] = 1
    
    wx_in = (wx_use['winddir'] == 'fromlf') | (wx_use['winddir'] == 'fromrf') | (wx_use['winddir'] == 'fromcf')
    wx_use.loc[wx_in, 'in'] = 1

    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    a_t = wx_use['actual_type']
    
    for a in at_list:
        wx_use[a] = 0
        x = a_t[a_t == a].index
        wx_use.loc[x, a] = 1
        
    
    wx_group = wx_use.groupby('game_pk')[['temp', 'windspeed', 'out', 'in', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']].agg('mean')
    
    #Pull League Averages
    la_file = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    la_col = la_file['ALL']
    
    double_avg = la_col.loc[0, ]
    flyball_avg = la_col.loc[1, ]
    groundball_avg = la_col.loc[2, ]
    hr_avg = la_col.loc[3, ]
    linedrive_avg = la_col.loc[4, ]
    popup_avg = la_col.loc[5, ]
    sacb_avg = la_col.loc[6, ]
    single_avg = la_col.loc[7, ]
    strikeout_avg = la_col.loc[8, ]
    triple_avg = la_col.loc[9, ]
    walk_avg = la_col.loc[10, ]
    
    wx_group['double'] = wx_group['double'].transform(lambda x: x - double_avg)
    wx_group['fly_ball'] = wx_group['fly_ball'].transform(lambda x: x - flyball_avg)
    wx_group['ground_ball'] = wx_group['ground_ball'].transform(lambda x: x - groundball_avg)
    wx_group['hr'] = wx_group['hr'].transform(lambda x: x - hr_avg)
    wx_group['line_drive'] = wx_group['line_drive'].transform(lambda x: x - linedrive_avg)
    wx_group['popup'] = wx_group['popup'].transform(lambda x: x - popup_avg)
    wx_group['sacb'] = wx_group['sacb'].transform(lambda x: x - sacb_avg)
    wx_group['single'] = wx_group['single'].transform(lambda x: x - single_avg)
    wx_group['strikeout'] = wx_group['strikeout'].transform(lambda x: x - strikeout_avg)
    wx_group['triple'] = wx_group['triple'].transform(lambda x: x - triple_avg)
    wx_group['walk'] = wx_group['walk'].transform(lambda x: x - walk_avg)
    
    fn_use = 'weather\\weatherreg_' + str(yr) + '.csv'
    wx_group.to_csv(fn_use, index = False, header = True, encoding='utf-8-sig')
    
#OVER LEAGUE AVERAGE!!!
#Binds Selected Years of Weather Data to Form a Master Regression Database
def savant_wx_binder(syr, eyr):
    
    a = syr
    ct = 0
    while a <= eyr:
        
        wx_file = 'weather\\weatherreg_' + str(a) + '.csv'
        wx_open = pd.read_csv(wx_file)
        if (ct == 0):
            wx_bind = wx_open
        else:
            wx_bind = pd.concat([wx_bind, wx_open], axis='rows')
        
        ct += 1
        a += 1
    
    wx_bind.to_csv('weather\\weather_reg_master.csv', index = False, header = True, encoding='utf-8-sig')
    
#Fit Regressions to Weather Splits of Wind In, Wind Out, Wind Neutral
def wx_regression_fit():
    
    reg_file = pd.read_csv('weather\\weather_reg_master.csv')
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    wx_out_frame = reg_file[reg_file['out'] == 1]
    wx_in_frame = reg_file[reg_file['in'] == 1]
    wx_neut_frame = reg_file[(reg_file['in'] == 0) & (reg_file['out']) == 0]
    
    out_x = wx_out_frame[['temp', 'windspeed']]
    in_x = wx_in_frame[['temp', 'windspeed']]
    neut_x = wx_neut_frame[['temp', 'windspeed']]
    
    for a in at_list:
        
        out_reg = LinearRegression()
        in_reg = LinearRegression()
        neut_reg = LinearRegression()
        
        out_y = wx_out_frame[a]
        in_y = wx_in_frame[a]
        neut_y = wx_neut_frame[a]
        
        out_reg.fit(out_x, out_y)
        in_reg.fit(in_x, in_y)
        neut_reg.fit(neut_x, neut_y)
        
        f_out = 'weather\\wxout_' + str(a) + '_fit.sav'
        f_in = 'weather\\wxin_' + str(a) + '_fit.sav'
        f_neut = 'weather\\wxneut_' + str(a) + '_fit.sav'
        
        pickle.dump(out_reg, open(f_out, 'wb'))
        pickle.dump(in_reg, open(f_in, 'wb'))
        pickle.dump(neut_reg, open(f_neut, 'wb'))
        

#Use Savant + Weather Data to Calculate Regression Predictions Based on Weather to Group By
def wx_neutralize(yr):
    
    master_wx_frame = pd.read_csv('savant\\savant_wx_' + str(yr) + '.csv')
    master_wx_frame['out'] = 0
    master_wx_frame['in'] = 0
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    wx_out = (master_wx_frame['winddir'] == 'tolf') | (master_wx_frame['winddir'] == 'torf') | (master_wx_frame['winddir'] == 'tocf')
    master_wx_frame.loc[wx_out, 'out'] = 1
    
    wx_in = (master_wx_frame['winddir'] == 'fromlf') | (master_wx_frame['winddir'] == 'fromrf') | (master_wx_frame['winddir'] == 'fromcf')
    master_wx_frame.loc[wx_in, 'in'] = 1
   
    #Wind Out
    wx_out_slice = master_wx_frame[master_wx_frame['out'] == 1]
    wx_out_slice_x = wx_out_slice[['temp', 'windspeed']]
    
    for a in at_list:
       
        file_name = 'weather\\wxout_' + str(a) + '_fit.sav'
        fit_load = pickle.load(open(file_name, 'rb'))
        fit_pred = fit_load.predict(wx_out_slice_x)
        wx_out_slice[a] = fit_pred
        act_use = str(a) + '_act'
        act_log = wx_out_slice['actual_type'] == a
        wx_out_slice[act_use] = act_log
        wx_out_slice[act_use] = wx_out_slice[act_use].astype(int)
        neut_use = str(a) + '_neut'
        neut_comb = wx_out_slice[a] + wx_out_slice[act_use]
        wx_out_slice[neut_use] = neut_comb
        
    #Wind In
    wx_in_slice = master_wx_frame[master_wx_frame['in'] == 1]
    wx_in_slice_x = wx_in_slice[['temp', 'windspeed']]
    
    for a in at_list:
       
        file_name = 'weather\\wxin_' + str(a) + '_fit.sav'
        fit_load = pickle.load(open(file_name, 'rb'))
        fit_pred = fit_load.predict(wx_in_slice_x)
        wx_in_slice[a] = fit_pred
        act_use = str(a) + '_act'
        act_log = wx_in_slice['actual_type'] == a
        wx_in_slice[act_use] = act_log
        wx_in_slice[act_use] = wx_in_slice[act_use].astype(int)
        neut_use = str(a) + '_neut'
        neut_comb = wx_in_slice[a] + wx_in_slice[act_use]
        wx_in_slice[neut_use] = neut_comb
        
    #Wind Neutral
    wx_neut_slice = master_wx_frame[(master_wx_frame['in'] == 0) & (master_wx_frame['out'] == 0)]
    wx_neut_slice_x = wx_neut_slice[['temp', 'windspeed']]
    
    for a in at_list:
       
        file_name = 'weather\\wxneut_' + str(a) + '_fit.sav'
        fit_load = pickle.load(open(file_name, 'rb'))
        fit_pred = fit_load.predict(wx_neut_slice_x)
        wx_neut_slice[a] = fit_pred
        act_use = str(a) + '_act'
        act_log = wx_neut_slice['actual_type'] == a
        wx_neut_slice[act_use] = act_log
        wx_neut_slice[act_use] = wx_neut_slice[act_use].astype(int)
        neut_use = str(a) + '_neut'
        neut_comb = wx_neut_slice[a] + wx_neut_slice[act_use]
        wx_neut_slice[neut_use] = neut_comb
    
    wx_bind = pd.concat([wx_out_slice, wx_in_slice, wx_neut_slice], axis='rows')

    #Batters
    wx_bat_groups = wx_bind.groupby(['batter', 'stand', 'p_throws'])[at_list].agg('mean')
    wx_bat_groups = wx_bat_groups.reset_index()
    
    stand_split = ['L', 'R']
    throw_split = ['L', 'R']
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = wx_bat_groups[(wx_bat_groups['stand'] == st) & (wx_bat_groups['p_throws'] == th)]
            
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_vla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_wnvla_bat_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_vla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_wnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_vla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_wnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_vla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_wnvla_bat_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='batter')
            
            print(wx_slice_merge)
            
            pa_use = wx_slice_merge['pa'].astype(float)
            
            a = 0
            g = len(pa_use)
            for a in range(g):
                paa = pa_use[a]
                pid = wx_slice_merge.loc[a, 'batter']
                if (paa > 15):
                    pa_use[a] = 1
                else:
                    pa_use[a] = paa / 15
                    
            
            for a2 in at_list:
                        
                stat_x = str(a2) + '_x'
                stat_y = str(a2) + '_y'
                        
                comb_stat = (wx_slice_merge[stat_y] - (wx_slice_merge[stat_x] * pa_use))
                        
                wx_slice_merge[a2] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')
                
    
    #Pitchers
    wx_bat_groups = wx_bind.groupby(['pitcher', 'stand', 'p_throws'])[at_list].agg('mean')
    wx_bat_groups = wx_bat_groups.reset_index()
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = wx_bat_groups[(wx_bat_groups['stand'] == st) & (wx_bat_groups['p_throws'] == th)]
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_vla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_wnvla_pit_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_vla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_wnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_vla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_wnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_vla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_wnvla_pit_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='pitcher')
            pa_use = wx_slice_merge['pa'].astype(float)
            
            a = 0
            g = len(pa_use)
            for a in range(g):
                paa = pa_use[a]

                if (paa > 15):
                    pa_use[a] = 1
                else:
                    pa_use[a] = paa / 15          

            for a in at_list:
                    
                stat_x = str(a) + '_x'
                stat_y = str(a) + '_y'
                    
                comb_stat = (wx_slice_merge[stat_y] - (wx_slice_merge[stat_x] * pa_use))
                    
                wx_slice_merge[a] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')

    wx_bind.to_csv('savant\\wnx_tmp.csv', index=False, header=True, encoding='utf-8-sig')

#Calculate Weather Neutralized Park Factors
def wn_parkfac(yr):
    
    tm_list = pd.read_csv('config\\teamnames.csv')
    tm_abb_list = tm_list['Abb']
    at_list = ['double_neut', 'fly_ball_neut', 'ground_ball_neut', 'hr_neut', 'line_drive_neut', 
               'popup_neut', 'sacb_neut', 'single_neut', 'strikeout_neut', 'triple_neut', 'walk_neut']
    wx_bind = pd.read_csv('savant\\wnx_tmp.csv')
    wx_park_group = wx_bind.groupby(['stand', 'home_team', 'away_team', 'inning_topbot'])[at_list].agg('mean')
    wx_park_group = wx_park_group.reset_index()
    
    ct = 0
    
    for tm in tm_abb_list:

        #Left, Home Team Batting Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'L') & (wx_park_group['home_team'] == tm) & (wx_park_group['inning_topbot'] == 'Bot')]
        wx_slice_group_hlb = wx_park_slice.groupby(['stand', 'home_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_hlb = wx_slice_group_hlb.reset_index()
        
        #Left, Home Team Pitching Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'L') & (wx_park_group['home_team'] == tm) & (wx_park_group['inning_topbot'] == 'Top')]
        wx_slice_group_hlp = wx_park_slice.groupby(['stand', 'home_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_hlp = wx_slice_group_hlp.reset_index()
        
        #Left, Away Team Batting Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'L') & (wx_park_group['away_team'] == tm) & (wx_park_group['inning_topbot'] == 'Top')]
        wx_slice_group_alb = wx_park_slice.groupby(['stand', 'away_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_alb = wx_slice_group_alb.reset_index()
        
        #Left, Away Team Pitching Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'L') & (wx_park_group['away_team'] == tm) & (wx_park_group['inning_topbot'] == 'Bot')]
        wx_slice_group_alp = wx_park_slice.groupby(['stand', 'away_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_alp = wx_slice_group_alp.reset_index()
        
        #Right, Home Team Batting Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'R') & (wx_park_group['home_team'] == tm) & (wx_park_group['inning_topbot'] == 'Bot')]
        wx_slice_group_hrb = wx_park_slice.groupby(['stand', 'home_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_hrb = wx_slice_group_hrb.reset_index()
        
        #Right, Home Team Pitching Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'R') & (wx_park_group['home_team'] == tm) & (wx_park_group['inning_topbot'] == 'Top')]
        wx_slice_group_hrp = wx_park_slice.groupby(['stand', 'home_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_hrp = wx_slice_group_hrp.reset_index()
        
        #Right, Away Team Batting Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'R') & (wx_park_group['away_team'] == tm) & (wx_park_group['inning_topbot'] == 'Top')]
        wx_slice_group_arb = wx_park_slice.groupby(['stand', 'away_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_arb = wx_slice_group_arb.reset_index()
        
        #Right, Away Team Pitching Stats
        wx_park_slice = wx_park_group[(wx_park_group['stand'] == 'R') & (wx_park_group['away_team'] == tm) & (wx_park_group['inning_topbot'] == 'Bot')]
        wx_slice_group_arp = wx_park_slice.groupby(['stand', 'away_team', 'inning_topbot'])[at_list].agg('mean')
        wx_slice_group_arp = wx_slice_group_arp.reset_index()
        
        team_conc_home = pd.concat([wx_slice_group_hlb, wx_slice_group_hlp, wx_slice_group_hrb, wx_slice_group_hrp], axis='rows')
        team_conc_away = pd.concat([wx_slice_group_alb, wx_slice_group_alp, wx_slice_group_arb, wx_slice_group_arp], axis='rows')
        
        if ct == 0:
            master_home_conc = team_conc_home
            master_away_conc = team_conc_away
        else:
            master_home_conc = pd.concat([master_home_conc, team_conc_home], axis='rows')
            master_away_conc = pd.concat([master_away_conc, team_conc_away], axis='rows')
        
        ct += 1
        
    team_list = master_home_conc.groupby('home_team')['strikeout_neut'].agg('count')
    team_list = team_list.reset_index()
    team_list = team_list['home_team']
    
    la_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    la_frame = la_frame.pivot_table(columns='actual_type', values='ALL')
    la_frame = la_frame.reset_index()
    la_frame.drop(['index'], inplace=True, axis='columns')
    la_frame = la_frame.iloc[:, 0:11]
    
    ct = 0
    
    for tm in team_list:
        
        #Left Hand
        home_slice_l = master_home_conc[(master_home_conc['stand'] == 'L') & (master_home_conc['home_team'] == tm)]
        home_slice_l = home_slice_l.groupby(['stand', 'home_team'])[at_list].agg('mean')
        home_slice_l = home_slice_l.reset_index()
        home_slice_l.rename(columns={'home_team':'team', 'double_neut':'double', 'fly_ball_neut':'fly_ball', 'ground_ball_neut':'ground_ball',
                                     'hr_neut':'hr', 'line_drive_neut':'line_drive', 'popup_neut':'popup', 'sacb_neut':'sacb',
                                     'single_neut':'single', 'strikeout_neut':'strikeout', 'triple_neut':'triple', 'walk_neut':'walk'}, inplace=True)
        
        away_slice_l = master_away_conc[(master_away_conc['stand'] == 'L') & (master_away_conc['away_team'] == tm)]
        away_slice_l = away_slice_l.groupby(['stand', 'away_team'])[at_list].agg('mean')
        away_slice_l = away_slice_l.reset_index()
        away_slice_l.rename(columns={'away_team':'team', 'double_neut':'double', 'fly_ball_neut':'fly_ball', 'ground_ball_neut':'ground_ball',
                                     'hr_neut':'hr', 'line_drive_neut':'line_drive', 'popup_neut':'popup', 'sacb_neut':'sacb',
                                     'single_neut':'single', 'strikeout_neut':'strikeout', 'triple_neut':'triple', 'walk_neut':'walk'}, inplace=True)
        
        fill_slice_l = home_slice_l
        
        
        home_slicer_l = home_slice_l.loc[:, 'double':'walk']
        home_slice_l_vla = home_slicer_l - la_frame
        away_slicer_l = away_slice_l.loc[:, 'double':'walk']
        away_slice_l_vla = away_slicer_l - la_frame
        
        park_fac_l = home_slice_l_vla - away_slice_l_vla
        print(tm)
        print(home_slice_l_vla)
        print(away_slice_l_vla)
        print(park_fac_l)
        
        fill_slice_l.iloc[:, 2:13] = park_fac_l
        
        #Right Hand
        home_slice_l = master_home_conc[(master_home_conc['stand'] == 'R') & (master_home_conc['home_team'] == tm)]
        home_slice_l = home_slice_l.groupby(['stand', 'home_team'])[at_list].agg('mean')
        home_slice_l = home_slice_l.reset_index()
        home_slice_l.rename(columns={'home_team':'team', 'double_neut':'double', 'fly_ball_neut':'fly_ball', 'ground_ball_neut':'ground_ball',
                                     'hr_neut':'hr', 'line_drive_neut':'line_drive', 'popup_neut':'popup', 'sacb_neut':'sacb',
                                     'single_neut':'single', 'strikeout_neut':'strikeout', 'triple_neut':'triple', 'walk_neut':'walk'}, inplace=True)
        
        away_slice_l = master_away_conc[(master_away_conc['stand'] == 'R') & (master_away_conc['away_team'] == tm)]
        away_slice_l = away_slice_l.groupby(['stand', 'away_team'])[at_list].agg('mean')
        away_slice_l = away_slice_l.reset_index()
        away_slice_l.rename(columns={'away_team':'team', 'double_neut':'double', 'fly_ball_neut':'fly_ball', 'ground_ball_neut':'ground_ball',
                                     'hr_neut':'hr', 'line_drive_neut':'line_drive', 'popup_neut':'popup', 'sacb_neut':'sacb',
                                     'single_neut':'single', 'strikeout_neut':'strikeout', 'triple_neut':'triple', 'walk_neut':'walk'}, inplace=True)
        
        fill_slice_r = home_slice_l
        
        
        home_slicer_l = home_slice_l.loc[:, 'double':'walk']
        home_slice_l_vla = home_slicer_l - la_frame
        away_slicer_l = away_slice_l.loc[:, 'double':'walk']
        away_slice_l_vla = away_slicer_l - la_frame
        
        park_fac_r = home_slice_l_vla - away_slice_l_vla
        
        fill_slice_r.iloc[:, 2:13] = park_fac_r
        
        if ct == 0:
            park_factor_bind = pd.concat([fill_slice_l, fill_slice_r], axis='rows')
            park_factor_use = park_factor_bind
        else:
            park_factor_bind = pd.concat([fill_slice_l, fill_slice_r], axis='rows')
            park_factor_use = pd.concat([park_factor_use, park_factor_bind], axis='rows')
            
        ct = ct + 1
            
    
    out_file = 'stats\\wn_parkfactors_' + str(yr) + '.csv'
    park_factor_use.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
def park_neutralize(yr):
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    savant_file = pd.read_csv('savant\\\savant_wx_' + str(yr) + '.csv')
    park_factors = pd.read_csv('stats\\wn_parkfactors_' + str(yr) + '.csv')
    
    savant_merge = savant_file.merge(park_factors, left_on = ['stand', 'home_team'], right_on = ['stand', 'team'], how = 'inner')
    
    
    #Batters
    
    park_bat_groups = savant_merge.groupby(['stand', 'p_throws', 'batter'])[at_list].agg('mean')
    park_bat_groups = park_bat_groups.reset_index()
    
    
    stand_split = ['L', 'R']
    throw_split = ['L', 'R']
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = park_bat_groups[(park_bat_groups['stand'] == st) & (park_bat_groups['p_throws'] == th)]
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_wnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_pkwnvla_bat_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_wnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_pkwnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_wnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_pkwnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_wnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_pkwnvla_bat_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='batter')
            #print(wx_slice_merge[['batter', 'p_throws', 'stand', 'hr_x', 'hr_y']])
            pa_use = wx_slice_merge['pa'].astype(float)
            a = 0
            g = len(pa_use)
            for a in range(g):
                paa = pa_use[a]
                if (paa > 15):
                    pa_use[a] = 1
                else:
                    pa_use[a] = paa / 15
                
            for a in at_list:
                    
                stat_x = str(a) + '_x'
                stat_y = str(a) + '_y'
                    
                comb_stat = (wx_slice_merge[stat_y] - (wx_slice_merge[stat_x] * pa_use))
                    
                wx_slice_merge[a] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')
    
    #Pitchers
    
    park_bat_groups = savant_merge.groupby(['stand', 'p_throws', 'pitcher'])[at_list].agg('mean')
    park_bat_groups = park_bat_groups.reset_index()
    
    
    stand_split = ['L', 'R']
    throw_split = ['L', 'R']
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = park_bat_groups[(park_bat_groups['stand'] == st) & (park_bat_groups['p_throws'] == th)]
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_wnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_pkwnvla_pit_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_wnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_pkwnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_wnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_pkwnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_wnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_pkwnvla_pit_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='pitcher')
            #print(wx_slice_merge[['batter', 'p_throws', 'stand', 'hr_x', 'hr_y']])
            pa_use = wx_slice_merge['pa'].astype(float)
            a = 0
            g = len(pa_use)
            for a in range(g):
                paa = pa_use[a]
                if (paa > 15):
                    pa_use[a] = 1
                else:
                    pa_use[a] = paa / 15
                
            for a in at_list:
                    
                stat_x = str(a) + '_x'
                stat_y = str(a) + '_y'
                    
                comb_stat = (wx_slice_merge[stat_y] - (wx_slice_merge[stat_x] * pa_use))
                    
                wx_slice_merge[a] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')
    
        
def weather_do_all(yr):
    savant_wx_load(yr)
    savant_wx_parse(yr)
    savant_wx_assign(yr)
    wx_neutralize(yr)
    wn_parkfac(yr)
    park_neutralize(yr)
