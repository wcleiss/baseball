#This script is the first step of my baseball model's process as it takes the raw statcast data from MLB and cleans it, transforms it, and structures it.
#The script is divided into two parts, current year is the list of processes for current season's stats, and then there is a separate process for previous seasons.
#The main reason current year has a different process is so it can impute statistics of players with not enough sample size with their preseason projections.
#the script calculates batter/pitcher averages, then calculates their stats vs. the league average, imputes their preseason estimates into fill gaps
#The script also calculates the average number of pitches batters/pitchers accumulate in an at bat, the max number of pitches a pitcher throws in an appearance
#Errors are also calculated

import pandas as pd
import numpy as np

#----------------CURRENT YEAR---------------------

def current_wrangle(yr):
    savant_wrangle_init(yr)
    savant_wrangle_parse(yr)
    savant_bf_pa(yr)
    savant_batter_avgs(yr)
    savant_pitcher_avgs(yr)
    savant_avgs(yr)
    league_avg_impute(yr)
    savant_vla_cy(yr)
    cy_impute(yr)
    savant_pitches(yr)
    savant_maxpitch(yr)
    savant_errors(yr)
    numpitch_impute(yr)
    error_impute(yr)
    savant_master_ids_2(2015, yr)
    savant_ids_2()
    savant_new(yr)
    


def league_avg_impute(yr):
    
    this_year_savant = pd.read_csv('savant\\savant_' + str(yr) + '.csv')
    current_avgs = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    pps_avgs = pd.read_csv('pps\\league_splits_' + str(yr) + '.csv')
    output_avgs = pd.read_csv('pps\\league_splits_' + str(yr) + '.csv')
    
    this_year_plays = len(this_year_savant)
    
    if this_year_plays < 15000:
        
        actual_use = this_year_plays / 15000
        impute_use = 1 - actual_use
        
        c_a = current_avgs.iloc[:, 1:]
        i_a = pps_avgs.iloc[:, 1:]
        
        c_a_calc = c_a * actual_use
        i_a_calc = i_a * impute_use
        
        comb_calc = c_a_calc + i_a_calc
        
        output_avgs.iloc[:, 1:] = comb_calc
        
        output_avgs.to_csv('stats\\league_splits_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
        
def savant_vla_cy(yr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    avg_list = ['LVLO', 'LVRO', 'RVLO', 'RVRO']
    avg_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    bf_pa_frame = pd.read_csv('stats\\bf_pa_frame_' + str(yr) + '.csv')
    bf_pa_frame = bf_pa_frame[['player', 'player_type']]
    
    
    a = 0
    g = len(split_list) - 1
    while a <= g:
        
        split_use = split_list[a]
        avg_use = avg_list[a]
        
        for b in type_list:
            
            file_name = 'stats\\' + str(split_use) + '_' + str(b) + '_avg_' + str(yr) + '.csv'
            file_frame = pd.read_csv(file_name)
            
            for c in at_list:
                
                la_use = avg_frame[avg_frame['actual_type'] == c][avg_use]
                file_frame[c] = file_frame[c].transform(lambda x: x - la_use)
            
            if b == 'bat':
                file_frame = file_frame.merge(bf_pa_frame, left_on = 'batter', right_on = 'player')
            else:
                file_frame = file_frame.merge(bf_pa_frame, left_on = 'pitcher', right_on = 'player')
                
            file_frame.drop('player', axis='columns', inplace=True)
                
            file_frame.to_csv('stats\\' + str(split_use) + '_vlap_' + str(b) + '_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')

        a += 1
        
def cy_impute(yr):
    
    thresh_hold = 125
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    bf_pa_frame = pd.read_csv('stats\\bf_pa_frame_' + str(yr) + '.csv')
    
    for s in split_list:
        
        for t in type_list:
            
            this_year_file = pd.read_csv('stats\\' + str(s) + '_vlap_' + str(t) + '_' + str(yr) + '.csv')
            pps_file = pd.read_csv('pps\\proj_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv')
            rookie_file = pd.read_csv('pps\\rookie_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv')
            out_file = pd.read_csv('stats\\' + str(s) + '_vlap_' + str(t) + '_' + str(yr) + '.csv')
            
            a = 0
            g = len(this_year_file)
            
            for a in range(g):
                
                pa = this_year_file.loc[a, 'pa']
                if t == 'bat':
                    pid = this_year_file.loc[a, 'batter']
                else:
                    pid = this_year_file.loc[a, 'pitcher']
                
                if pa < thresh_hold:
                         
                    if t == 'bat':
                        ty_line = this_year_file[this_year_file['batter'] == pid]
                        pp_line = pps_file[pps_file['batter'] == pid]
                    else:
                        ty_line = this_year_file[this_year_file['pitcher'] == pid]
                        pp_line = pps_file[pps_file['pitcher'] == pid]
                        
                    pp_line_len = len(pp_line)
                    actual_use = pa / thresh_hold
                    impute_use = 1 - actual_use
                    
                    #Existing Player
                    
                    if pp_line_len > 0:

                        ty_line_short = ty_line[at_list]
                        pp_line_short = pp_line[at_list]
                        
                        ty_line_calc = ty_line_short * actual_use
                        pp_line_calc = pp_line_short * impute_use
                        ty_line_calc = ty_line_calc.reset_index()
                        pp_line_calc = pp_line_calc.reset_index()
                        cc_line_calc = ty_line_calc + pp_line_calc
                        cc_line_calc = cc_line_calc[at_list]
                        
                        if (pid == 542303):
                            print(str(s) + '_' + str(t))
                            print(ty_line['hr'])
                            print(pp_line['hr'])
                            print(cc_line_calc['hr'])
                        
                    
                    #Non Existing Player
                    
                    else:
                        
                        player_type = bf_pa_frame[bf_pa_frame['player'] == pid]
                        player_type = player_type.iloc[0, 3]
                        
                        if t == 'bat':
                            if player_type == 'B':
                                pp_line = rookie_file[rookie_file['player_type'] == 'B']
                            else:
                                pp_line = rookie_file[rookie_file['player_type'] == 'P']
                        else:
                            pp_line = rookie_file[rookie_file['player_type'] == 'P']
                        
                        ty_line_short = ty_line[at_list]
                        pp_line_short = pp_line[at_list]

                        
                        ty_line_calc = ty_line_short * actual_use
                        pp_line_calc = pp_line_short * impute_use
                        ty_line_calc = ty_line_calc.reset_index()
                        pp_line_calc = pp_line_calc.reset_index()
                        cc_line_calc = ty_line_calc + pp_line_calc
                        cc_line_calc = cc_line_calc[at_list]
                        

                        
                        
                    out_file.at[a, at_list] = cc_line_calc.iloc[0, :]
            
            out_file.to_csv('stats\\' + str(s) + '_vla_' + str(t) + '_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')

def numpitch_impute(yr):
    
    thresh = 60
    
    #Batters
    
    ty_file = pd.read_csv('stats\\numpitch_bat_' + str(yr) + '.csv')
    pp_file = pd.read_csv('pps\\numpitch_bat_' + str(yr) + '.csv')
    
    file_merge = pp_file.merge(ty_file, on = 'batter', how = 'left')
    
    out_file = pd.read_csv('pps\\numpitch_bat_' + str(yr) + '.csv')
    
    print(file_merge)
    
    a = 0
    g = len(file_merge)
    
    for a in range(g):
        
        ty_stat = file_merge.iloc[a, 5]
        pp_stat = file_merge.iloc[a, 1]
        ab_stat = file_merge.iloc[a, 3]
        
        if np.isnan(ab_stat) == True:
            cc_calc = pp_stat
        
        else:
            if ab_stat < thresh:
                
                actual_use = ab_stat / thresh
                impute_use = 1 - actual_use
                
                ty_calc = ty_stat * actual_use
                pp_calc = pp_stat * impute_use
                cc_calc = ty_calc + pp_calc
            else:
                cc_calc = ty_stat
        
        out_file.iloc[a, 1] = cc_calc
    
    out_file.to_csv('stats\\numpitch_bat_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    #Pitchers
    
    ty_file = pd.read_csv('stats\\numpitch_pit_' + str(yr) + '.csv')
    pp_file = pd.read_csv('pps\\numpitch_pit_' + str(yr) + '.csv')
    
    file_merge = pp_file.merge(ty_file, on = 'pitcher', how = 'left')
    
    out_file = pd.read_csv('pps\\numpitch_pit_' + str(yr) + '.csv')
    
    print(file_merge)
    
    a = 0
    g = len(file_merge)
    
    for a in range(g):
        
        ty_stat = file_merge.iloc[a, 5]
        pp_stat = file_merge.iloc[a, 1]
        ab_stat = file_merge.iloc[a, 3]
        
        if np.isnan(ab_stat) == True:
            cc_calc = pp_stat
        
        else:
            if ab_stat < thresh:
                
                actual_use = ab_stat / thresh
                impute_use = 1 - actual_use
                
                ty_calc = ty_stat * actual_use
                pp_calc = pp_stat * impute_use
                cc_calc = ty_calc + pp_calc
            else:
                cc_calc = ty_stat
        
        out_file.iloc[a, 1] = cc_calc
    
    out_file.to_csv('stats\\numpitch_pit_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
            
            
def error_impute(yr):
    
    thresh = 800
    
    ty_file = pd.read_csv('stats\\errors_' + str(yr) + '.csv')
    pp_file = pd.read_csv('pps\\errors_' + str(yr) + '.csv')
    out_file = pd.read_csv('pps\\errors_' + str(yr) + '.csv')
    
        
    file_merge = pp_file.merge(ty_file, on = 'team', how = 'left')
        
    a = 0
    g = len(file_merge)
        
    for a in range(g):
        
        ty_stat = file_merge.iloc[a, 6]
        pp_stat = file_merge.iloc[a, 3]
        ch_stat = file_merge.iloc[a, 5]
            
        if ch_stat < thresh:
            
            actual_use = ch_stat / thresh
            impute_use = 1 - actual_use
            
            ty_calc = ty_stat * actual_use
            pp_calc = pp_stat * impute_use
            cc_calc = ty_calc + pp_calc
            
        else:
            
            cc_calc = ty_stat
            
        out_file.iloc[a, 3] = cc_calc
        
    out_file = out_file[['team', 'field_pct']]
    
    out_file.to_csv('stats\\errors_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
#Combine All Pitcher/Batter IDS into one master frame
def savant_master_ids_2(syr, eyr):
    
    a = syr
    ct = 0
    while (a <= eyr):
        
        bat_tmp = pd.read_csv('savant\\savant_batters_' + str(a) + '.csv')
        pit_tmp = pd.read_csv('savant\\savant_pitchers_' + str(a) + '.csv')
        all_tmp = pd.concat([bat_tmp, pit_tmp], axis='rows')
        
        if ct == 0:
            all_master = all_tmp
        else:
            all_master = pd.concat([all_master, all_tmp], axis='rows')
        
        a += 1
        ct += 1
    
    all_master['counter'] = 1
    all_group = all_master.groupby(['player_id', 'player_name'])['counter'].agg('count')
    all_group = all_group.reset_index()
    
    a = 1
    g = len(all_group)
    for a in range(g):
        
        ni = all_group.loc[a, 'player_name']
        if ',' not in ni:
            
            spl_list = ni.split(' ')
            fn = spl_list[0]
            ln = spl_list[1:]
            
            b = 0
            h = len(ln)
            for b in range(h):
                
                if b == 0:
                    ln_use = ln[b]
                else:
                    ln_use = str(ln_use) + ' ' + ln[b]
                
            cn = str(ln_use) + ', ' + str(fn)
            all_group.loc[a, 'player_name'] = cn
            print(all_group.loc[a, 'player_name'])
    
    all_group = all_group.groupby(['player_id', 'player_name'])['counter'].agg('count')
    all_group = all_group.reset_index()
    all_group.to_csv('savant\\master_ids2.csv', index=False, header=True, encoding='utf-8-sig')
                
#Initial Merge with Retro Player File, Flags Any Duplicates
#Must Manually Prune Duplicates 
def savant_ids_2():
    
    all_tmp = pd.read_csv('savant\\master_ids2.csv')
    all_tmp['counter'] = 1
    all_group = all_tmp
    new = all_group["player_name"].str.split(", ", n = 2, expand = True)
    all_group['nameFirst'] = new[1].astype(str)
    all_group['nameLast'] = new[0].astype(str)
    all_group['nameFull'] = 'N'
    a = 0
    g = len(all_group)
    for a in range(g):
        all_group.loc[a, 'nameFull'] = str(all_group.loc[a, 'nameFirst']) + ' ' + str(all_group.loc[a, 'nameLast'])
        
    player_file = pd.read_csv('retro\\players.csv')
    player_file = player_file[['birthYear', 'birthMonth', 'birthDay', 'nameFirst', 'nameLast', 'weight', 'height', 'bats', 'throws']]
    
    all_merge = all_group.merge(player_file, on = ['nameFirst', 'nameLast'], how = 'left')
    
    all_merge_group = all_merge.groupby(['player_id', 'player_name'])['counter'].agg('count')
    all_merge_group = all_merge_group.reset_index()
     
    multi_group = all_merge_group[all_merge_group['counter'] > 1]
    multi_group = multi_group.reset_index()
    
    a = 0
    g = len(multi_group)
    
    for a in range(g):
        
        i_d = multi_group.loc[a, 'player_id']
        id_find = all_merge[all_merge['player_id'] == i_d]
        all_merge.loc[id_find.index, 'counter'] = 2
    
    all_merge.to_csv('savant\\player_info2.csv', index=False, header=True, encoding='utf-8-sig')
    
def savant_new(yr):
    
    info_final = pd.read_csv('pps\\player_info_final.csv', encoding = "ISO-8859-1")
    info_two = pd.read_csv('savant\\master_ids2.csv')
    
    info_merge = info_two.merge(info_final, on = 'player_id', how = 'left')
    info_merge.to_csv('savant\\master_ids_new.csv', index=False, header=True, encoding='utf-8-sig')
    
    

    
 
            
#----------------PAST YEARS----------------------

#Create a List of All Batters from Year


def savant_stack(yr):
    
    date_list = ['aug', 'sep']
    ct = 0
    for d in date_list:
        file_name = 'savant_' + str(yr) + '_' + str(d) + '.csv'
        
        file_op = pd.read_csv(file_name)
        if ct == 0:
            savant_st = file_op
        else:
            savant_st = pd.concat([savant_st, file_op], axis='rows')
        
        ct += 1
    
    savant_st.to_csv('savant_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')

#Load Raw Savant CSV and Remove Uneeded Columns
def savant_wrangle_init(yr):
    savant_file = pd.read_csv('savant\\savant_' + str(yr) + '.csv')
    savant_file = savant_file.iloc[:,[1, 5, 6, 7, 8, 9, 15, 17, 18, 19, 20, 23, 35, 36, 43, 58, 76, 77]]
    savant_file.to_csv('savant\\savant_tmp.csv', index = False, header = True, encoding='utf-8-sig')
    

#Assign Outcomes and Filter Savant Frame
def savant_wrangle_parse(yr):
    savant_plays = pd.read_csv('savant\\play_types.csv')
    savant_use = pd.read_csv('savant\\savant_tmp.csv')
    #Join Savant Event Types to Savant Frame
    savant_outcome = savant_use.merge(savant_plays, on='events')
    #Remove Rows assigned "Remove"
    savant_outcome_filt = savant_outcome[savant_outcome['actual_type'] != 'remove']
    #Assign rows assigned "Check" Various out values
    savant_outcome_filt.loc[savant_outcome_filt.actual_type == 'check', 'actual_type'] = savant_outcome_filt[savant_outcome_filt.actual_type == 'check']['bb_type']
    savant_outcome_filt.to_csv('savant\\savant_tmp2.csv', index = False, header = True, encoding='utf-8-sig')

    
#Create At Bat and Batters Faced Frame to Determine Player Type
def savant_bf_pa(yr):
    savant_use = pd.read_csv('savant\\savant_tmp2.csv')
    true_outcomes = pd.read_csv('stats\\true_outcomes.csv')
    savant_use['counter'] = 1
    savant_battersfaced = savant_use[['pitcher', 'counter']].groupby('pitcher', as_index=False)['counter'].agg('count')
    savant_plateapps = savant_use[['batter', 'counter']].groupby('batter', as_index=False)['counter'].agg('count')
    savant_combo = savant_plateapps.merge(savant_battersfaced, right_on = 'pitcher', left_on = 'batter', how = 'outer', suffixes=('_pa', '_bf'))
    a = 0
    g = len(savant_combo) - 1
    while a <= g:
        b_id = savant_combo.loc[a, 'batter']
        p_id = savant_combo.loc[a, 'pitcher']
        bf = savant_combo.loc[a, 'counter_bf']
        pa = savant_combo.loc[a, 'counter_pa']
        if np.isnan(b_id) == True:
            savant_combo.loc[a, 'batter'] = p_id
        if np.isnan(bf) == True:
            savant_combo.loc[a, 'counter_bf'] = 0
        if np.isnan(pa) == True:
            savant_combo.loc[a, 'counter_pa'] = 0
        if savant_combo.loc[a, 'counter_bf'] > savant_combo.loc[a, 'counter_pa']:
            savant_combo.loc[a, 'player_type'] = 'P'
        else:
            savant_combo.loc[a, 'player_type'] = 'B'
        a += 1
    
    savant_combo.drop('pitcher', inplace=True, axis='columns')
    savant_combo.rename(columns={'batter': "player", 'counter_pa':'pa', 'counter_bf':'bf'}, inplace=True)
    
    
    print(savant_combo)
                        
    file_n = 'stats\\bf_pa_frame_' + str(yr) + '.csv'
    
    savant_combo.to_csv(file_n, index = False, header = True, encoding='utf-8-sig')
    
#Calculate Batter Split Averages
def savant_batter_avgs(yr):
    savant_use = pd.read_csv('savant\\savant_tmp2.csv')
    true_outcomes = pd.read_csv('stats\\true_outcomes.csv')
    savant_use['counter'] = 1
    savant_group_outcomes = savant_use[['stand', 'p_throws', 'player_name', 'batter', 'actual_type', 'counter']].groupby(['stand', 'p_throws', 'player_name', 'batter', 'actual_type'], as_index=False)['counter'].agg('count')

    #LEFT VS LEFT
    file_nms = 'stats\\lvl_bat_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\lvl_bat_avg_' + str(yr) + '.csv'
    lvl_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "L")]    
    lvl_wide_sum = lvl_frame.reset_index().pivot_table(index='batter', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    lvl_wide_sum['pa'] = lvl_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    lvl_wide_sum = lvl_wide_sum.reset_index()
    lvl_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    lvl_wide_avg = lvl_wide_sum
    
    a = 0
    g = len(lvl_wide_avg) - 1
    while a <= g:
        
        pa = lvl_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = lvl_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            lvl_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    lvl_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #LEFT VS RIGHT
    file_nms = 'stats\\lvr_bat_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\lvr_bat_avg_' + str(yr) + '.csv'
    lvr_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "R")]    
    lvr_wide_sum = lvr_frame.reset_index().pivot_table(index='batter', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    lvr_wide_sum['pa'] = lvr_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    lvr_wide_sum = lvr_wide_sum.reset_index()
    lvr_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    lvr_wide_avg = lvr_wide_sum
    
    a = 0
    g = len(lvr_wide_avg) - 1
    while a <= g:
        
        pa = lvr_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = lvr_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            lvr_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    lvr_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #RIGHT VS LEFT
    file_nms = 'stats\\rvl_bat_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\rvl_bat_avg_' + str(yr) + '.csv'
    rvl_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "L")]    
    rvl_wide_sum = rvl_frame.reset_index().pivot_table(index='batter', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    rvl_wide_sum['pa'] = rvl_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    rvl_wide_sum = rvl_wide_sum.reset_index()
    rvl_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    rvl_wide_avg = rvl_wide_sum
    
    a = 0
    g = len(rvl_wide_avg) - 1
    while a <= g:
        
        pa = rvl_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = rvl_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            rvl_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    rvl_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #RIGHT VS RIGHT
    file_nms = 'stats\\rvr_bat_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\rvr_bat_avg_' + str(yr) + '.csv'
    rvr_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "R")]    
    rvr_wide_sum = rvr_frame.reset_index().pivot_table(index='batter', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    rvr_wide_sum['pa'] = rvr_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    rvr_wide_sum = rvr_wide_sum.reset_index()
    rvr_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    rvr_wide_avg = rvr_wide_sum
    
    a = 0
    g = len(rvr_wide_avg) - 1
    while a <= g:
        
        pa = rvr_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = rvr_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            rvr_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    rvr_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')
    
    
#Calculate Batter Split Averages
def savant_pitcher_avgs(yr):
    savant_use = pd.read_csv('savant\\savant_tmp2.csv')
    true_outcomes = pd.read_csv('stats\\true_outcomes.csv')
    savant_use['counter'] = 1
    savant_group_outcomes = savant_use[['stand', 'p_throws', 'pitcher', 'actual_type', 'counter']].groupby(['stand', 'p_throws', 'pitcher', 'actual_type'], as_index=False)['counter'].agg('count')
    
    #LEFT VS LEFT
    file_nms = 'stats\\lvl_pit_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\lvl_pit_avg_' + str(yr) + '.csv'
    lvl_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "L")]    
    lvl_wide_sum = lvl_frame.reset_index().pivot_table(index='pitcher', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    lvl_wide_sum['pa'] = lvl_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    lvl_wide_sum = lvl_wide_sum.reset_index()
    lvl_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    lvl_wide_avg = lvl_wide_sum
    
    a = 0
    g = len(lvl_wide_avg) - 1
    while a <= g:
        
        pa = lvl_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = lvl_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            lvl_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    lvl_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #LEFT VS RIGHT
    file_nms = 'stats\\lvr_pit_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\lvr_pit_avg_' + str(yr) + '.csv'
    lvr_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "R")]    
    lvr_wide_sum = lvr_frame.reset_index().pivot_table(index='pitcher', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    lvr_wide_sum['pa'] = lvr_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    lvr_wide_sum = lvr_wide_sum.reset_index()
    lvr_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    lvr_wide_avg = lvr_wide_sum
    
    a = 0
    g = len(lvr_wide_avg) - 1
    while a <= g:
        
        pa = lvr_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = lvr_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            lvr_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    lvr_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #RIGHT VS LEFT
    file_nms = 'stats\\rvl_pit_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\rvl_pit_avg_' + str(yr) + '.csv'
    rvl_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "L")]    
    rvl_wide_sum = rvl_frame.reset_index().pivot_table(index='pitcher', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    rvl_wide_sum['pa'] = rvl_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    rvl_wide_sum = rvl_wide_sum.reset_index()
    rvl_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    rvl_wide_avg = rvl_wide_sum
    
    a = 0
    g = len(rvl_wide_avg) - 1
    while a <= g:
        
        pa = rvl_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = rvl_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            rvl_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    rvl_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')   
    
    #RIGHT VS RIGHT
    file_nms = 'stats\\rvr_pit_sum_' + str(yr) + '.csv'
    file_nma = 'stats\\rvr_pit_avg_' + str(yr) + '.csv'
    rvr_frame = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "R")]    
    rvr_wide_sum = rvr_frame.reset_index().pivot_table(index='pitcher', columns='actual_type', values='counter', fill_value=0, aggfunc='sum')
    rvr_wide_sum['pa'] = rvr_wide_sum.loc[:, 'double':'walk'].sum(axis='columns')
    rvr_wide_sum = rvr_wide_sum.reset_index()
    rvr_wide_sum.to_csv(file_nms, index = False, header = True, encoding='utf-8-sig')
    rvr_wide_avg = rvr_wide_sum
    
    a = 0
    g = len(rvr_wide_avg) - 1
    while a <= g:
        
        pa = rvr_wide_avg.loc[a, 'pa']
        b = 1
        h = 11
        while b <= h:
            
            stu = rvr_wide_avg.iloc[a, b]
            stu_avg = stu / pa
            rvr_wide_avg.iloc[a, b] = stu_avg
            b += 1
        a += 1

    rvr_wide_avg.to_csv(file_nma, index = False, header = True, encoding='utf-8-sig')
    

     
#Calcuate League Split Averages   
def savant_avgs(yr):
    savant_use = pd.read_csv('savant\\savant_tmp2.csv')
    savant_use['counter'] = 1
    bfpa_file = 'stats\\bf_pa_frame_' + str(yr) + '.csv'
    bfpa_frame = pd.read_csv(bfpa_file)
    savant_use = savant_use.merge(bfpa_frame, left_on='batter', right_on='player', how='inner')
    
    savant_group_outcomes = savant_use[['stand', 'p_throws', 'player_type', 'actual_type', 'counter']].groupby(['stand', 'p_throws', 'player_type', 'actual_type'], as_index=False)['counter'].agg('count')
    savant_group_sums = savant_use[['stand', 'p_throws', 'player_type', 'counter']].groupby(['stand', 'p_throws', 'player_type'], as_index=False)['counter'].agg('count')
    
    oavant_group_outcomes = savant_use[['stand', 'p_throws', 'actual_type', 'counter']].groupby(['stand', 'p_throws', 'actual_type'], as_index=False)['counter'].agg('count')
    oavant_group_sums = savant_use[['stand', 'p_throws', 'counter']].groupby(['stand', 'p_throws'], as_index=False)['counter'].agg('count')
    
    aavant_group_outcomes = savant_use[['actual_type', 'counter']].groupby(['actual_type'], as_index=False)['counter'].agg('count')
    aavant_group_sums = len(savant_use)
    
    lvl_frame_b = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "L") & (savant_group_outcomes['player_type'] == "B")]
    lvl_tot_b = int(savant_group_sums[(savant_group_sums["stand"] == "L") & (savant_group_sums["p_throws"] == "L") & (savant_group_sums['player_type'] == "B")]['counter'])
    lvl_frame_b['pcts'] = lvl_frame_b['counter'] / lvl_tot_b
    
    lvr_frame_b = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "R") & (savant_group_outcomes['player_type'] == "B")]
    lvr_tot_b = int(savant_group_sums[(savant_group_sums["stand"] == "L") & (savant_group_sums["p_throws"] == "R") & (savant_group_sums['player_type'] == "B")]['counter'])
    lvr_frame_b['pcts'] = lvr_frame_b['counter'] / lvr_tot_b
    
    rvl_frame_b = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "L") & (savant_group_outcomes['player_type'] == "B")]
    rvl_tot_b = int(savant_group_sums[(savant_group_sums["stand"] == "R") & (savant_group_sums["p_throws"] == "L") & (savant_group_sums['player_type'] == "B")]['counter'])
    rvl_frame_b['pcts'] = rvl_frame_b['counter'] / rvl_tot_b
    
    rvr_frame_b = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "R") & (savant_group_outcomes['player_type'] == "B")]
    rvr_tot_b = int(savant_group_sums[(savant_group_sums["stand"] == "R") & (savant_group_sums["p_throws"] == "R") & (savant_group_sums['player_type'] == "B")]['counter'])
    rvr_frame_b['pcts'] = rvr_frame_b['counter'] / rvr_tot_b
    
    lvl_frame_p = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "L") & (savant_group_outcomes['player_type'] == "P")]
    if len(lvl_frame_p) > 0:
        lvl_tot_p = int(savant_group_sums[(savant_group_sums["stand"] == "L") & (savant_group_sums["p_throws"] == "L") & (savant_group_sums['player_type'] == "P")]['counter'])
        lvl_frame_p['pcts'] = lvl_frame_p['counter'] / lvl_tot_p
    
    lvr_frame_p = savant_group_outcomes[(savant_group_outcomes["stand"] == "L") & (savant_group_outcomes["p_throws"] == "R") & (savant_group_outcomes['player_type'] == "P")]
    if len(lvr_frame_p) > 0:
        lvr_tot_p = int(savant_group_sums[(savant_group_sums["stand"] == "L") & (savant_group_sums["p_throws"] == "R") & (savant_group_sums['player_type'] == "P")]['counter'])
        lvr_frame_p['pcts'] = lvr_frame_p['counter'] / lvr_tot_p
    
    rvl_frame_p = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "L") & (savant_group_outcomes['player_type'] == "P")]
    if len(rvl_frame_p) > 0:
        rvl_tot_p = int(savant_group_sums[(savant_group_sums["stand"] == "R") & (savant_group_sums["p_throws"] == "L") & (savant_group_sums['player_type'] == "P")]['counter'])
        rvl_frame_p['pcts'] = rvl_frame_p['counter'] / rvl_tot_p
    
    rvr_frame_p = savant_group_outcomes[(savant_group_outcomes["stand"] == "R") & (savant_group_outcomes["p_throws"] == "R") & (savant_group_outcomes['player_type'] == "P")]
    if len(rvr_frame_p) > 0:
        rvr_tot_p = int(savant_group_sums[(savant_group_sums["stand"] == "R") & (savant_group_sums["p_throws"] == "R") & (savant_group_sums['player_type'] == "P")]['counter'])
        rvr_frame_p['pcts'] = rvr_frame_p['counter'] / rvr_tot_p
    
    lvl_frame_o = oavant_group_outcomes[(oavant_group_outcomes["stand"] == "L") & (oavant_group_outcomes["p_throws"] == "L")]
    lvl_tot_o = int(oavant_group_sums[(oavant_group_sums["stand"] == "L") & (oavant_group_sums["p_throws"] == "L")]['counter'])
    lvl_frame_o['pcts'] = lvl_frame_o['counter'] / lvl_tot_o
    
    lvr_frame_o = oavant_group_outcomes[(oavant_group_outcomes["stand"] == "L") & (oavant_group_outcomes["p_throws"] == "R")]
    lvr_tot_o = int(oavant_group_sums[(oavant_group_sums["stand"] == "L") & (oavant_group_sums["p_throws"] == "R")]['counter'])
    lvr_frame_o['pcts'] = lvr_frame_o['counter'] / lvr_tot_o
    
    rvl_frame_o = oavant_group_outcomes[(oavant_group_outcomes["stand"] == "R") & (oavant_group_outcomes["p_throws"] == "L")]
    rvl_tot_o = int(oavant_group_sums[(oavant_group_sums["stand"] == "R") & (oavant_group_sums["p_throws"] == "L")]['counter'])
    rvl_frame_o['pcts'] = rvl_frame_o['counter'] / rvl_tot_o
    
    rvr_frame_o = oavant_group_outcomes[(oavant_group_outcomes["stand"] == "R") & (oavant_group_outcomes["p_throws"] == "R")]
    rvr_tot_o = int(oavant_group_sums[(oavant_group_sums["stand"] == "R") & (oavant_group_sums["p_throws"] == "R")]['counter'])
    rvr_frame_o['pcts'] = rvr_frame_o['counter'] / rvr_tot_o
    
    all_frame_o = aavant_group_outcomes
    all_frame_o['pcts'] = aavant_group_outcomes['counter'] / aavant_group_sums
    
    lvl_frame_b = lvl_frame_b.sort_values('actual_type')
    lvl_frame_abb_b = lvl_frame_b[['actual_type', 'pcts']]
    lvr_frame_b = lvr_frame_b.sort_values('actual_type')
    lvr_frame_abb_b = lvr_frame_b[['actual_type', 'pcts']]
    rvl_frame_b = rvl_frame_b.sort_values('actual_type')
    rvl_frame_abb_b = rvl_frame_b[['actual_type', 'pcts']]
    rvr_frame_b = rvr_frame_b.sort_values('actual_type')
    rvr_frame_abb_b = rvr_frame_b[['actual_type', 'pcts']]
    
    if len(lvl_frame_p) > 0:
        lvl_frame_p = lvl_frame_p.sort_values('actual_type')
        lvl_frame_abb_p = lvl_frame_p[['actual_type', 'pcts']]
    if len(lvr_frame_p) > 0:
        lvr_frame_p = lvr_frame_p.sort_values('actual_type')
        lvr_frame_abb_p = lvr_frame_p[['actual_type', 'pcts']]
    if len(rvl_frame_p) > 0:
        rvl_frame_p = rvl_frame_p.sort_values('actual_type')
        rvl_frame_abb_p = rvl_frame_p[['actual_type', 'pcts']]
    if len(rvr_frame_p) > 0:
        rvr_frame_p = rvr_frame_p.sort_values('actual_type')
        rvr_frame_abb_p = rvr_frame_p[['actual_type', 'pcts']]
    
    lvl_frame_o = lvl_frame_o.sort_values('actual_type')
    lvl_frame_abb_o = lvl_frame_o[['actual_type', 'pcts']]
    lvr_frame_o = lvr_frame_o.sort_values('actual_type')
    lvr_frame_abb_o = lvr_frame_o[['actual_type', 'pcts']]
    rvl_frame_o = rvl_frame_o.sort_values('actual_type')
    rvl_frame_abb_o = rvl_frame_o[['actual_type', 'pcts']]
    rvr_frame_o = rvr_frame_o.sort_values('actual_type')
    rvr_frame_abb_o = rvr_frame_o[['actual_type', 'pcts']]
    
    all_frame_o = all_frame_o.sort_values('actual_type')
    all_frame_abb_o = all_frame_o[['actual_type', 'pcts']]
    
    true_outcomes = pd.read_csv('stats\\true_outcomes.csv')
    
    lvl_frame_b_merge = true_outcomes.merge(lvl_frame_abb_b, how='left', left_on='actual_type', right_on='actual_type')
    lvl_frame_b_merge.drop('zero', inplace=True, axis='columns')
    lvl_frame_b_merge.rename(columns={'pcts':'LVLB'}, inplace=True)
    lvr_frame_b_merge = true_outcomes.merge(lvr_frame_abb_b, how='left', left_on='actual_type', right_on='actual_type')
    lvr_frame_b_merge.drop('zero', inplace=True, axis='columns')
    lvr_frame_b_merge.rename(columns={'pcts':'LVRB'}, inplace=True)
    rvl_frame_b_merge = true_outcomes.merge(rvl_frame_abb_b, how='left', left_on='actual_type', right_on='actual_type')
    rvl_frame_b_merge.drop('zero', inplace=True, axis='columns')
    rvl_frame_b_merge.rename(columns={'pcts':'RVLB'}, inplace=True)
    rvr_frame_b_merge = true_outcomes.merge(rvr_frame_abb_b, how='left', left_on='actual_type', right_on='actual_type')
    rvr_frame_b_merge.drop('zero', inplace=True, axis='columns')
    rvr_frame_b_merge.rename(columns={'pcts':'RVRB'}, inplace=True)
    
    if len(lvl_frame_p) > 0:
        lvl_frame_p_merge = true_outcomes.merge(lvl_frame_abb_p, how='left', left_on='actual_type', right_on='actual_type')
        lvl_frame_p_merge.drop('zero', inplace=True, axis='columns')
        lvl_frame_p_merge.rename(columns={'pcts':'LVLP'}, inplace=True)
    if len(lvr_frame_p) > 0:
        lvr_frame_p_merge = true_outcomes.merge(lvr_frame_abb_p, how='left', left_on='actual_type', right_on='actual_type')
        lvr_frame_p_merge.drop('zero', inplace=True, axis='columns')
        lvr_frame_p_merge.rename(columns={'pcts':'LVRP'}, inplace=True)
    if len(rvl_frame_p) > 0:
        rvl_frame_p_merge = true_outcomes.merge(rvl_frame_abb_p, how='left', left_on='actual_type', right_on='actual_type')
        rvl_frame_p_merge.drop('zero', inplace=True, axis='columns')
        rvl_frame_p_merge.rename(columns={'pcts':'RVLP'}, inplace=True)
    if len(rvr_frame_p) > 0:
        rvr_frame_p_merge = true_outcomes.merge(rvr_frame_abb_p, how='left', left_on='actual_type', right_on='actual_type')
        rvr_frame_p_merge.drop('zero', inplace=True, axis='columns')
        rvr_frame_p_merge.rename(columns={'pcts':'RVRP'}, inplace=True)
    
    lvl_frame_o_merge = true_outcomes.merge(lvl_frame_abb_o, how='left', left_on='actual_type', right_on='actual_type')
    lvl_frame_o_merge.drop('zero', inplace=True, axis='columns')
    lvl_frame_o_merge.rename(columns={'pcts':'LVLO'}, inplace=True)
    lvr_frame_o_merge = true_outcomes.merge(lvr_frame_abb_o, how='left', left_on='actual_type', right_on='actual_type')
    lvr_frame_o_merge.drop('zero', inplace=True, axis='columns')
    lvr_frame_o_merge.rename(columns={'pcts':'LVRO'}, inplace=True)
    rvl_frame_o_merge = true_outcomes.merge(rvl_frame_abb_o, how='left', left_on='actual_type', right_on='actual_type')
    rvl_frame_o_merge.drop('zero', inplace=True, axis='columns')
    rvl_frame_o_merge.rename(columns={'pcts':'RVLO'}, inplace=True)
    rvr_frame_o_merge = true_outcomes.merge(rvr_frame_abb_o, how='left', left_on='actual_type', right_on='actual_type')
    rvr_frame_o_merge.drop('zero', inplace=True, axis='columns')
    rvr_frame_o_merge.rename(columns={'pcts':'RVRO'}, inplace=True)
    
    all_frame_o_merge = true_outcomes.merge(all_frame_abb_o, how='left', left_on='actual_type', right_on='actual_type')
    all_frame_o_merge.drop('zero', inplace=True, axis='columns')
    all_frame_o_merge.rename(columns={'pcts':'ALL'}, inplace=True)
    
    if yr == 2020:
        p_file = pd.read_csv('stats\\league_splits_2019.csv')
        master_league_avgs = pd.concat([lvl_frame_b_merge, lvr_frame_b_merge['LVRB'], rvl_frame_b_merge['RVLB'], rvr_frame_b_merge['RVRB'],
                                    p_file['LVLP'], p_file['LVRP'], p_file['RVLP'], p_file['RVRP'], 
                                    lvl_frame_o_merge['LVLO'], lvr_frame_o_merge['LVRO'], rvl_frame_o_merge['RVLO'], rvr_frame_o_merge['RVRO'], 
                                    all_frame_o_merge['ALL']], axis = 'columns')

    else:
        master_league_avgs = pd.concat([lvl_frame_b_merge, lvr_frame_b_merge['LVRB'], rvl_frame_b_merge['RVLB'], rvr_frame_b_merge['RVRB'],
                                    lvl_frame_p_merge['LVLP'], lvr_frame_p_merge['LVRP'], rvl_frame_p_merge['RVLP'], rvr_frame_p_merge['RVRP'], 
                                    lvl_frame_o_merge['LVLO'], lvr_frame_o_merge['LVRO'], rvl_frame_o_merge['RVLO'], rvr_frame_o_merge['RVRO'], 
                                    all_frame_o_merge['ALL']], axis = 'columns')
    

    master_league_avgs.fillna({'LVLB': 0}, inplace=True)
    master_league_avgs.fillna({'LVRB': 0}, inplace=True)
    master_league_avgs.fillna({'RVLB': 0}, inplace=True)
    master_league_avgs.fillna({'RVRB': 0}, inplace=True)
    master_league_avgs.fillna({'LVLP': 0}, inplace=True)
    master_league_avgs.fillna({'LVRP': 0}, inplace=True)
    master_league_avgs.fillna({'RVLP': 0}, inplace=True)
    master_league_avgs.fillna({'RVRP': 0}, inplace=True)
    master_league_avgs.fillna({'LVLO': 0}, inplace=True)
    master_league_avgs.fillna({'LVRO': 0}, inplace=True)
    master_league_avgs.fillna({'RVLO': 0}, inplace=True)
    master_league_avgs.fillna({'RVRO': 0}, inplace=True)
    master_league_avgs.fillna({'ALL': 0}, inplace=True)

    mla_file = 'stats\\league_splits_' + str(yr) + '.csv'
    master_league_avgs.to_csv(mla_file, index = False, header = True, encoding='utf-8-sig')

#Impute Past Season Stats < 100
#Batters
def savant_impute_bat(yr):
    thresh = 100
    league_avg_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    player_type_frame = pd.read_csv('stats\\bf_pa_frame_' + str(yr) + '.csv')
    
    lvl_b = pd.read_csv('stats\\lvl_bat_avg_' + str(yr) + '.csv')
    lvl_b = lvl_b.merge(player_type_frame[['player', 'player_type']], left_on = 'batter', right_on = 'player', how = 'inner')
    lvl_b.drop('player', inplace=True, axis='columns')
    lvl_b_rep = lvl_b
    
    lvr_b = pd.read_csv('stats\\lvr_bat_avg_' + str(yr) + '.csv')
    lvr_b = lvr_b.merge(player_type_frame[['player', 'player_type']], left_on = 'batter', right_on = 'player', how = 'inner')
    lvr_b.drop('player', inplace=True, axis='columns')
    lvr_b_rep = lvr_b
    
    rvl_b = pd.read_csv('stats\\rvl_bat_avg_' + str(yr) + '.csv')
    rvl_b = rvl_b.merge(player_type_frame[['player', 'player_type']], left_on = 'batter', right_on = 'player', how = 'inner')
    rvl_b.drop('player', inplace=True, axis='columns')
    rvl_b_rep = rvl_b
    
    rvr_b = pd.read_csv('stats\\rvr_bat_avg_' + str(yr) + '.csv')
    rvr_b = rvr_b.merge(player_type_frame[['player', 'player_type']], left_on = 'batter', right_on = 'player', how = 'inner')
    rvr_b.drop('player', inplace=True, axis='columns')
    rvr_b_rep = rvr_b
    
    
    act_frame = lvl_b
    imp_col_b = 1
    imp_col_p = 5
    a = 0
    g = len(lvl_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                lvl_b_rep.loc[a, stat_type] = combo_portion
                
    
    lvl_b_rep.to_csv('stats\\lvl_imp_bat_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = lvr_b
    imp_col_b = 2
    imp_col_p = 6
    a = 0
    g = len(lvr_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                lvr_b_rep.loc[a, stat_type] = combo_portion
                
    
    lvr_b_rep.to_csv('stats\\lvr_imp_bat_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = rvl_b
    imp_col_b = 3
    imp_col_p = 7
    a = 0
    g = len(rvl_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                rvl_b_rep.loc[a, stat_type] = combo_portion
                
    
    rvl_b_rep.to_csv('stats\\rvl_imp_bat_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = rvr_b
    imp_col_b = 4
    imp_col_p = 8
    a = 0
    g = len(rvr_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                rvr_b_rep.loc[a, stat_type] = combo_portion
                
    
    rvr_b_rep.to_csv('stats\\rvr_imp_bat_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
            

#Impute Past Season Stats < 100
#Pitchers
def savant_impute_pit(yr):
    thresh = 100
    league_avg_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    player_type_frame = pd.read_csv('stats\\bf_pa_frame_' + str(yr) + '.csv')
    
    lvl_b = pd.read_csv('stats\\lvl_pit_avg_' + str(yr) + '.csv')
    lvl_b = lvl_b.merge(player_type_frame[['player', 'player_type']], left_on = 'pitcher', right_on = 'player', how = 'inner')
    lvl_b.drop('player', inplace=True, axis='columns')
    lvl_b_rep = lvl_b
    
    lvr_b = pd.read_csv('stats\\lvr_pit_avg_' + str(yr) + '.csv')
    lvr_b = lvr_b.merge(player_type_frame[['player', 'player_type']], left_on = 'pitcher', right_on = 'player', how = 'inner')
    lvr_b.drop('player', inplace=True, axis='columns')
    lvr_b_rep = lvr_b
    
    rvl_b = pd.read_csv('stats\\rvl_pit_avg_' + str(yr) + '.csv')
    rvl_b = rvl_b.merge(player_type_frame[['player', 'player_type']], left_on = 'pitcher', right_on = 'player', how = 'inner')
    rvl_b.drop('player', inplace=True, axis='columns')
    rvl_b_rep = rvl_b
    
    rvr_b = pd.read_csv('stats\\rvr_pit_avg_' + str(yr) + '.csv')
    rvr_b = rvr_b.merge(player_type_frame[['player', 'player_type']], left_on = 'pitcher', right_on = 'player', how = 'inner')
    rvr_b.drop('player', inplace=True, axis='columns')
    rvr_b_rep = rvr_b
    
    
    act_frame = lvl_b
    imp_col_b = 9
    imp_col_p = 9
    a = 0
    g = len(lvl_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                lvl_b_rep.loc[a, stat_type] = combo_portion
                
    
    lvl_b_rep.to_csv('stats\\lvl_imp_pit_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = lvr_b
    imp_col_b = 10
    imp_col_p = 10
    a = 0
    g = len(lvr_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                lvr_b_rep.loc[a, stat_type] = combo_portion
                
    
    lvr_b_rep.to_csv('stats\\lvr_imp_pit_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = rvl_b
    imp_col_b = 11
    imp_col_p = 11
    a = 0
    g = len(rvl_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                rvl_b_rep.loc[a, stat_type] = combo_portion
                
    
    rvl_b_rep.to_csv('stats\\rvl_imp_pit_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    act_frame = rvr_b
    imp_col_b = 12
    imp_col_p = 12
    a = 0
    g = len(rvr_b)
    for a in range(g):
        act_pa = act_frame.loc[a, 'pa']
        player_type = act_frame.loc[a, 'player_type']
        if (act_pa < thresh):
            pct_played = act_pa / thresh
            pct_impute = 1 - pct_played
            b = 0
            h = len(league_avg_frame)
            for b in range(h):
                stat_type = league_avg_frame.loc[b, 'actual_type']
                if player_type == "B":
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_b]
                else:
                    lgavg_stat_use = league_avg_frame.iloc[b, imp_col_p]
                    
                player_stat_use = act_frame.loc[a, stat_type]
                played_portion = pct_played * player_stat_use
                impute_portion = pct_impute * lgavg_stat_use
                combo_portion = played_portion + impute_portion
                rvr_b_rep.loc[a, stat_type] = combo_portion
                
    
    rvr_b_rep.to_csv('stats\\rvr_imp_pit_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
#Create Player Stat Frames vs. League Average
#Uses Imputed Frames
def savant_vla(yr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    avg_list = ['LVLO', 'LVRO', 'RVLO', 'RVRO']
    avg_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    
    a = 0
    g = len(split_list) - 1
    while a <= g:
        
        split_use = split_list[a]
        avg_use = avg_list[a]
        
        for b in type_list:
            
            file_name = 'stats\\' + str(split_use) + '_imp_' + str(b) + '_' + str(yr) + '.csv'
            file_frame = pd.read_csv(file_name)

            for c in at_list:
                
                la_use = float(avg_frame[avg_frame['actual_type'] == c][avg_use])
                file_frame[c] = file_frame[c] - la_use

                
            file_frame.to_csv('stats\\' + str(split_use) + '_vla_' + str(b) + '_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')

        a += 1
        
#Creates Average Number of Pitches Seen for Batters and Pitchers
#Impute League Average if Less than 25 PA
def savant_pitches(yr):
    
    type_list = ['bat', 'pit']
    
    savant_file = pd.read_csv('savant\\savant_wx_' + str(yr) + '.csv')
    
    league_avg = savant_file['pitch_number'].mean()

    batter_group = savant_file.groupby('batter')['pitch_number'].agg('mean')
    batter_pa = savant_file.groupby('batter')['counter'].agg('sum')
    batter_group = batter_group.reset_index()
    batter_pa = batter_pa.reset_index()
    batter_merge = batter_group.merge(batter_pa, on = 'batter')
    
    pitcher_group = savant_file.groupby('pitcher')['pitch_number'].agg('mean')
    pitcher_pa = savant_file.groupby('pitcher')['counter'].agg('sum')
    pitcher_group = pitcher_group.reset_index()
    pitcher_pa = pitcher_pa.reset_index()
    pitcher_merge = pitcher_group.merge(pitcher_pa, on = 'pitcher')
    
    #Impute
    
    a = 0
    g = len(batter_merge)
    
    for a in range(g):
        
        pa = batter_merge.loc[a, 'counter']
        if pa < 25:
            
            np = batter_merge.loc[a, 'pitch_number']
            
            actual_pct = pa / 25
            impute_pct = 1 - actual_pct
            
            actual_pn = np * actual_pct
            impute_pn = league_avg * impute_pct
            combo_pn = actual_pn + impute_pn
        
            batter_merge.loc[a, 'pitch_number'] = combo_pn
            
    a = 0
    g = len(pitcher_merge)
    
    for a in range(g):
        
        pa = pitcher_merge.loc[a, 'counter']
        if pa < 25:
            
            np = pitcher_merge.loc[a, 'pitch_number']
            
            actual_pct = pa / 25
            impute_pct = 1 - actual_pct
            
            actual_pn = np * actual_pct
            impute_pn = league_avg * impute_pct
            combo_pn = actual_pn + impute_pn
        
            pitcher_merge.loc[a, 'pitch_number'] = combo_pn
            
    raw_batter = batter_merge
    raw_pitcher = pitcher_merge
    raw_batter_2 = batter_merge
    raw_pitcher_2 = pitcher_merge
        
    
    #Adjust
    
    batter_join = savant_file.merge(raw_pitcher_2, on = 'pitcher')
    batter_group = batter_join.groupby('batter')['pitch_number_y'].agg('mean')
    batter_pa = batter_join.groupby('batter')['counter_x'].agg('sum')
    batter_group = batter_group.reset_index()
    batter_pa = batter_pa.reset_index()
    batter_merge = batter_group.merge(batter_pa, on = 'batter')
    
    a = 0
    g = len(batter_merge)
    
    for a in range(g):
        
        pa = batter_merge.loc[a, 'counter_x']
        pid = batter_merge.loc[a, 'batter']
        if pa < 25:
            
            np = batter_merge.loc[a, 'pitch_number_y']
            
            actual_pct = pa / 25
            impute_pct = 1 - actual_pct
            
            actual_pn = np * actual_pct
            impute_pn = league_avg * impute_pct
            combo_pn = actual_pn + impute_pn
        
            batter_merge.loc[a, 'pitch_number_y'] = combo_pn
        
        y = batter_merge.loc[a, 'pitch_number_y']
        yy = y - league_avg
        batter_merge.loc[a, 'vla'] = yy
        i = raw_batter[raw_batter['batter'] == pid].index
        raw_batter.loc[i, 'vla'] = yy      
                    
    raw_batter['adj'] = raw_batter['pitch_number'] - raw_batter['vla']

    pitcher_join = savant_file.merge(raw_batter_2, on = 'batter')
    pitcher_group = pitcher_join.groupby('pitcher')['pitch_number_y'].agg('mean')
    pitcher_pa = pitcher_join.groupby('pitcher')['counter_x'].agg('sum')
    pitcher_group = pitcher_group.reset_index()
    pitcher_pa = pitcher_pa.reset_index()
    pitcher_merge = pitcher_group.merge(pitcher_pa, on = 'pitcher')
    
    a = 0
    g = len(pitcher_merge)
    
    for a in range(g):
        
        pa = pitcher_merge.loc[a, 'counter_x']
        pid = pitcher_merge.loc[a, 'pitcher']
        if pa < 25:
            
            np = pitcher_merge.loc[a, 'pitch_number_y']
            
            actual_pct = pa / 25
            impute_pct = 1 - actual_pct
            
            actual_pn = np * actual_pct
            impute_pn = league_avg * impute_pct
            combo_pn = actual_pn + impute_pn
        
            pitcher_merge.loc[a, 'pitch_number_y'] = combo_pn
        
        y = pitcher_merge.loc[a, 'pitch_number_y']
        yy = y - league_avg
        pitcher_merge.loc[a, 'vla'] = yy
        i = raw_pitcher[raw_pitcher['pitcher'] == pid].index
        raw_pitcher.loc[i, 'vla'] = yy      
                    
    raw_pitcher['adj'] = raw_pitcher['pitch_number'] - raw_pitcher['vla']
    
    raw_batter.to_csv('stats\\numpitch_bat_' + str(yr) + '.csv', index=False,  header=True, encoding='utf-8-sig')
    raw_pitcher.to_csv('stats\\numpitch_pit_' + str(yr) + '.csv', index=False,  header=True, encoding='utf-8-sig')
    
#Calcuate Max Number of Pitches in Starts and in Relief, and Average Number of Pitches in Starts/Relief

def savant_maxpitch(yr):
    
    savant_file = pd.read_csv('savant\\savant_wx_' + str(yr) + '.csv')
    start_group = savant_file[savant_file['inning'] == 1]
    start_group = start_group.groupby(['pitcher', 'game_pk'])['counter'].agg('count')
    start_group = start_group.reset_index()
    start_group.rename(columns={'counter':'start_flag'}, inplace=True)
    start_group['start_flag'] = 1
    
    savant_start = savant_file.merge(start_group, on = ['pitcher', 'game_pk'], how = 'left')
    
    savant_start.replace(np.nan, 0, inplace=True)
    
    #Starting Pitchers Only
    
    savant_sp = savant_start[savant_start['start_flag'] == 1]
    savant_pitches = savant_sp.groupby(['pitcher', 'game_pk'])['pitch_number'].agg('sum')
    savant_pitches = savant_pitches.reset_index()
    league_sp = savant_pitches['pitch_number'].mean()
    savant_max_sp = savant_pitches.groupby('pitcher')['pitch_number'].agg('max')
    savant_mean_sp = savant_pitches.groupby('pitcher')['pitch_number'].agg('mean')
    savant_count_sp = savant_pitches.groupby('pitcher')['pitch_number'].agg('count')
    savant_max_sp = savant_max_sp.reset_index()
    savant_mean_sp = savant_mean_sp.reset_index()
    savant_count_sp = savant_count_sp.reset_index()
    
    #Relief Pitchers Only

    savant_rp = savant_start[savant_start['start_flag'] == 0]
    savant_pitches = savant_rp.groupby(['pitcher', 'game_pk'])['pitch_number'].agg('sum')
    savant_pitches = savant_pitches.reset_index()
    league_rp = savant_pitches['pitch_number'].mean()
    savant_max_rp = savant_pitches.groupby('pitcher')['pitch_number'].agg('max')
    savant_mean_rp = savant_pitches.groupby('pitcher')['pitch_number'].agg('mean')
    savant_max_rp = savant_max_rp.reset_index()
    savant_mean_rp = savant_mean_rp.reset_index()
    
    #Msater Frame
    
    maxpitch_master = savant_max_sp.merge(savant_mean_sp, on = 'pitcher', how = 'left', suffixes=('_1', '_2'))
    
    maxpitch_master = maxpitch_master.merge(savant_max_rp, on = 'pitcher', how = 'outer', suffixes=('_3', '_4'))
    
    maxpitch_master = maxpitch_master.merge(savant_mean_rp, on = 'pitcher', how = 'left', suffixes=('_5', '_6'))
    
    maxpitch_master.rename(columns={maxpitch_master.columns[1]:'max_start'}, inplace=True)
    maxpitch_master.rename(columns={maxpitch_master.columns[2]:'mean_start'}, inplace=True)
    maxpitch_master.rename(columns={maxpitch_master.columns[3]:'max_relief'}, inplace=True)
    maxpitch_master.rename(columns={maxpitch_master.columns[4]:'mean_relief'}, inplace=True)
    
    maxpitch_master = maxpitch_master.merge(savant_count_sp, on = 'pitcher', how = 'left')
    
    maxpitch_master.rename(columns={maxpitch_master.columns[5]:'starts'}, inplace=True)
    
    maxpitch_master['max_relief'].replace(np.nan, league_rp, inplace=True)
    maxpitch_master['mean_relief'].replace(np.nan, league_rp, inplace=True)
    maxpitch_master['max_start'].replace(np.nan, league_sp, inplace=True)
    maxpitch_master['mean_start'].replace(np.nan, league_sp, inplace=True)
    maxpitch_master['starts'].replace(np.nan, 0, inplace=True)
    
    a = 0
    g = len(maxpitch_master)
    
    for a in range(g):
        
        starts = maxpitch_master.loc[a, 'starts']
        max_start = maxpitch_master.loc[a, 'max_start']
        mean_start = maxpitch_master.loc[a, 'mean_start']
        if (starts > 0) & (starts < 4) & (max_start < league_sp) & (mean_start < league_sp):
            
            maxpitch_master.loc[a, 'max_start'] = league_sp
            maxpitch_master.loc[a, 'mean_start'] = league_sp
        
        if (starts >= 4) & (starts < 7) & (max_start < league_sp) & (mean_start < league_sp):
            
            yes_mult = .5 + (.167 * (starts - 4))
            imp_mult = 1 - yes_mult
            
            maxpitch_master.loc[a, 'max_start'] = (league_sp * imp_mult) + (yes_mult * max_start)
            maxpitch_master.loc[a, 'mean_start'] = (league_sp * imp_mult) + (yes_mult * mean_start)
        
    
    maxpitch_master.to_csv('stats\\maxpitch_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
    maxpitch_avg_list = [league_sp, league_rp]
    mp_avg_df = pd.DataFrame(data={'max':maxpitch_avg_list})
    mp_avg_df.to_csv('mc\\maxpitchavg.csv', index=False, header=True, encoding='utf-8-sig')
    
def savant_errors(yr):
    
    savant_file = pd.read_csv('savant\\savant_wx_' + str(yr) + '.csv')
    
    savant_error = savant_file[savant_file['events'] == 'field_error']
    savant_noerror = savant_file[savant_file['actual_type'].isin(["fly_ball", "ground_ball", "line_drive", "popup", "sacb"])]
    
    #Home
    
    savant_home_error = savant_error[savant_error['inning_topbot'] == 'Top']
    savant_hg_error = savant_home_error.groupby('home_team')['counter'].agg('count')
    savant_hg_error = savant_hg_error.reset_index()
    
    savant_home_noerror = savant_noerror[savant_noerror['inning_topbot'] == 'Top']
    savant_hg_noerror = savant_home_noerror.groupby('home_team')['counter'].agg('count')
    savant_hg_noerror = savant_hg_noerror.reset_index()
    
    savant_hg_merge = savant_hg_noerror.merge(savant_hg_error, on = 'home_team', how = 'left')
    savant_hg_merge.fillna(0, inplace=True)
    
    #Away
    
    savant_home_error = savant_error[savant_error['inning_topbot'] == 'Bot']
    savant_hg_error = savant_home_error.groupby('away_team')['counter'].agg('count')
    savant_hg_error = savant_hg_error.reset_index()
    
    savant_home_noerror = savant_noerror[savant_noerror['inning_topbot'] == 'Bot']
    savant_hg_noerror = savant_home_noerror.groupby('away_team')['counter'].agg('count')
    savant_hg_noerror = savant_hg_noerror.reset_index()
    
    savant_ag_merge = savant_hg_noerror.merge(savant_hg_error, on = 'away_team', how = 'left')
    savant_ag_merge.fillna(0, inplace=True)
    
    #Change Column Names
    
    savant_hg_merge.rename(columns={'home_team':'team', 'counter_x':'chances', 'counter_y':'errors'}, inplace=True)
    savant_ag_merge.rename(columns={'away_team':'team', 'counter_x':'chances', 'counter_y':'errors'}, inplace=True)
    
    savant_whole = savant_hg_merge.merge(savant_ag_merge, on = 'team', how = 'outer')
    savant_whole.fillna(0, inplace=True)
    
    savant_whole['tot_errors'] = savant_whole['errors_x'] + savant_whole['errors_y']
    savant_whole['tot_chances'] = savant_whole['chances_x'] + savant_whole['chances_y']
    savant_whole['field_pct'] = savant_whole['tot_errors'] / savant_whole['tot_chances']
    
    savant_final = savant_whole[['team', 'tot_errors', 'tot_chances', 'field_pct']]
    
    savant_final.to_csv('stats\\errors_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    

def savant_do_all(yr):
    savant_wrangle_init(yr)
    savant_wrangle_parse(yr)
    savant_bf_pa(yr)
    savant_batter_avgs(yr)
    savant_pitcher_avgs(yr)
    savant_avgs(yr)
    savant_impute_bat(yr)
    savant_impute_pit(yr)
    savant_vla(yr)
    savant_pitches(yr)
    savant_maxpitch(yr)
    savant_errors(yr)
