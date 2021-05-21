#The PPS is the Player Projection System which uses a hybrid of a weighted baseline + nearest neighbor analysis to project each player's statistics for the upcoming season
#This is helpful to fill in gaps until the player has had enough at bats/batters faced to constitute a large enough sample size from the current season's statistics.
#The projection system projects platoon split statistics for batters and pitchers.
#It calculates statistics for new players/rookies based on performace of rookies in the past. 
#It also projects the overall league average by split and park factors for the upcoming season as well.

import pandas as pd
import numpy as np
from sklearn.preprocessing import Normalizer
from sklearn import preprocessing

#Get a list of every single batter/pitcher ID
def master_id_create(syr, eyr):
    
    a = syr
    ct = 0
    while (a <= eyr):
        
        rvr_bf = pd.read_csv('stats\\rvr_adj2_bat_' + str(a) + '.csv')
        rvl_bf = pd.read_csv('stats\\rvl_adj2_bat_' + str(a) + '.csv')
        lvr_bf = pd.read_csv('stats\\lvr_adj2_bat_' + str(a) + '.csv')
        lvl_bf = pd.read_csv('stats\\lvl_adj2_bat_' + str(a) + '.csv')
        
        rvr_pf = pd.read_csv('stats\\rvr_adj2_pit_' + str(a) + '.csv')
        rvl_pf = pd.read_csv('stats\\rvl_adj2_pit_' + str(a) + '.csv')
        lvr_pf = pd.read_csv('stats\\lvr_adj2_pit_' + str(a) + '.csv')
        lvl_pf = pd.read_csv('stats\\lvl_adj2_pit_' + str(a) + '.csv')
        
        if ct == 0:
            
            mst_rvr_bat = rvr_bf
            mst_rvl_bat = rvl_bf
            mst_lvr_bat = lvr_bf
            mst_lvl_bat = lvl_bf
            
            mst_rvr_pit = rvr_pf
            mst_rvl_pit = rvl_pf
            mst_lvr_pit = lvr_pf
            mst_lvl_pit = lvl_pf
        
        else:
            
            mst_rvr_bat = pd.concat([mst_rvr_bat, rvr_bf], axis='rows')
            mst_rvl_bat = pd.concat([mst_rvl_bat, rvl_bf], axis='rows')
            mst_lvr_bat = pd.concat([mst_lvr_bat, lvr_bf], axis='rows')
            mst_lvl_bat = pd.concat([mst_lvl_bat, lvl_bf], axis='rows')
            
            mst_rvr_pit = pd.concat([mst_rvr_pit, rvr_pf], axis='rows')
            mst_rvl_pit = pd.concat([mst_rvl_pit, rvl_pf], axis='rows')
            mst_lvr_pit = pd.concat([mst_lvr_pit, lvr_pf], axis='rows')
            mst_lvl_pit = pd.concat([mst_lvl_pit, lvl_pf], axis='rows')
        
        ct += 1
        a += 1
        
    mst_rvr_group = mst_rvr_bat.groupby('batter')['pa'].agg('mean')
    mst_rvr_group = mst_rvr_group.reset_index()
    mst_rvr_group.rename(columns={mst_rvr_group.columns[0]:'RVR'}, inplace=True)
    mst_rvr_ids = mst_rvr_group['RVR']
    
    mst_rvl_group = mst_rvl_bat.groupby('batter')['pa'].agg('mean')
    mst_rvl_group = mst_rvl_group.reset_index()
    mst_rvl_group.rename(columns={mst_rvl_group.columns[0]:'RVL'}, inplace=True)
    mst_rvl_ids = mst_rvl_group['RVL']
    
    mst_lvr_group = mst_lvr_bat.groupby('batter')['pa'].agg('mean')
    mst_lvr_group = mst_lvr_group.reset_index()
    mst_lvr_group.rename(columns={mst_lvr_group.columns[0]:'LVR'}, inplace=True)
    mst_lvr_ids = mst_lvr_group['LVR']
    
    mst_lvl_group = mst_lvl_bat.groupby('batter')['pa'].agg('mean')
    mst_lvl_group = mst_lvl_group.reset_index()
    mst_lvl_group.rename(columns={mst_lvl_group.columns[0]:'LVL'}, inplace=True)
    mst_lvl_ids = mst_lvl_group['LVL']
    
    master_bat_ids = pd.concat([mst_lvl_ids, mst_lvr_ids, mst_rvl_ids, mst_rvr_ids], axis='columns')
    
    mst_rvr_group = mst_rvr_pit.groupby('pitcher')['pa'].agg('mean')
    mst_rvr_group = mst_rvr_group.reset_index()
    mst_rvr_group.rename(columns={mst_rvr_group.columns[0]:'RVR'}, inplace=True)
    mst_rvr_ids = mst_rvr_group['RVR']
    
    mst_rvl_group = mst_rvl_pit.groupby('pitcher')['pa'].agg('mean')
    mst_rvl_group = mst_rvl_group.reset_index()
    mst_rvl_group.rename(columns={mst_rvl_group.columns[0]:'RVL'}, inplace=True)
    mst_rvl_ids = mst_rvl_group['RVL']
    
    mst_lvr_group = mst_lvr_pit.groupby('pitcher')['pa'].agg('mean')
    mst_lvr_group = mst_lvr_group.reset_index()
    mst_lvr_group.rename(columns={mst_lvr_group.columns[0]:'LVR'}, inplace=True)
    mst_lvr_ids = mst_lvr_group['LVR']
    
    mst_lvl_group = mst_lvl_pit.groupby('pitcher')['pa'].agg('mean')
    mst_lvl_group = mst_lvl_group.reset_index()
    mst_lvl_group.rename(columns={mst_lvl_group.columns[0]:'LVL'}, inplace=True)
    mst_lvl_ids = mst_lvl_group['LVL']
    
    
    master_pit_ids = pd.concat([mst_lvl_ids, mst_lvr_ids, mst_rvl_ids, mst_rvr_ids], axis='columns')
    
    master_bat_ids.to_csv('pps\\master_bat_ids.csv', index=False, header=True)
    master_pit_ids.to_csv('pps\\master_pit_ids.csv', index=False, header=True)

#------------BASELINE----------------

#Create Matrices of Players Year by Year Split Stats
def blank_bl_frame(syr, eyr):
    
    split_list = ['LVL', 'LVR', 'RVL', 'RVR']
    split_list2 = ['lvl', 'lvr', 'rvl', 'rvr']
    pt_list = ['bat', 'pit']
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    bat_ids = pd.read_csv('pps\\master_bat_ids.csv')
    pit_ids = pd.read_csv('pps\\master_pit_ids.csv')
    
    bat_ids['DP'] = 0
    pit_ids['DP'] = 0

              
    for p in pt_list:
            
        for s in split_list:
                
            l_s = s.lower()
                
            for t in at_list:
                    
                if (p == 'bat'):
                    use_bt = 'batter'
                    blank_frame = bat_ids[[s, 'DP']]
                else:
                    use_bt = 'pitcher'
                    blank_frame = pit_ids[[s, 'DP']]
                
                
                blank_frame.dropna(inplace=True)
                blank_frame.rename(columns={blank_frame.columns[0]:use_bt}, inplace=True)
                out_file = 'pps\\smx_' + str(p) + '_' + str(l_s) + '_' + str(t) + '.csv'

                a = syr
                while a <= eyr:
                
                    use_file = 'stats\\' + str(l_s) + '_adj2_' + str(p) + '_' + str(a) + '.csv'
                    use_frame = pd.read_csv(use_file)
                    use_slice = use_frame[[use_bt, t]]
                    
                    if p == 'bat':
                        blank_frame = blank_frame.merge(use_slice, on = 'batter', how = 'left')
                    else:
                        blank_frame = blank_frame.merge(use_slice, on = 'pitcher', how = 'left')
                        
                    col_count = blank_frame.shape[1] - 1
                    blank_frame.rename(columns={blank_frame.columns[col_count]:a}, inplace=True)
                    
                    a += 1
                
                blank_frame.to_csv(out_file, index=False, header=True)
 
        a += 1

#Calcuate Weighted 3 Year Average Baseline
def baseline_calcu(syr, eyr):
    
    pt_list = ['bat', 'pit']
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    

    for p in pt_list:
        
        for s in split_list:
            
            ct = 0
            
            for a in at_list:
            
                in_file = 'pps\\smx_' + str(p) + '_' + str(s) + '_' + str(a) + '.csv'
                out_file = 'pps\\baseline_' + str(p) + '_' + str(s) + '_' + str(eyr + 1) + '.csv'
                
                if p == 'bat':
                    pf = 'batter'
                else:
                    pf = 'pitcher'
                
                in_frame = pd.read_csv(in_file)
                col_list = in_frame.columns
                
                b = 2
                h = in_frame.shape[1] - 1
                
                while (b <= h):
                    
                    z = int(col_list[b])
                    zz = col_list[b]
                    if (z < syr) | (z > eyr):
                        in_frame.drop(zz, axis='columns', inplace=True)
                
                    b += 1
                    
                in_frame.fillna(-99, inplace=True)

                
                b = 2
                h = in_frame.shape[1] - 1
                
                while b <= h:
                    
                    z = in_frame.columns[b]
                    z_x = 'Y_' + str(z)
                    in_frame[z_x] = in_frame[z] > -90
                    in_frame[z_x] = in_frame[z_x].astype(int)
                    
                    b += 1
                
                col_start = in_frame.shape[1] - ((eyr - syr) + 1)
                col_end = in_frame.shape[1]
                in_frame['PLAYED'] = in_frame.iloc[:, col_start:col_end].sum(axis='columns')
                
                row_y1 = 'Y_' + str(syr)
                row_y2 = 'Y_' + str(syr + 1)
                row_y3 = 'Y_' + str(eyr)
                
                played_frame = in_frame[in_frame['PLAYED'] > 0]
                played_frame = played_frame.reset_index()
                played_frame[a] = 0
                
                y = 0
                j = len(played_frame)
                
                for y in range(j):
                    
                    ln_use = played_frame.loc[y, :]
                    
                    plyd = ln_use['PLAYED']
                    
                    y1 = ln_use.loc[row_y1]
                    y2 = ln_use.loc[row_y2]
                    y3 = ln_use.loc[row_y3]
                        
                    s1 = ln_use.loc[str(syr)]
                    s2 = ln_use.loc[str(syr + 1)]
                    s3 = ln_use.loc[str(eyr)]
                        
                    if plyd == 1:

                        if (y1 == 1):
                            wt_avg = (.5 * s1) + (.5 * 0)
                            crit = 3
                        if (y2 == 1):
                            wt_avg = (.65 * s2) + (.35 * 0)
                            crit = 2
                        if (y3 == 1):
                            wt_avg = (.75 * s3) + (.25 * 0)
                            crit = 1
                    
                    if plyd == 2:
                        
                        #Y1 YES, #Y2 YES, #Y3 NO
                        if (y1 == 1) & (y2 == 1) & (y3 == 0):
                            wt_avg = (.2 * s1) + (.45 * s2) + (.35 * 0)
                            crit = 3
                        #Y1 YES, #Y2 NO, #Y3 YES
                        if (y1 == 1) & (y2 == 0) & (y3 == 1):
                            wt_avg = (.1 * s1) + (.25 * 0) + (.65 * s3)
                            crit = 3
                        #Y1 NO, #Y2 YES, #Y3 YES
                        if (y1 == 0) & (y2 == 1) & (y3 == 1):
                            wt_avg = (.1 * 0) + (.25 * 0) + (.65 * s3)
                            crit = 2
                    
                    if plyd == 3:
                        
                        wt_avg = (.1 * s1) + (.25 * s2) + (.65 * s3)
                        crit = 3
                        
                    played_frame.loc[y, a] = wt_avg
                    played_frame.loc[y, 'PLAYED'] = crit
                    
                
                if ct == 0:
                    out_frame = played_frame[[pf, 'PLAYED', a]]
                else:
                    out_frame2 = played_frame[[pf, a]]
                    out_frame = out_frame.merge(out_frame2, on = pf, how = 'left')

                
                ct += 1
            
            out_frame.to_csv(out_file, index=False, header=True)
                

#-----------ID SETUP---------------



#Combine All Pitcher/Batter IDS into one master frame
def savant_master_ids(syr, eyr):
    
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
    all_group.to_csv('savant\\master_ids.csv', index=False, header=True)
                
#Initial Merge with Retro Player File, Flags Any Duplicates
#Must Manually Prune Duplicates 
def savant_ids():
    
    all_tmp = pd.read_csv('savant\\master_ids.csv')
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
    
    all_merge.to_csv('pps\\player_info.csv', index=False, header=True)

#After Pruning Duplicates, Rename File to player_info_r1.csv
#This Function Replaces Spanish Letters
#Anyone Missing from R1 goes to the R2 File
#Correct Players from R1 go to the Yes File
#Must Prune Remaining Manually, bind pruned R2 to Yes File
#There will still be duplicates, must use COUNTIF in Excel file to find them
def savant_id_2():
    
    id_file = pd.read_csv('pps\\player_info_r1.csv')
    print(id_file.loc[25, :])
    yes_file = id_file.dropna()
    id_file = id_file[id_file['birthYear'].isna()]
    id_file = id_file.reset_index()
    a = 0
    g = len(id_file)
    for a in range(g):
        n = id_file.loc[a, 'nameFirst']
        print(n)
        n_rep = n.replace('í', 'i')
        n_rep = n_rep.replace('é', 'e')
        n_rep = n_rep.replace('è', 'e')
        n_rep = n_rep.replace('ú', 'u')
        n_rep = n_rep.replace('á', 'a')
        n_rep = n_rep.replace('ó', 'o')
        n_rep = n_rep.replace('Á', 'A')
        n_rep = n_rep.replace('ñ', 'n')
        n_rep = n_rep.replace('í', 'i')
        n_rep = n_rep.replace (' Jr.', '')
        id_file.loc[a, 'nameFirst'] = n_rep
        
        n = id_file.loc[a, 'nameLast']
        n_rep = n.replace('í', 'i')
        n_rep = n_rep.replace('é', 'e')
        n_rep = n_rep.replace('í', 'i')
        n_rep = n_rep.replace('è', 'e')
        n_rep = n_rep.replace('ú', 'u')
        n_rep = n_rep.replace('á', 'a')
        n_rep = n_rep.replace('ó', 'o')
        n_rep = n_rep.replace('Á', 'A')
        n_rep = n_rep.replace('ñ', 'n')
        n_rep = n_rep.replace (' Jr.', '')
        id_file.loc[a, 'nameLast'] = n_rep
    
    player_file = pd.read_csv('retro\\players.csv')
    player_file = player_file[['birthYear', 'birthMonth', 'birthDay', 'nameFirst', 'nameLast', 'weight', 'height', 'bats', 'throws']]
    
    id_file = id_file[['player_id', 'player_name', 'counter', 'nameFirst', 'nameLast', 'nameFull']]
    
    all_merge = id_file.merge(player_file, on = ['nameFirst', 'nameLast'], how = 'left')
    
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
    
    all_merge.to_csv('pps\\player_info.csv', index=False, header=True)
    
    all_merge.to_csv('pps\\player_info_r2.csv', index=False, header=True)
    yes_file.to_csv('pps\\player_yes.csv', index=False, header=True)
    
    
#--------------NEAREST NEIGHBORS--------------

#Creates Master Baseline File of All Seasons and calculates Age
def master_baseline(syr, eyr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    
    player_file = pd.read_csv('pps\\player_info_final.csv')
    player_file = player_file[['player_id', 'player_name', 'birthYear', 'birthMonth', 'birthDay', 'weight', 'height']]
    
    for s in split_list:
        
        for t in type_list:
            
            a = syr
            
            ct = 0
            while a <= eyr:
                
                file_use = 'pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(a) + '.csv'
                
                file_open = pd.read_csv(file_use)
                file_open['PreYear'] = a
                
                if ct == 0:
                    file_bind = file_open
                else:
                    file_bind = pd.concat([file_bind, file_open], axis='rows')
                
                ct += 1
                a += 1
            
            if t == 'bat':
                file_merge = file_bind.merge(player_file, left_on = 'batter', right_on = 'player_id')
            else:
                file_merge = file_bind.merge(player_file, left_on = 'pitcher', right_on = 'player_id')
            
            file_merge['Age'] = 0
            b = 0
            h = len(file_merge)
            for b in range(h):
                
                dob_year = file_merge.loc[b, 'birthYear']
                dob_month = file_merge.loc[b, 'birthMonth']
                pre_year = file_merge.loc[b, 'PreYear']
                age_first = pre_year - dob_year
                if dob_month > 6:
                    age_first = age_first -1
                
                file_merge.loc[b, 'Age'] = age_first
            
            file_merge.to_csv('pps\\master_baseline_' + str(t) + '_' + str(s) + '.csv', index=False, header=True)
                
            

#Merge Master Baseline with Next Year's Stats
def master_nextline(syr, eyr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    
    for s in split_list:
        
        for t in type_list:
            
            file_use = pd.read_csv('pps\\master_baseline_' + str(t) + '_' + str(s) + '.csv')
            
            a = syr
            ct = 0
            while a <= eyr:
                
                year_slice = file_use[file_use['PreYear'] == a]
                next_file = pd.read_csv('stats\\' + str(s) + '_adj2_' + str(t) + '_' + str(a) + '.csv')
                
                if t == 'bat':    
                    next_merge = year_slice.merge(next_file, on = 'batter', how = 'inner')
                else:
                    next_merge = year_slice.merge(next_file, on = 'pitcher', how = 'inner')
                
                if ct == 0:
                    next_bind = next_merge
                else:
                    next_bind = pd.concat([next_bind, next_merge], axis='rows')
                
                ct += 1
                a += 1
            
            next_bind.drop(['player_id', 'birthYear', 'birthMonth', 'birthDay', 'player_type'], axis='columns', inplace=True)
            next_bind['dbltrip_x'] = next_bind['double_x'] + next_bind['triple_x']
            next_bind['dbltrip_y'] = next_bind['double_y'] + next_bind['triple_y']
            next_bind['gbfb_x'] = (1 + next_bind['ground_ball_x']) / (1 + (next_bind['fly_ball_x'] + next_bind['line_drive_x'] + next_bind['popup_x']))
            next_bind['gbfb_y'] = (1 + next_bind['ground_ball_y']) / (1 + (next_bind['fly_ball_y'] + next_bind['line_drive_y'] + next_bind['popup_y']))
            next_bind.to_csv('pps\\knn_database_' + str(t) + '_' + str(s) + '.csv', index=False, header=True)
    
#Normalize Stats, Calculate Year over Year Difference
#Final File is knn_delta
def master_normalize(syr, eyr):

    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    for s in split_list:
        
        for t in type_list:
            
            file_use = pd.read_csv('pps\\knn_database_' + str(t) + '_' + str(s) + '.csv')
            file_to_norm = file_use[['single_x', 'dbltrip_x', 'hr_x', 'strikeout_x', 'walk_x', 'gbfb_x']]
            
            single_max = file_use['single_x'].max()
            single_min = file_use['single_x'].min()
            dbltrip_max = file_use['dbltrip_x'].max()
            dbltrip_min = file_use['dbltrip_x'].min()
            hr_max = file_use['hr_x'].max()
            hr_min = file_use['hr_x'].min()
            strikeout_max = file_use['strikeout_x'].max()
            strikeout_min = file_use['strikeout_x'].min()
            walk_max = file_use['walk_x'].max()
            walk_min = file_use['walk_x'].min()
            gbfb_max = file_use['gbfb_x'].max()
            gbfb_min = file_use['gbfb_x'].min()
            
            min_max_frame = pd.DataFrame({'single':[single_min, single_max], 'dbltrip':[dbltrip_min, dbltrip_max],
                                          'hr':[hr_min, hr_max], 'strikeout':[strikeout_min, strikeout_max],
                                          'walk':[walk_min, walk_max], 'gbfb':[gbfb_min, gbfb_max]})
            
            min_max_frame.to_csv('pps\\minmax_' + str(t) + '_' + str(s) + '.csv', index=False, header=True)
            
            print(min_max_frame)
            
            print(single_max)
            
            normalized_df=(file_to_norm-file_to_norm.min())/(file_to_norm.max()-file_to_norm.min())
            
            if t == 'bat':
                
                file_no_norm = file_use[['batter', 'player_name', 'PreYear', 'Age', 'PLAYED', 'double_y',
                                         'fly_ball_y', 'ground_ball_y', 'hr_y', 'line_drive_y', 'popup_y',
                                         'sacb_y', 'single_y', 'strikeout_y', 'triple_y', 'walk_y']]
                
            else:
                
                file_no_norm = file_use[['pitcher', 'player_name', 'PreYear', 'Age', 'PLAYED', 'double_y',
                                         'fly_ball_y', 'ground_ball_y', 'hr_y', 'line_drive_y', 'popup_y',
                                         'sacb_y', 'single_y', 'strikeout_y', 'triple_y', 'walk_y']]
            
            combo_frame = pd.concat([normalized_df, file_no_norm], axis='columns')
            
            a = syr
            ct = 0
            while a <= eyr:
                
                last_year = a - 1
                past_file = pd.read_csv('stats\\' + str(s) + '_adj2_' + str(t) + '_' + str(last_year) + '.csv')
                
                past_file = past_file.drop(['pa', 'player_type'], axis='columns')
            
                
                year_slice = combo_frame[combo_frame['PreYear'] == a]
                
                if t == 'bat':
                    year_merge = year_slice.merge(past_file, on = 'batter', how = 'inner')
                else:
                    year_merge = year_slice.merge(past_file, on = 'pitcher', how = 'inner')
                
                if ct == 0:
                    year_combo = year_merge
                else:
                    year_combo = pd.concat([year_combo, year_merge], axis='rows')
                
                ct += 1
                a += 1
            
            for at in at_list:
                
                delta_col = str(at) + '_d'
                y_col = str(at) + '_y'
                
                year_combo[delta_col] = year_combo[y_col] - year_combo[at]

            if t == 'bat':
                year_combo = year_combo[['batter', 'player_name', 'PreYear', 'Age', 'PLAYED', 'single_x', 'dbltrip_x', 'hr_x', 'strikeout_x',
                                         'walk_x', 'gbfb_x', 'double_d', 'fly_ball_d', 'ground_ball_d', 'hr_d', 'line_drive_d', 'popup_d',
                                         'sacb_d', 'single_d', 'strikeout_d', 'triple_d', 'walk_d']]
            else:
                year_combo = year_combo[['pitcher', 'player_name', 'PreYear', 'Age', 'PLAYED', 'single_x', 'dbltrip_x', 'hr_x', 'strikeout_x',
                                         'walk_x', 'gbfb_x', 'double_d', 'fly_ball_d', 'ground_ball_d', 'hr_d', 'line_drive_d', 'popup_d',
                                         'sacb_d', 'single_d', 'strikeout_d', 'triple_d', 'walk_d']]
                
            year_combo.to_csv('pps\\knn_delta_' + str(t) + '_' + str(s) + '.csv', index=False, header=True)
            
            
#Feeder Function that Processes the KNN Matching and Projects Next Years Stats
def knn_do(spl, typ, pid, yr):
    
    knn_file = pd.read_csv('pps\\knn_delta_' + str(typ) + '_' + str(spl) + '.csv')
    base_file = pd.read_csv('pps\\baseline_' + str(typ) + '_' + str(spl) + '_' + str(yr) + '.csv')
    player_info = pd.read_csv('pps\\player_info_final.csv')
    minmax_frame = pd.read_csv('pps\\minmax_' + str(typ) + '_' + str(spl) + '.csv')
    
    if typ == 'bat':
        base_line = base_file[base_file['batter'] == pid]
        chk = 0
        yr_chk = yr
        while chk == 0:
            lastyear_file = pd.read_csv('stats\\' + str(spl) + '_adj2_' + str(typ) + '_' + str(yr_chk - 1) + '.csv')
            lastyear_line = lastyear_file[lastyear_file['batter'] == pid]
            if len(lastyear_line) > 0:
                chk += 1
            else:
                yr_chk = yr_chk - 1
    
    else:
        base_line = base_file[base_file['pitcher'] == pid]
        chk = 0
        yr_chk = yr
        while chk == 0:
            lastyear_file = pd.read_csv('stats\\' + str(spl) + '_adj2_' + str(typ) + '_' + str(yr_chk - 1) + '.csv')
            lastyear_line = lastyear_file[lastyear_file['pitcher'] == pid]
            if len(lastyear_line) > 0:
                chk += 1
            else:
                yr_chk = yr_chk - 1
    
    player_line = player_info[player_info['player_id'] == pid]

    dob_year = int(player_line['birthYear'])
    dob_month = int(player_line['birthMonth'])
    age_first = yr - dob_year
    if dob_month > 6:
        age_first = age_first -1

    played_type = int(base_line['PLAYED'])
    
    if age_first > 40:
        age_first = 40
    
    age_high = age_first + 1
    age_low = age_first - 1
    
    single_x = float(base_line['single'])
    double_x = float(base_line['double'])
    triple_x = float(base_line['triple'])
    hr_x = float(base_line['hr'])
    strikeout_x = float(base_line['strikeout'])
    walk_x = float(base_line['walk'])
    ground_ball_x = float(base_line['ground_ball'])
    fly_ball_x = float(base_line['fly_ball'])
    line_drive_x = float(base_line['line_drive'])
    popup_x = float(base_line['popup'])
    
    dbltrip_x = double_x + triple_x
    gbfb_x = (1 + ground_ball_x) / (1 + (fly_ball_x + line_drive_x + popup_x))

    single_min = minmax_frame.loc[0, 'single']
    single_max = minmax_frame.loc[1, 'single']
    dbltrip_min = minmax_frame.loc[0, 'dbltrip']
    dbltrip_max = minmax_frame.loc[1, 'dbltrip']
    hr_min = minmax_frame.loc[0, 'hr']
    hr_max = minmax_frame.loc[1, 'hr']
    strikeout_min = minmax_frame.loc[0, 'strikeout']
    strikeout_max = minmax_frame.loc[1, 'strikeout']
    walk_min = minmax_frame.loc[0, 'walk']
    walk_max = minmax_frame.loc[1, 'walk']
    gbfb_min = minmax_frame.loc[0, 'gbfb']
    gbfb_max = minmax_frame.loc[1, 'gbfb']
    
    single_range = single_max - single_min
    dbltrip_range = dbltrip_max - dbltrip_min
    hr_range = hr_max - hr_min
    strikeout_range = strikeout_max - strikeout_min
    walk_range = walk_max - walk_min
    gbfb_range = gbfb_max - gbfb_min
    
    norm_single = (single_x - single_min) / single_range
    norm_dbltrip = (dbltrip_x - dbltrip_min) / dbltrip_range
    norm_hr = (hr_x - hr_min) / hr_range
    norm_strikeout = (strikeout_x - strikeout_min) / strikeout_range
    norm_walk = (walk_x - walk_min) / walk_range
    norm_gbfb = (gbfb_x - gbfb_min) / gbfb_range
    
    
    knn_first_filter = knn_file[(knn_file['Age'] >= age_low) & (knn_file['Age'] <= age_high) & (knn_file['PLAYED'] == played_type)]
    
    if (len(knn_first_filter) < 5) & (played_type < 3):
        knn_first_filter = knn_file[(knn_file['Age'] >= age_low) & (knn_file['Age'] <= age_high) & (knn_file['PLAYED'] == 3)]
    
    knn_clust_portion = knn_first_filter
    
    knn_single_dif = abs(knn_clust_portion['single_x'] - norm_single)
    knn_dbltrip_dif = abs(knn_clust_portion['dbltrip_x'] - norm_dbltrip)
    knn_hr_dif = abs(knn_clust_portion['hr_x'] - norm_hr)
    knn_strikeout_dif = abs(knn_clust_portion['strikeout_x'] - norm_strikeout)
    knn_walk_dif = abs(knn_clust_portion['walk_x'] - norm_walk)
    knn_gbfb_dif = abs(knn_clust_portion['gbfb_x'] - norm_gbfb)
    
    #print([single_x, dbltrip_x, hr_x, strikeout_x, walk_x, gbfb_x])
    #print([norm_single, norm_dbltrip, norm_hr, norm_strikeout, norm_walk, norm_gbfb])
    
    dist_frame = pd.concat([knn_single_dif, knn_dbltrip_dif, knn_hr_dif, knn_strikeout_dif, knn_walk_dif, knn_gbfb_dif], axis='columns')
    
    #print(dist_frame)
 
    dist_score = dist_frame.mean(axis='columns')

    dif_frame = pd.concat([knn_first_filter, dist_score], axis='columns')

    dif_frame.rename(columns={0:'dist'}, inplace=True)
    
    dif_score_frame = dif_frame.sort_values('dist')
    
    dif_len = len(dif_score_frame)
    dif_round = round(dif_len * .02, 0)
    dif_round = int(dif_round) - 1
    
    if dif_round < 8:
        
        if dif_len < 8:
            dif_round = dif_len
        else:
            dif_round = 8
    
    dif_score_frame = dif_score_frame.reset_index()
    
    dif_score_use = dif_score_frame.iloc[0:dif_round, :]
    
    dif_score_use['weight'] = 1 - dif_score_use['dist']
    
    #'double_d', 'fly_ball_d', 'ground_ball_d', 'hr_d', 'line_drive_d', 'popup_d',
    #'sacb_d', 'single_d', 'strikeout_d', 'triple_d', 'walk_d'
                                         
    dif_score_use['double_w'] = dif_score_use['double_d'] * dif_score_use['weight']
    dif_score_use['fly_ball_w'] = dif_score_use['fly_ball_d'] * dif_score_use['weight']
    dif_score_use['ground_ball_w'] = dif_score_use['ground_ball_d'] * dif_score_use['weight']
    dif_score_use['hr_w'] = dif_score_use['hr_d'] * dif_score_use['weight']
    dif_score_use['line_drive_w'] = dif_score_use['line_drive_d'] * dif_score_use['weight']
    dif_score_use['popup_w'] = dif_score_use['popup_d'] * dif_score_use['weight']
    dif_score_use['sacb_w'] = dif_score_use['sacb_d'] * dif_score_use['weight']
    dif_score_use['single_w'] = dif_score_use['single_d'] * dif_score_use['weight']
    dif_score_use['strikeout_w'] = dif_score_use['strikeout_d'] * dif_score_use['weight']
    dif_score_use['triple_w'] = dif_score_use['triple_d'] * dif_score_use['weight']
    dif_score_use['walk_w'] = dif_score_use['walk_d'] * dif_score_use['weight']
    
    
    weight_sum = dif_score_use['weight'].sum()
    
    double_proj = dif_score_use['double_w'].sum() / weight_sum
    fly_ball_proj = dif_score_use['fly_ball_w'].sum() / weight_sum
    ground_ball_proj = dif_score_use['ground_ball_w'].sum() / weight_sum
    hr_proj = dif_score_use['hr_w'].sum() / weight_sum
    line_drive_proj = dif_score_use['line_drive_w'].sum() / weight_sum
    popup_proj = dif_score_use['popup_w'].sum() / weight_sum
    sacb_proj = dif_score_use['sacb_w'].sum() / weight_sum
    single_proj = dif_score_use['single_w'].sum() / weight_sum
    strikeout_proj = dif_score_use['strikeout_w'].sum() / weight_sum
    triple_proj = dif_score_use['triple_w'].sum() / weight_sum
    walk_proj = dif_score_use['walk_w'].sum() / weight_sum
    
    double_fin = float(double_proj + lastyear_line['double'])
    ground_ball_fin = float(ground_ball_proj + lastyear_line['ground_ball'])
    fly_ball_fin = float(fly_ball_proj + lastyear_line['fly_ball'])
    hr_fin = float(hr_proj + lastyear_line['hr'])
    line_drive_fin = float(line_drive_proj + lastyear_line['line_drive'])
    popup_fin = float(popup_proj + lastyear_line['popup'])
    sacb_fin = float(sacb_proj + lastyear_line['sacb'])
    single_fin = float(single_proj + lastyear_line['single'])
    strikeout_fin = float(strikeout_proj + lastyear_line['strikeout'])
    triple_fin = float(triple_proj + lastyear_line['triple'])
    walk_fin = float(walk_proj + lastyear_line['walk'])

    fin_list = [double_fin, fly_ball_fin, ground_ball_fin, hr_fin, line_drive_fin, popup_fin, sacb_fin, single_fin, strikeout_fin, triple_fin, walk_fin]
    
    return(fin_list)

#Loop that Runs Through Every ID in the Baseline File
def knn_loop(yr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    for s in split_list:
        
        for t in type_list:
            
            baseline_file = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv')
            
            if t == 'bat':
                baseline_input = baseline_file[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']]
                
            else:
                baseline_input = baseline_file[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']]
            
            ct = 0
            a = 0
            g = len(baseline_file)
            
            for a in range(g):
                
                pid = int(baseline_file.iloc[a, 0])
                
                knn_get = knn_do(s, t, pid, yr)
                
                b = 0
                h = len(knn_get)
                
                for b in range(h):
                    
                    at_use = at_list[b]
                    baseline_input.loc[a, at_use] = knn_get[b]
                
            baseline_input.to_csv('pps\\atm_' + str(t) + '_' + str(s) + '.csv', index=False, header=True)
                    

#Project Number of Pitches
def bl_numpitch(syr, eyr):
    
    #Batters
    
    p_file = pd.read_csv('stats\\numpitch_bat_' + str(eyr) + '.csv')
    p_file = p_file[['batter', 'adj']]

    a = syr
    b = eyr - 1
    ct = 0
    while b >= a:
        
        p_file2 = pd.read_csv('stats\\numpitch_bat_' + str(b) + '.csv')
        p_file2 = p_file2[['batter', 'adj']]
        if ct == 0:
            master_merge = p_file.merge(p_file2, on = 'batter', how = 'left', suffixes=('_1', '_2'))
        else:
            master_merge = master_merge.merge(p_file2, on = 'batter', how = 'left', suffixes=('_3', '_4'))
        
        b = b - 1
        ct = ct + 1
        
    a = 1
    g = len(master_merge)
    h = master_merge.shape[1] - 1
    
    for a in range(g):
        
        played_list = [0, 0, 0]
        use_list = [0, 0, 0]
        
        b = 1
        while (b <= h):
        
            stat_use = master_merge.iloc[a, b]
            if (np.isnan(stat_use) == False):
                played_list[b - 1] = 1
                use_list[b - 1] = stat_use
            
            b += 1 
        
        if played_list == [1, 1, 1]:
            wt_stat = (use_list[0] * .6) + (use_list[1] * .25) + (use_list[2] * .15)
        if played_list == [1, 1, 0]:
            wt_stat = (use_list[0] * .7) + (use_list[2] * .3)
        if played_list == [1, 0, 0]:
            wt_stat = use_list[0]
        if played_list == [1, 0, 1]:
            wt_stat = (use_list[0] * .85) + (use_list[2] * .15)
        if played_list == [0, 1, 1]:
            wt_stat = (use_list[1] * .66) + (use_list[2] * .33)
        if played_list == [0, 1, 0]:
            wt_stat = use_list[1]
        if played_list == [0, 0, 1]:
            wt_stat = use_list[2]
        if played_list == [0, 0, 0]:
            wt_stat = 3.95
            
        master_merge.loc[a, 'use'] = wt_stat

    master_merge = master_merge[['batter', 'use']]
    master_merge.to_csv('pps\\numpitch_bat_' + str(eyr + 1) + '.csv', index=False, header=True)
    
    #Pitchers
    
    p_file = pd.read_csv('stats\\numpitch_pit_' + str(eyr) + '.csv')
    p_file = p_file[['pitcher', 'adj']]

    a = syr
    b = eyr - 1
    ct = 0
    while b >= a:
        
        p_file2 = pd.read_csv('stats\\numpitch_pit_' + str(b) + '.csv')
        p_file2 = p_file2[['pitcher', 'adj']]
        if ct == 0:
            master_merge = p_file.merge(p_file2, on = 'pitcher', how = 'left', suffixes=('_1', '_2'))
        else:
            master_merge = master_merge.merge(p_file2, on = 'pitcher', how = 'left', suffixes=('_3', '_4'))
        
        b = b - 1
        ct = ct + 1
        
    a = 1
    g = len(master_merge)
    h = master_merge.shape[1] - 1
    
    for a in range(g):
        
        played_list = [0, 0, 0]
        use_list = [0, 0, 0]
        
        b = 1
        while (b <= h):
        
            stat_use = master_merge.iloc[a, b]
            if (np.isnan(stat_use) == False):
                played_list[b - 1] = 1
                use_list[b - 1] = stat_use
            
            b += 1 
        
        if played_list == [1, 1, 1]:
            wt_stat = (use_list[0] * .6) + (use_list[1] * .25) + (use_list[2] * .15)
        if played_list == [1, 1, 0]:
            wt_stat = (use_list[0] * .7) + (use_list[2] * .3)
        if played_list == [1, 0, 0]:
            wt_stat = use_list[0]
        if played_list == [1, 0, 1]:
            wt_stat = (use_list[0] * .85) + (use_list[2] * .15)
        if played_list == [0, 1, 1]:
            wt_stat = (use_list[1] * .66) + (use_list[2] * .33)
        if played_list == [0, 1, 0]:
            wt_stat = use_list[1]
        if played_list == [0, 0, 1]:
            wt_stat = use_list[2]
        if played_list == [0, 0, 0]:
            wt_stat = 3.95
            
        master_merge.loc[a, 'use'] = wt_stat

    master_merge = master_merge[['pitcher', 'use']]
    master_merge.to_csv('pps\\numpitch_pit_' + str(eyr + 1) + '.csv', index=False, header=True)
    
#Project Max Number of Pitches in Starts/Relief

def proj_maxpitch(yr):
    
    pps_use = pd.read_csv('stats\\maxpitch_' + str(yr - 1) + '.csv')
    pps_use.to_csv('pps\\maxpitch_' + str(yr) + '.csv', index=False, header=True)
    
#Project League Averages

def proj_league_avg(yr):
    
    lg_file1 = pd.read_csv('stats\\league_splits_' + str(yr - 1) + '.csv')
    lg_file2 = pd.read_csv('stats\\league_splits_' + str(yr - 2) + '.csv')
    lg_file3 = pd.read_csv('stats\\league_splits_' + str(yr - 3) + '.csv')
    lg_rep = pd.read_csv('stats\\league_splits_' + str(yr - 1) + '.csv')
    lg_rep.iloc[:, 1:] = 0
    
    lg_calc_1 = (lg_file1.iloc[:, 1:] * .7)
    lg_calc_2 = (lg_file2.iloc[:, 1:] * .2)
    lg_calc_3 = (lg_file3.iloc[:, 1:] * .1)
    
    lg_proj = lg_calc_1 + lg_calc_2 + lg_calc_3
    lg_rep.iloc[:, 1:] = lg_proj
    
    lg_rep.to_csv('pps\\league_splits_' + str(yr) + '.csv', index=False, header=True)

#Project Home Advantges

def proj_ha(yr):
    
    lg_file1 = pd.read_csv('stats\\ha_frame_' + str(yr - 1) + '.csv')
    lg_file2 = pd.read_csv('stats\\ha_frame_' + str(yr - 2) + '.csv')
    lg_file3 = pd.read_csv('stats\\ha_frame_' + str(yr - 3) + '.csv')
    lg_rep = pd.read_csv('stats\\ha_frame_' + str(yr - 1) + '.csv')
    lg_rep.iloc[:, 3:] = 0
    
    #COVID WEIGHTS
    
    lg_calc_1 = (lg_file1.iloc[:, 3:] * .2)
    lg_calc_2 = (lg_file2.iloc[:, 3:] * .4)
    lg_calc_3 = (lg_file3.iloc[:, 3:] * .4)
    
    lg_proj = lg_calc_1 + lg_calc_2 + lg_calc_3
    lg_rep.iloc[:, 3:] = lg_proj
    
    lg_rep.to_csv('pps\\ha_frame_' + str(yr) + '.csv', index=False, header=True)

#Project Park Factors(Manually Update Config File)
def proj_parkfac(yr):
    
    lg_file1 = pd.read_csv('stats\\wn_parkfactors_' + str(yr - 2) + '.csv')
    lg_file2 = pd.read_csv('stats\\wn_parkfactors_' + str(yr - 1) + '.csv')
    lg_rep = pd.read_csv('stats\\wn_parkfactors_' + str(yr - 1) + '.csv')
    conf_file = pd.read_csv('config\parkfac.csv')
    
    a = 0
    g = len(conf_file)
    
    for a in range(g):
        
        tm = conf_file.loc[a, 'team']
        team_1 = conf_file.loc[a, 'LINE_1']
        team_2 = conf_file.loc[a, 'LINE_2']
        wt_1 = conf_file.loc[a, 'WT_1']
        wt_2 = conf_file.loc[a, 'WT_2']
        
        lg_file1_filt = lg_file1[lg_file1['team'] == team_1]
        lg_file2_filt = lg_file2[lg_file2['team'] == team_2]
        
        lg_file_1_half = lg_file1_filt.iloc[:, 2:] * wt_1
        lg_file_2_half = lg_file2_filt.iloc[:, 2:] * wt_2
        lg_file_comb = lg_file_1_half + lg_file_2_half
        
        lg_rep.iloc[lg_rep['team'] == tm, 2:] = lg_file_comb 
        
    lg_rep.to_csv('pps\\wnparkfac_' + str(yr) + '.csv', index=False, header=True)

#Project Fielding %
def proj_errors(yr):
    
    error_frame = pd.read_csv('stats\\errors_' + str(yr - 1) + '.csv')
    error_rep = pd.read_csv('stats\\errors_' + str(yr - 1) + '.csv')
    error_avg = error_frame['field_pct'].mean()
    
    error_frame['field_pct'] = (error_frame['field_pct'] * .5) + (.5 * error_avg)
    
    error_frame.to_csv('pps\errors_' + str(yr) + '.csv', index=False, header=True)
    
    error_rep = error_rep.iloc[0, :]
    error_rep['field_pct'] = error_avg
    
    error_rep.to_csv('pps\\league_errors_' +str(yr) + '.csv', index=False, header=True)
    
#Merge Baseline and KNN ATM Files to Project VLA Stats
def proj_bl_atm_merge(yr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']

    for s in split_list:
        
        for t in type_list:
            
            baseline_file = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv')
            atm_file = pd.read_csv('pps\\atm_' + str(t) + '_' + str(s) + '.csv')
            rep_file = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv')
            
            if t == 'bat':
                files_merge = baseline_file.merge(atm_file, on = 'batter')
            else:
                files_merge = baseline_file.merge(atm_file, on = 'pitcher')
                
            half_1 = files_merge.loc[:, 'double_x':'walk_x'] * .5
            half_2 = files_merge.loc[:, 'double_y':'walk_y'] * .5
            
            half_1.rename(columns={'double_x':'double', 'fly_ball_x':'fly_ball', 'ground_ball_x':'ground_ball', 'hr_x':'hr', 'line_drive_x':'line_drive',
                                   'popup_x':'popup', 'sacb_x':'sacb', 'single_x':'single', 'strikeout_x':'strikeout', 'triple_x':'triple', 'walk_x':'walk'},
                          inplace=True)
            
            half_2.rename(columns={'double_y':'double', 'fly_ball_y':'fly_ball', 'ground_ball_y':'ground_ball', 'hr_y':'hr', 'line_drive_y':'line_drive',
                                   'popup_y':'popup', 'sacb_y':'sacb', 'single_y':'single', 'strikeout_y':'strikeout', 'triple_y':'triple', 'walk_y':'walk'},
                          inplace=True)
            
            comb_half = half_1 + half_2
            
            rep_file.loc[:, 'double':'walk'] = comb_half
            
            rep_file.to_csv('pps\\proj_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv', index=False, header=True)

#Project Rookie Baselines
def proj_rookie(yr):
    
    split_list = ['lvl', 'lvr', 'rvl', 'rvr']
    type_list = ['bat', 'pit']
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    
    
    for s in split_list:
        
        for t in type_list:
            
            file_1 = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr - 1) + '.csv')
            file_2 = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr - 2) + '.csv')
            file_3 = pd.read_csv('pps\\baseline_' + str(t) + '_' + str(s) + '_' + str(yr - 3) + '.csv')
            
            file_1_filt = file_1[file_1['PLAYED'] == 1]
            file_2_filt = file_2[file_2['PLAYED'] == 1]
            file_3_filt = file_3[file_3['PLAYED'] == 1]
            
            file_1_info = pd.read_csv('stats\\' + str(s) + '_adj2_' + str(t) + '_' + str(yr - 1) + '.csv')
            file_2_info = pd.read_csv('stats\\' + str(s) + '_adj2_' + str(t) + '_' + str(yr - 2) + '.csv')
            file_3_info = pd.read_csv('stats\\' + str(s) + '_adj2_' + str(t) + '_' + str(yr - 3) + '.csv')
            
            if t == 'bat':
                file_1_slice = file_1_info[['batter', 'player_type']]
                file_2_slice = file_2_info[['batter', 'player_type']]
                file_3_slice = file_3_info[['batter', 'player_type']]
                
                file_1_join = file_1_filt.merge(file_1_slice, on = 'batter', how = 'left')
                file_2_join = file_2_filt.merge(file_2_slice, on = 'batter', how = 'left')
                file_3_join = file_3_filt.merge(file_3_slice, on = 'batter', how = 'left')
                
            else:
                file_1_slice = file_1_info[['pitcher', 'player_type']]
                file_2_slice = file_2_info[['pitcher', 'player_type']]
                file_3_slice = file_3_info[['pitcher', 'player_type']]
                
                file_1_join = file_1_filt.merge(file_1_slice, on = 'pitcher', how = 'left')
                file_2_join = file_2_filt.merge(file_2_slice, on = 'pitcher', how = 'left')
                file_3_join = file_3_filt.merge(file_3_slice, on = 'pitcher', how = 'left')
            
            file_conc = pd.concat([file_1_join, file_2_join, file_3_join], axis='rows')

            file_conc = file_conc.dropna()
            
            file_gonc = file_conc.groupby('player_type')[at_list].agg('mean')
            file_gonc = file_gonc.reset_index()

            file_gonc.to_csv('pps\\rookie_' + str(t) + '_' + str(s) + '_' + str(yr) + '.csv', index = False, header = True)            
            
    
    
    
