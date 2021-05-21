#This is the 3rd part of the baseball process, which factors in home advantage and opponent adjustments.
#Home advantage is calculated in a simple manner by determining the delta from the league average for away stats and home stats.
#Opponent Adjustments seek to adjust each player's stats based on the strength of batters/pitchers they have faced
#The idea is to "neutralize" each player's stats to where the stats would reflect what their expected stats would be against the league average pitcher/batter.
#2 rounds of adjustments are used to stablilize.
#Opponent strength is determined by calculating their stats vs. the league average as was done in the Savant Wrangle file.

import pandas as pd
import numpy as np


#-----------------CURRENT--------------------

def current_hadj(yr):
    
    home_adv_calcu(yr)
    impute_ha(yr)
    ha_neutralize(yr)
    adj_loop(yr, 2)
    savant_batters_do(yr)
    
def impute_ha(yr):
    
    wx_file = pd.read_csv("savant\\savant_wx_" + str(yr) + ".csv")
    wx_line = len(wx_file)
    
    this_year_file = pd.read_csv('stats\\ha_frame_' + str(yr) + '.csv')
    pps_file = pd.read_csv('pps\\ha_frame_' + str(yr) + '.csv')
    
    out_file = pd.read_csv('pps\\ha_frame_' + str(yr) + '.csv')
    
    if wx_line < 100000:
        
        this_year_short = this_year_file.iloc[:, 3:14]
        pps_short = pps_file.iloc[:, 3:14]
        
        actual_use = wx_line / 100000
        impute_use = 1 - actual_use
        
        ty_calc = this_year_short * actual_use
        pp_calc = pps_short * impute_use
        
        cc_calc = ty_calc + pp_calc
        
        out_file.iloc[:, 3:14] = cc_calc
        
    out_file.to_csv('stats\\ha_frame_' + str(yr) + '.csv', encoding='utf-8-sig')
        
        

#-----------------PAST-----------------------

def home_adv_calcu(yr):
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    savant_file = pd.read_csv('savant\\\savant_wx_' + str(yr) + '.csv')
    
    home_slice = savant_file[savant_file['inning_topbot'] == 'Bot']
    away_slice = savant_file[savant_file['inning_topbot'] == 'Top']
    
    all_slice = savant_file
    
    for a in at_list:
       
        act_log = home_slice['actual_type'] == a
        home_slice[a] = act_log
        home_slice[a] = home_slice[a].astype(int)
        
        act_log = away_slice['actual_type'] == a
        away_slice[a] = act_log
        away_slice[a] = away_slice[a].astype(int)
        
        act_log = all_slice['actual_type'] == a
        all_slice[a] = act_log
        all_slice[a] = all_slice[a].astype(int)
    
    home_group = home_slice.groupby(['stand', 'p_throws'])[at_list].agg('mean')
    home_group = home_group.reset_index()
    home_group_cut = home_group.iloc[:, 2:13]
    
    away_group = away_slice.groupby(['stand', 'p_throws'])[at_list].agg('mean')
    away_group = away_group.reset_index()
    away_group_cut = away_group.iloc[:, 2:13]
    
    all_group = all_slice.groupby(['stand', 'p_throws'])[at_list].agg('mean')
    all_group = all_group.reset_index()
    all_group_cut = all_group.iloc[:, 2:13]
    
    home_adv = home_group_cut - all_group_cut
    away_adv = away_group_cut - all_group_cut
    
    home_group.iloc[:, 2:13] = home_adv
    away_group.iloc[:, 2:13] = away_adv
    
    home_adv_frame = home_group
    away_adv_frame = away_group
    
    home_adv_frame['inning_topbot'] = 'Bot'
    away_adv_frame['inning_topbot'] = 'Top'
    
    master_adv_frame = pd.concat([home_adv_frame, away_adv_frame], axis='rows')
    
    master_adv_frame = master_adv_frame[['inning_topbot', 'stand', 'p_throws', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']]

    master_adv_frame.to_csv('stats\\ha_frame_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    

def ha_neutralize(yr):
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    savant_file = pd.read_csv('savant\\\savant_wx_' + str(yr) + '.csv')
    
    ha_frame = pd.read_csv('stats\\ha_frame_' + str(yr) + '.csv')
    
    savant_merge = savant_file.merge(ha_frame, on = ['inning_topbot', 'stand', 'p_throws'])
    
    #Batters
    
    park_bat_groups = savant_merge.groupby(['stand', 'p_throws', 'batter'])[at_list].agg('mean')
    park_bat_groups = park_bat_groups.reset_index()
    
    print(park_bat_groups)
    
    stand_split = ['L', 'R']
    throw_split = ['L', 'R']
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = park_bat_groups[(park_bat_groups['stand'] == st) & (park_bat_groups['p_throws'] == th)]
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_pkwnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_hapkwnvla_bat_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_pkwnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_hapkwnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_pkwnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_hapkwnvla_bat_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_pkwnvla_bat_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_hapkwnvla_bat_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='batter')
            #print(wx_slice_merge[['batter', 'p_throws', 'stand', 'hr_x', 'hr_y']])
                
            for a in at_list:
                    
                stat_x = str(a) + '_x'
                stat_y = str(a) + '_y'
                    
                comb_stat = wx_slice_merge[stat_y] - wx_slice_merge[stat_x]
                    
                wx_slice_merge[a] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')
            
    #Pitchers
    
    park_bat_groups = savant_merge.groupby(['stand', 'p_throws', 'pitcher'])[at_list].agg('mean')
    park_bat_groups = park_bat_groups.reset_index()
    
    print(park_bat_groups)
    
    stand_split = ['L', 'R']
    throw_split = ['L', 'R']
    
    for st in stand_split:
        
        for th in throw_split:
            
            wx_slice = park_bat_groups[(park_bat_groups['stand'] == st) & (park_bat_groups['p_throws'] == th)]
            
            if (st == "L") & (th == "L"):
                vla_file = pd.read_csv('stats\\lvl_pkwnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvl_hapkwnvla_pit_' + str(yr) + '.csv'
            if (st == "L") & (th == "R"):
                vla_file = pd.read_csv('stats\\lvr_pkwnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\lvr_hapkwnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "L"):
                vla_file = pd.read_csv('stats\\rvl_pkwnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvl_hapkwnvla_pit_' + str(yr) + '.csv'
            if (st == "R") & (th == "R"):
                vla_file = pd.read_csv('stats\\rvr_pkwnvla_pit_' + str(yr) + '.csv')
                vla_out = 'stats\\rvr_hapkwnvla_pit_' + str(yr) + '.csv'
                
            wx_slice_merge = wx_slice.merge(vla_file, on='pitcher')
            #print(wx_slice_merge[['batter', 'p_throws', 'stand', 'hr_x', 'hr_y']])
                
            for a in at_list:
                    
                stat_x = str(a) + '_x'
                stat_y = str(a) + '_y'
                    
                comb_stat = wx_slice_merge[stat_y] - wx_slice_merge[stat_x]
                    
                wx_slice_merge[a] = comb_stat
                
            wx_slice_sel = wx_slice_merge[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
            wx_slice_sel.to_csv(vla_out, index=False, header=True, encoding='utf-8-sig')
            
def opp_adjust(yr, rd):
    
    thresh = 20
    
    at_list = ['double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk']
    savant_file = pd.read_csv('savant\\\savant_wx_' + str(yr) + '.csv')
    
    lvl_frame = savant_file[(savant_file['stand'] == 'L') & (savant_file['p_throws'] == 'L')]
    lvr_frame = savant_file[(savant_file['stand'] == 'L') & (savant_file['p_throws'] == 'R')]
    rvl_frame = savant_file[(savant_file['stand'] == 'R') & (savant_file['p_throws'] == 'L')]
    rvr_frame = savant_file[(savant_file['stand'] == 'R') & (savant_file['p_throws'] == 'R')]
    
    lvl_rep_bat = pd.read_csv('stats\\lvl_hapkwnvla_bat_' + str(yr) + '.csv')
    lvr_rep_bat = pd.read_csv('stats\\lvr_hapkwnvla_bat_' + str(yr) + '.csv')
    rvl_rep_bat = pd.read_csv('stats\\rvl_hapkwnvla_bat_' + str(yr) + '.csv')
    rvr_rep_bat = pd.read_csv('stats\\rvr_hapkwnvla_bat_' + str(yr) + '.csv')
        
    lvl_rep_pit = pd.read_csv('stats\\lvl_hapkwnvla_pit_' + str(yr) + '.csv')
    lvr_rep_pit = pd.read_csv('stats\\lvr_hapkwnvla_pit_' + str(yr) + '.csv')
    rvl_rep_pit = pd.read_csv('stats\\rvl_hapkwnvla_pit_' + str(yr) + '.csv')
    rvr_rep_pit = pd.read_csv('stats\\rvr_hapkwnvla_pit_' + str(yr) + '.csv')
    
    if rd == 1:
        lvl_stats_bat = pd.read_csv('stats\\lvl_hapkwnvla_bat_' + str(yr) + '.csv')
        lvr_stats_bat = pd.read_csv('stats\\lvr_hapkwnvla_bat_' + str(yr) + '.csv')
        rvl_stats_bat = pd.read_csv('stats\\rvl_hapkwnvla_bat_' + str(yr) + '.csv')
        rvr_stats_bat = pd.read_csv('stats\\rvr_hapkwnvla_bat_' + str(yr) + '.csv')
        
        lvl_stats_pit = pd.read_csv('stats\\lvl_hapkwnvla_pit_' + str(yr) + '.csv')
        lvr_stats_pit = pd.read_csv('stats\\lvr_hapkwnvla_pit_' + str(yr) + '.csv')
        rvl_stats_pit = pd.read_csv('stats\\rvl_hapkwnvla_pit_' + str(yr) + '.csv')
        rvr_stats_pit = pd.read_csv('stats\\rvr_hapkwnvla_pit_' + str(yr) + '.csv')
    if rd == 2:
        lvl_stats_bat = pd.read_csv('stats\\lvl_adj1_bat_' + str(yr) + '.csv')
        lvr_stats_bat = pd.read_csv('stats\\lvr_adj1_bat_' + str(yr) + '.csv')
        rvl_stats_bat = pd.read_csv('stats\\rvl_adj1_bat_' + str(yr) + '.csv')
        rvr_stats_bat = pd.read_csv('stats\\rvr_adj1_bat_' + str(yr) + '.csv')
        
        lvl_stats_pit = pd.read_csv('stats\\lvl_adj1_pit_' + str(yr) + '.csv')
        lvr_stats_pit = pd.read_csv('stats\\lvr_adj1_pit_' + str(yr) + '.csv')
        rvl_stats_pit = pd.read_csv('stats\\rvl_adj1_pit_' + str(yr) + '.csv')
        rvr_stats_pit = pd.read_csv('stats\\rvr_adj1_pit_' + str(yr) + '.csv')
    if rd >= 3:
        lvl_stats_bat = pd.read_csv('stats\\lvl_adj2_bat_' + str(yr) + '.csv')
        lvr_stats_bat = pd.read_csv('stats\\lvr_adj2_bat_' + str(yr) + '.csv')
        rvl_stats_bat = pd.read_csv('stats\\rvl_adj2_bat_' + str(yr) + '.csv')
        rvr_stats_bat = pd.read_csv('stats\\rvr_adj2_bat_' + str(yr) + '.csv')
        
        lvl_stats_pit = pd.read_csv('stats\\lvl_adj2_pit_' + str(yr) + '.csv')
        lvr_stats_pit = pd.read_csv('stats\\lvr_adj2_pit_' + str(yr) + '.csv')
        rvl_stats_pit = pd.read_csv('stats\\rvl_adj2_pit_' + str(yr) + '.csv')
        rvr_stats_pit = pd.read_csv('stats\\rvr_adj2_pit_' + str(yr) + '.csv')

    
    la_frame = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    la_frame = la_frame.pivot_table(columns='actual_type', values='LVLO')
    la_frame = la_frame.reset_index()
    la_frame.drop(['index'], inplace=True, axis='columns')
    la_frame = la_frame.iloc[:, 0:11]
    repper = la_frame * 0
    la_frame_whole = pd.read_csv('stats\\league_splits_' + str(yr) + '.csv')
    
    #LVL PLAY BY PLAY BATTER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = lvl_frame.merge(lvl_stats_pit, on = 'pitcher')
    use_bat_groups = use_bat_merge.groupby(['batter'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['batter'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='batter')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(lvl_rep_bat, on = 'batter')
    
    if rd == 1:
        out_file = 'stats\\lvl_adj1_bat_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\lvl_adj2_bat_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')

    #LVR PLAY BY PLAY BATTER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = lvr_frame.merge(lvr_stats_pit, on = 'pitcher')
    use_bat_groups = use_bat_merge.groupby(['batter'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['batter'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='batter')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(lvr_rep_bat, on = 'batter')

    if rd == 1:
        out_file = 'stats\\lvr_adj1_bat_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\lvr_adj2_bat_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')

    #RVL PLAY BY PLAY BATTER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = rvl_frame.merge(rvl_stats_pit, on = 'pitcher')
    use_bat_groups = use_bat_merge.groupby(['batter'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['batter'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='batter')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(rvl_rep_bat, on = 'batter')

    if rd == 1:
        out_file = 'stats\\rvl_adj1_bat_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\rvl_adj2_bat_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
    #RVR PLAY BY PLAY BATTER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = rvr_frame.merge(rvr_stats_pit, on = 'pitcher')
    use_bat_groups = use_bat_merge.groupby(['batter'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['batter'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='batter')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']

            
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(rvr_rep_bat, on = 'batter')

    if rd == 1:
        out_file = 'stats\\rvr_adj1_bat_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\rvr_adj2_bat_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['batter', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
    #LVL PLAY BY PLAY PITCHER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = lvl_frame.merge(lvl_stats_bat, on = 'batter')
    use_bat_groups = use_bat_merge.groupby(['pitcher'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['pitcher'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='pitcher')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(lvl_rep_pit, on = 'pitcher')

    if rd == 1:
        out_file = 'stats\\lvl_adj1_pit_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\lvl_adj2_pit_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
    #LVR PLAY BY PLAY PITCHER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = lvr_frame.merge(lvr_stats_bat, on = 'batter')
    use_bat_groups = use_bat_merge.groupby(['pitcher'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['pitcher'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='pitcher')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(lvr_rep_pit, on = 'pitcher')

    if rd == 1:
        out_file = 'stats\\lvr_adj1_pit_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\lvr_adj2_pit_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
    #RVL PLAY BY PLAY PITCHER, MERGE WITH PITCHER SEASON STATS
    
    use_bat_merge = rvl_frame.merge(rvl_stats_bat, on = 'batter')
    use_bat_groups = use_bat_merge.groupby(['pitcher'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['pitcher'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='pitcher')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(rvl_rep_pit, on = 'pitcher')

    if rd == 1:
        out_file = 'stats\\rvl_adj1_pit_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\rvl_adj2_pit_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
                    
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat
                
    wx_slice_sel = use_stat_xy[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
            
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')
    
    #RVR PLAY BY PLAY PITCHER, MERGE WITH PITCHER SEASON STATS

    use_bat_merge = rvr_frame.merge(rvr_stats_bat, on = 'batter')
    use_bat_groups = use_bat_merge.groupby(['pitcher'])[at_list].agg('mean')
    use_bat_groups = use_bat_groups.reset_index()
    use_bat_count = use_bat_merge.groupby(['pitcher'])['counter'].agg('sum')
    use_bat_join = use_bat_groups.merge(use_bat_count, on='pitcher')
    
    a = 0
    g = len(use_bat_join)
    
    for a in range(g):
        
        ct = use_bat_join.loc[a, 'counter']
        pid = use_bat_join.loc[a, 'pitcher']
        if pid == 489446:
            print(use_bat_join.loc[a, 'double':'walk'])
        if (ct < thresh):
            s_line = use_bat_join.loc[a, 'double':'walk']
            actual_count = ct
            replac_count = thresh - actual_count
            actual_pct = actual_count / thresh
            replac_pct = 1 - actual_pct
            
            actual_portion = s_line * actual_pct
            replac_portion = repper * replac_pct

            
            comb_portion = actual_portion + replac_portion
            comb_portion = comb_portion.reset_index()
            for b in at_list:
                use_bat_join.loc[a, b] = float(comb_portion[b])

    use_stat_xy = use_bat_join.merge(rvr_rep_pit, on = 'pitcher')

    if rd == 1:
        out_file = 'stats\\rvr_adj1_pit_' + str(yr) + '.csv'
    else:
        out_file = 'stats\\rvr_adj2_pit_' + str(yr) + '.csv'
    
    for a in at_list:
                    
        stat_x = str(a) + '_x'
        stat_y = str(a) + '_y'
        comb_stat = use_stat_xy[stat_y] * (1 - use_stat_xy[stat_x])
                    
        use_stat_xy[a] = comb_stat

                
    wx_slice_sel = use_stat_xy[['pitcher', 'double', 'fly_ball', 'ground_ball', 'hr', 'line_drive', 
                                           'popup', 'sacb', 'single', 'strikeout', 'triple', 'walk', 'pa', 'player_type']]
    
    print(out_file)
    wx_slice_sel.to_csv(out_file, index=False, header=True, encoding='utf-8-sig')


def adj_ladder(pid, typ, spl, yr):
    
    print(pid)
    
    if typ == 'P':

        raw_file_sum = pd.read_csv('stats\\' + str(spl) + '_pit_sum_' + str(yr) + '.csv')
        raw_file_avg = pd.read_csv('stats\\' + str(spl) + '_pit_avg_' + str(yr) + '.csv')
        raw_file_vla = pd.read_csv('stats\\' + str(spl) + '_vla_pit_' + str(yr) + '.csv')
        raw_file_pk = pd.read_csv('stats\\' + str(spl) + '_pkwnvla_pit_' + str(yr) + '.csv')
        raw_file_ha = pd.read_csv('stats\\' + str(spl) + '_hapkwnvla_pit_' + str(yr) + '.csv')
        raw_file_ad1 = pd.read_csv('stats\\' + str(spl) + '_adj1_pit_' + str(yr) + '.csv')
        raw_file_ad2 = pd.read_csv('stats\\' + str(spl) + '_adj2_pit_' + str(yr) + '.csv')

        raw_vla_slice = raw_file_vla[raw_file_vla['pitcher'] == pid]['hr']
        raw_pk_slice = raw_file_pk[raw_file_pk['pitcher'] == pid]['hr']
        raw_ha_slice = raw_file_ha[raw_file_ha['pitcher'] == pid]['hr']
        raw_ad1_slice = raw_file_ad1[raw_file_ad1['pitcher'] == pid]['hr']
        raw_ad2_slice = raw_file_ad2[raw_file_ad2['pitcher'] == pid]['hr']
    
    
    print(raw_vla_slice)
    print(raw_pk_slice)
    print(raw_ha_slice)
    print(raw_ad1_slice)
    print(raw_ad2_slice)
    
def adj_loop(yr, no):
    
    print("Adjustment Round 1")
    opp_adjust(yr, 1)
    
    if no == 2:
        print("Adjustment Round 2")
        opp_adjust(yr, 2)
        
    if no >= 3:
        
        a = 1
        g = no - 1
        
        while a <= g:
            
            print("Adjustment Round " + str(a + 1))
            opp_adjust(yr, (a + 1))
            a += 1
            
def adj_do_all(yr):
    
    home_adv_calcu(yr)
    ha_neutralize(yr)
    adj_loop(yr, 2)
    
def savant_batters_do(yr):
    
    sv_tmp = pd.read_csv('savant\\savant_wx_' + str(yr) + '.csv')
    sv_group = sv_tmp.groupby(['batter', 'player_name'])['counter'].agg('count')
    sv_group = sv_group.reset_index()
    sv_group = sv_group[['batter', 'player_name']]
    sv_group.rename(columns={'batter':'player_id'}, inplace=True)
    sv_group.to_csv('savant\\savant_batters_' + str(yr) + '.csv', index=False, header=True, encoding='utf-8-sig')
    
