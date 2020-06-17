# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 14:20:09 2019

@author: thsiao17

This script takes in 2 arguments: The metadata label for the y-axis and the metadata label for the x-axis.
Please enter the labels exactly as they appear in the database (they are case sensitive).
For example:
    y_axis_input = "tissue"
    x_axis_input = "POD" #POD stands for Post-operation date
    
    This will find all the clones that belong to each tissue/POD.
    The file will be saved whereever path points to.
    
It's recommended you run this script on a machine with at least 32gb of RAM, to avoid potentially running into memory errors (especially for very large datasets). 
You can still try with less than 32gb of RAM.'
"""

import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
import math
import random
import re

# =============================================================================
# You must fill out the information below before running the script. 
# =============================================================================
###These are the metadata labels used for the graph.

"""
Example:
    for the influenza database, which has several time points but no tissues (only blood, which is not in the metadata):
        y_axis_input = "None"
        x_axis_input = "timepoint"
    This will result in a graph that has only 1 y-axis value (blood), and many time point values on the x-axis.
    
Example:
    for the lp15 database, which has several tissues but no time points:
        y_axis_input = "tissue"
        x_axis_input = "None"
    This will result in a graph that has all of the tissues listed on the y-axis, and only a single x-axis column to display the circles. 
    
If you want to see which metadata labels are available for your dataset, do the following:
    1. Fill out the database connection info.
    2. Uncomment lines 143-149.
    3. Run lines 97-149.
           
"""

###Leave y_axis_input as "None" if you do not want to use a second metadata label. Otherwise, replace it with the label of your choice.
y_axis_input = "None" ###if using tissue/sample_origin, please add it here

###Leave x_axis_input as "None" if you do not want to use a second metadata label. Otherwise, replace it with the label of your choice.
x_axis_input = "None" ###If using timepoint/POD, please put it here and not in y_axis_input in order for the script to work properly

###path on your computer to save the file. You shouldn't remove the "r", as it helps avoid errors due to spaces. Make sure to include the filename at the end, such as clone_distribution_chart.html
###For example, path = r"/home/user/JohnSmith/Desktop/Graphs/clone_distribution_chart.html"
path = r"enter_path_here/clone_distribution_chart.html" 

###The dictionary below (tissue_color_dict) is optional. If you want to use it, follow the instructions below. If you don't want to use it, leave it empty but do not comment it out.
###The dictionary is for the y-axis labels. It specifies the order and color of the y-axis labels in the graph.
###Change the dictionary if you want a different order or color of your labels. 
###If your y-axis labels are not present in the dictionary, add them in the order you want them to appear along with the color you want them to be. Also remove any labels not relevant to your dataset or leave them commented out.
###Also, make sure your labels are not commented (meaning they do have "#" in front of them). If you want to use the labels below, simply remove the # in front of each one.
tissue_color_dict = {
                # 'PBMC' : '#67001f',
                # 'PBL' : '#67001f',
                # 'Bone Marrow' : '#b2182b',
                # 'Spleen' : '#d6604d',
                # 'SPL' : '#d6604d',
                # 'Lung' : '#f4a582',
                # 'MLN' : '#515151',
                # 'ILE' : '#515151',
                # 'ILN' : '#515151',
                # "AxLN" : "#878787",
                # 'Stomach' : 'gold',
                # 'Stomach_allograft' : 'gold',
                # 'Duodenum' : '#a1daf7',
                # 'Duodenum_allograft' : '#a1daf7',
                # 'Jejunum' : '#92c5de',
                # 'Jejunum_allograft' : '#92c5de',
                # 'Ileum' : '#4393c3',                   
                # 'Ileum_allograft' : '#4393c3',
                # 'Cecum_allograft' : '#063d75',
                # 'Colon_allograft' : '#2166ac',
                # 'Colon' : '#2166ac',
                # 'Bowel': 'teal', 
                }
 
###Below are the parameters for connecting to the database.
# ssh variables. If you do not need ssh you can proceed directly to line 104 and comment/remove lines 110-116.
host = 'add_database_ip_here_within_the_quotes'
localhost = '127.0.0.1'
ssh_username = 'username' ###username for ssh connection
ssh_password = 'password' ###password for ssh connection
port = 22 ###default ssh port is 22, yours may be different
#ssh_private_key = '/path/to/key.pem'

# database variables
user='username'
password='password'
database='database'
localhost = '127.0.0.1'

server = SSHTunnelForwarder(
    (host, port),
    ssh_username=ssh_username,
    ssh_password=ssh_password,
    remote_bind_address=(localhost, 3306) ###default port is 3306, yours may be different
    )
server.start()

local_port = str(server.local_bind_port)
connect_string = 'mysql+pymysql://{}:{}@{}/{}'.format(user, password, localhost, database)
sql_engine = create_engine(connect_string)

# =============================================================================
# After filling out the information above, you can now run the entire script.
# =============================================================================

# =============================================================================
# Download MySQL tables as pandas dataframes
# =============================================================================
###sample_metadata table
print("Loading sample_metadata table.")
query = "select * from sample_metadata"
metadata_table = pd.read_sql_query(query, sql_engine)

print("Loading samples table.")
###samples table
query = "select * from samples"
samples_table = pd.read_sql_query(query, sql_engine)

print("Loading subjects table.")
###subjects table
query = "select * from subjects"
subjects_table = pd.read_sql_query(query, sql_engine)

# =============================================================================
# If you want to get a list of the metadata labels, enter the database information, load the metadata_table and run the command 
# Make sure to uncomment the function below before running it (meaning remove all #)
# =============================================================================
# def get_metadata_labels(metadata_table):
#     return print(metadata_table['key'].unique().tolist())
# get_metadata_labels(metadata_table)    

def make_clone_distribution_chart(subjects_table, samples_table, metadata_table, y_axis_input, x_axis_input, path, tissue_color_dict):
    
    ###Make dictionary of subjects
    subjects_dict = {}
    for subject_id, subject_name in zip(subjects_table['id'], subjects_table['identifier']):
        subjects_dict[subject_id] = subject_name

    ###make a list with all tissues to be used for shared yaxis and build a tissue_color_dict that associates a tissue with a color
    if len(tissue_color_dict) == 0:
        if 'tissue' in metadata_table['key'] and (y_axis_input == 'tissue' or y_axis_input == 'sample_origin'):
            total_tissue_list = metadata_table['value'].loc[metadata_table['key'] == 'tissue'].unique().tolist()
        elif 'sample_origin' in metadata_table['key'] and (y_axis_input == 'tissue' or y_axis_input == 'sample_origin'):
            total_tissue_list = metadata_table['value'].loc[metadata_table['key'] == 'sample_origin'].unique().tolist()
        else:
            total_tissue_list = metadata_table['value'].loc[metadata_table['key'] == y_axis_input].unique().tolist()           
        tissue_color_dict = {}
        for tissue in total_tissue_list:
            tissue_color_dict[tissue] = "#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
    else:
        total_tissue_list = list(tissue_color_dict.keys())
        
    ###reverse the list so the gut tissues are toward the bottom of the y-axis (only relevant if tissue_color_dict is specified with blood first and gut last)
    if len(tissue_color_dict) > 0:
        total_tissue_list = total_tissue_list[::-1]

    ###get the samples that belong to each subject
    samples_dict = {}
    for subject_id in subjects_dict.keys():
        samples_dict[subject_id] = set(samples_table['id'].loc[samples_table['subject_id'] == subject_id])
    
    if len(subjects_dict) < 4:
        ###find total number of rows for subplots
        num_rows = 1 ###math.ceil rounds up, so that there will be enough subplot spaces if number of subjects is odd
        num_cols = 3
        
        fig = make_subplots(rows=num_rows, cols=num_cols, subplot_titles=["temp" for x in range(len(subjects_dict))], shared_yaxes=True, vertical_spacing=0.031, horizontal_spacing=0.01, 
           )
    else:                                       
        ###find total number of rows for subplots
        num_rows = math.ceil(len(subjects_dict)/2) ###math.ceil rounds up, so that there will be enough subplot spaces if number of subjects is odd
        num_cols = round(len(subjects_dict)/2)
        
        fig = make_subplots(rows=num_rows, cols=num_cols, subplot_titles=["temp" for x in range(len(subjects_dict))], shared_yaxes=True, vertical_spacing=0.031, horizontal_spacing=0.01, 
                            ) 
    # =============================================================================
    # Now build the subplots for each subject  
    # =============================================================================
    row_counter = 1
    col_counter = 1
    tissue_legend = []
    num_clones_dict = {}
    subplot_titles = []
    for subject_id in subjects_dict.keys():
    # for subject_id in [1]:
        print("Making graph for: " + subjects_dict[subject_id])
        
        ###sequences table
        print("Loading sequences table for {}. This can take several minutes depending on the table size and download speed.".format(subjects_dict[subject_id]))
        query = "select * from sequences where clone_id IS NOT NULL and sample_id IN{}".format(str(samples_dict[subject_id]).replace('{', '(').replace('}', ')'))
        sequences_table = pd.read_sql_query(query, sql_engine)
        print("Finished loading table. Now making graph.")
        # print(len(sequences_table))
        
        ###get the number of clones for current subject
        num_clones_dict[subject_id] = len(sequences_table['clone_id'].loc[sequences_table['sample_id'].isin(samples_dict[subject_id])].unique())

        ###make the titles of the subplots
        subplot_titles.append("<b>" + subjects_dict[subject_id] + "</b><br>" + str(f'{num_clones_dict[subject_id]:,}') + " clones") 
                       
        ###filter the sequences table for the current patient
        df = sequences_table.loc[sequences_table['sample_id'].isin(samples_dict[subject_id])]
        subject_specific_metadata = metadata_table.loc[metadata_table['sample_id'].isin(samples_dict[subject_id])]
        ###get subject-specific metadata labels
        if x_axis_input == 'pod': 
            x_axis_values = [int(re.sub('\D', '', x.split(" ")[0])) for x in subject_specific_metadata['value'].loc[subject_specific_metadata['key'] == x_axis_input].unique()]
            x_axis_values_unedited = [x for x in subject_specific_metadata['value'].loc[subject_specific_metadata['key'] == x_axis_input].unique()]
            
            ###now sort both of the lists based on the integer version
            x_axis_values_sorted = [x for x, y in sorted(zip(x_axis_values, x_axis_values_unedited), key=lambda pair: pair[0])]
            x_axis_values_unedited_sorted = [y for x, y in sorted(zip(x_axis_values, x_axis_values_unedited), key=lambda pair: pair[0])]
        elif x_axis_input == 'timepoint': ###such as with the Influenza dataset
            convert_hours_to_days = []
            x_axis_values_unedited = []
            for x in subject_specific_metadata['value'].loc[subject_specific_metadata['key'] == x_axis_input].unique().tolist():
                if "h" in x:
                    convert_hours_to_days.append(float(x.replace("h", "")) / 24)
                elif "d" in x:
                    convert_hours_to_days.append(float(x.replace("d", "")))
                x_axis_values_unedited.append(x)               
            x_axis_values = convert_hours_to_days
            
            ###now sort both of the lists based on the numberical version
            x_axis_values_sorted = [x for x, y in sorted(zip(x_axis_values, x_axis_values_unedited), key=lambda pair: pair[0])]
            x_axis_values_unedited_sorted =[y for x, y in sorted(zip(x_axis_values, x_axis_values_unedited), key=lambda pair: pair[0])]            
        elif x_axis_input == "None":
            x_axis_values = [""]
            x_axis_values_sorted = x_axis_values
            x_axis_values_unedited_sorted = x_axis_values             
        else:
            x_axis_values = subject_specific_metadata['value'].loc[subject_specific_metadata['key'] == x_axis_input].unique().tolist()
            x_axis_values_sorted = x_axis_values
            x_axis_values_unedited_sorted = x_axis_values     
        # =============================================================================
        # Build the dataframe for graph
        # =============================================================================
        num_clones = []
        x_axis_list = []
        y_axis_list = []
        num_samples = []  
        if x_axis_input != 'None':
            for x_value in x_axis_values_sorted:
                ###get the sample_ids that belong to this x_value
                if x_axis_input == 'pod' or x_axis_input == 'timepoint':
                    x_current_sample_ids = subject_specific_metadata['sample_id'].loc[subject_specific_metadata['value'] == x_axis_values_unedited_sorted[x_axis_values_sorted.index(x_value)]].unique().tolist()
                else:
                    x_current_sample_ids = subject_specific_metadata['sample_id'].loc[subject_specific_metadata['value'] == x_value].unique().tolist()                          
                if len(total_tissue_list) > 0:
                    for y_value in total_tissue_list:
                        ###get sample_ids for current y_value
                        y_current_sample_ids = subject_specific_metadata['sample_id'].loc[subject_specific_metadata['value'] == y_value].unique().tolist()    
                        
                        sample_ids_in_both = set(x_current_sample_ids).intersection(set(y_current_sample_ids))
                        clones_in_both = set(df['clone_id'].loc[df['sample_id'].isin(sample_ids_in_both)].unique())
                        if len(df['clone_id'].unique()) > 0:
                            num_clones.append(len(clones_in_both)/len(df['clone_id'].unique()) * 100) 
                        else:
                            num_clones.append(0)
                        y_axis_list.append(y_value)
                        x_axis_list.append(x_value)
                        num_samples.append(len(set(x_current_sample_ids).union(set(y_current_sample_ids))))
                else:
                        y_axis_list.append("")
                        x_axis_list.append(x_value)      
                        num_samples.append(len(set(x_current_sample_ids)))
                        
                        sample_ids_in_both = x_current_sample_ids
                        clones_in_both = set(df['clone_id'].loc[df['sample_id'].isin(sample_ids_in_both)].unique())
                        if len(df['clone_id'].unique()) > 0:
                            num_clones.append(len(clones_in_both)/len(df['clone_id'].unique()) * 100) 
                        else:
                            num_clones.append(0)
        else:
            for x_value in x_axis_values_sorted:
                x_current_sample_ids = []                          
                for y_value in total_tissue_list:
                    ###get sample_ids for current y_value
                    y_current_sample_ids = subject_specific_metadata['sample_id'].loc[subject_specific_metadata['value'] == y_value].unique().tolist()    
                    
                    sample_ids_in_both = y_current_sample_ids
                    clones_in_both = set(df['clone_id'].loc[df['sample_id'].isin(sample_ids_in_both)].unique())
                    if len(df['clone_id'].unique()) > 0:
                        num_clones.append(len(clones_in_both)/len(df['clone_id'].unique()) * 100) 
                    else:
                        num_clones.append(0)
                    y_axis_list.append(y_value)
                    x_axis_list.append(x_value)
                    num_samples.append(len(set(y_current_sample_ids)))
                                        
        distribution_chart = pd.DataFrame()
        distribution_chart['x_axis'] = x_axis_list
        distribution_chart['num_clones'] = num_clones
        distribution_chart['y_axis'] = y_axis_list       
        distribution_chart['num_samples'] = num_samples         
        # =============================================================================
        # Make lists for each x_value. This list represents the y-axis values for a given x-axis value    
        # =============================================================================
        x_axis_counter = 1
        for x_value in x_axis_values_sorted:
            ###get the size of the circles for current x_value
            size = list(distribution_chart['num_clones'].loc[distribution_chart['x_axis'] == x_value])
            ###get the number of samples that belong to each circle
            samples_text_list = list(distribution_chart['num_samples'].loc[distribution_chart['x_axis'] == x_value])
            
            # =============================================================================
            # Check if dataset has allografted tissues and if so, find the indeces of the allograft tissues        
            # =============================================================================
            allograf_indeces = []
            if any('allograft' in tissue for tissue in total_tissue_list):

                for tissue in total_tissue_list:  
                    if 'allograft' in tissue:
                        allograf_indeces.append(total_tissue_list.index(tissue))
            
            ###go through the list of number of clones per y_axis label and get indeces of the y_axis labels that have at least 1 clone
            ###the result of this is to only add circles for y-axis labels that have clones
            y_list = [size.index(x) + 1 for x in size if x != 0]
        
            ###make a list that adds a circle outline to the allograft tissues
            allograft_line = []
            for x in y_list:              
                if x - 1 in allograf_indeces:                   
                    allograft_line.append(2)                   
                else:                   
                    allograft_line.append(0)
            
            ###Build x-axis values, which is the same value repeated for the length of the number of y_axis values
            ###For example, if y-axis is [2, 3, 6, 8], then we need to add this all to the same x-axis value so the x-axis would be [1, 1, 1, 1] if we are currenly on the first x-axis value
            x_list = [x_axis_counter for x in range(len(y_list))]
                       
            ###make a list of the current y-axis labels being plotted this iteration
            if len(total_tissue_list) > 0:
                current_y_labels = [total_tissue_list[x-1] for x in y_list]
            else:
                current_y_labels = [""]
                               
            ###get the y-labels' colors
            if len(total_tissue_list) > 0:
                color_list = [tissue_color_dict[x] for x in current_y_labels]
            else:
                color_list = ['black']

            #make lists match the length of y_list
            size = [x for x in size if x != 0]
            samples_text_list = [x for x in samples_text_list if x != 0]
            
            # =============================================================================
            # Add the traces to the graph            
            # =============================================================================
            fig.add_trace(go.Scatter(
                x=x_list, y=y_list,
                text=['Clones: ' + str(round(size[x], 1))    + "%" +
                      '<br>Samples: ' + str(samples_text_list[x])
                      for x in range(len(size))],
                mode='markers',
                showlegend=False,
                marker=dict(
                    size=size,
                    sizemin=4,
                    color=color_list,
                    line=dict(
                        color=['orchid' for x in range(len(allograft_line))],
                        width=allograft_line),          
                    opacity=1,
                            )
            ),row=row_counter, col=col_counter)
        
            x_axis_counter += 1
            
        ###fix x-axis values from the numerical value to its corresponding label
        if x_axis_input == 'timepoint':                       
            x_tick_text = ["<b>" + str(x) + "<b>" for x in x_axis_values_unedited_sorted]    
            
            fig.update_xaxes(
                ticktext=x_tick_text,
                tickvals=[x for x in range(1, len(x_axis_values_sorted) + 1)],
                tickangle=90,
                row=row_counter, col=col_counter
            )
        else:                       
            x_tick_text = ["<b>" + str(x) + "<b>" for x in x_axis_values_sorted]    
            
            fig.update_xaxes(
                ticktext=x_tick_text,
                tickvals=[x for x in range(1, len(x_axis_values_sorted) + 1)],
                tickangle=90,
                row=row_counter, col=col_counter
            )
                              
        ###add this subject's tissues to tissue_legend list      
        temp = total_tissue_list
        for tissue in temp:           
            if tissue not in tissue_legend:               
                tissue_legend.append(tissue)
        
        ###change y-axis values from the numerical value to its corresponding label
        fig.update_yaxes(
            ticktext=temp,
            tickfont=dict(
                 family="Arial",
                 color="black"
                ),
            tickvals=[x for x in range(1, len(total_tissue_list) + 1)],
            row=row_counter, col=col_counter
                    )
                   
        x_axis_counter = 1
        col_counter += 1      
        if col_counter > num_cols:         
            row_counter += 1
            col_counter = 1
 
    ###add allograft outline legend
    if any('allograft' in tissue for tissue in total_tissue_list):
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode='markers',
            showlegend=True,
            name="allografted tissue",
            marker=dict(
                size=20,
                color='white',
                line=dict(
                    color='orchid',
                    width=2),
                opacity=1,    
            )
        ),row=1, col=1)
        
    ###add tissue legend
    tissue_legend_sorted = [x for x in tissue_color_dict.keys() if x in tissue_legend]
    for tissue in tissue_legend_sorted:
        
        if 'allograft' not in tissue:
            
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode='markers',
                showlegend=True,
                name=tissue,
                marker=dict(
                    size=20,
                    color=tissue_color_dict[tissue],         
                    opacity=1,    
                )
            ),row=1, col=1)

        else:
        
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode='markers',
                showlegend=True,
                name=tissue,
                marker=dict(
                    size=20,
                    color=tissue_color_dict[tissue],         
                    opacity=1,
                    line=dict(
                        color="Orchid",
                        width=2,
                        ),
                )
            ),row=1, col=1)
                
    ###Update subplot titles
    for i, subplot_title in zip(fig['layout']['annotations'], subplot_titles):
            i['text'] = subplot_title
            i['font'] = dict(size=30, color='black', family="Arial")

    fig.update_layout(plot_bgcolor='whitesmoke', title='Clone Distribution Chart for: ' + str(database),
                       height=1300 * num_rows / 2,
                       width=1200 * num_cols / 3,
                      font=dict(
                                family="Courier New, monospace",
                                size=20,
                                color="black"
                               ),
                      margin=dict(
                            # l=50,
                            # r=50,
                            # b=100,
                            t=180,
                            # pad=4
                        ),
    )    
    pio.write_html(fig, file=path)   
    print("Finished!")
    print("You can find the graph at: " + path)
    
make_clone_distribution_chart(subjects_table, samples_table, metadata_table, y_axis_input, x_axis_input, path, tissue_color_dict)

