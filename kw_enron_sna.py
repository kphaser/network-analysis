# Individual Assignment 3 - SNA Enron
# Predict 452 Winter 2017
# By: Kevin Wong

# prepare for Python version 3x features and functions
from __future__ import division, print_function

# load package into the namespace for this program
from pymongo import MongoClient  # work with MongoDB 
import pandas as pd  # DataFrame object work
from datetime import datetime  # text/date manipulation
import matplotlib.pyplot as plt # plotting
import networkx as nx # network visualization

##############################################
# Connect to NUIT SSCC Mongo DB server
##############################################
print('First connect to the Northwestern VPN\n')

# prompt for user's NetID and password
my_netid = raw_input('Enter your NetID: ')
my_password = raw_input('Enter your password: ')

try:    
    client = MongoClient("129.105.208.225")
    client.enron.authenticate(my_netid, my_password,\
        source='$external', mechanism='PLAIN') 
    print('\nConnected to MongoDB enron database\n')    
    success = True    
except:
    print('\nUnable to connect to the enron database')

# if connection is successful, work with the database

###############################################
# Querying the MongoDB database
###############################################

print('\nCollections in the enron database:')
cols = client.enron.collection_names()
for col in cols:
    print(col)	

# work with documents in the messages collection 
workdocs = client.enron.messages

# inquire about the documents in messages collection
print('\nNumber of documents: ', workdocs.count())
print('\nOne such document:\n', workdocs.find_one())
one_dict = workdocs.find_one() # create dictionary
print('\nType of object workdocs: ', type(one_dict))  

# query some keywords to generate emails for
# how many documents contain 'mark-to-market' in text field
print('How many documents contain the string <mark-to-market>?')
print(workdocs.find({'$text':{'$search':'mark-to-market'}}).count())

# how many documents contain 'special purpose entity' in text field
print('How many documents contain the string <special purpose entity>?')
print(workdocs.find({'$text':{'$search':'special purpose entity'}}).count())

# how many documents contain 'revenue recognition' in text field
print('How many documents contain the string <revenue recognition>?')
print(workdocs.find({'$text':{'$search':'revenue recognition'}}).count())

# how many documents contain 'jedi' in text field
print('How many documents contain the string <jedi>?')
print(workdocs.find({'$text':{'$search':'jedi'}}).count())

# how many documents contain 'whitewing' in text field
print('How many documents contain the string <whitewing>?')
print(workdocs.find({'$text':{'$search':'whitewing'}}).count())

# how many documents contain 'raptor' in text field
print('How many documents contain the string <raptor>?')
print(workdocs.find({'$text':{'$search':'raptor'}}).count()) # selected this keyword for further analysis

################################################
# Obtain and clean the data 
################################################

# store documents in a list of dictionary objects
selectdocs =\
    list(workdocs.find({'$text':{'$search':'raptor'}}))
print('\nCreated Python object with raptor documents')
print('\nType of object selectdocs', type(selectdocs))
print('\nNumber of items in selectdocs: ', len(selectdocs))  

# flatten the nested dictionaries in selectdocs
# and remove _id field 
list_of_emails_dict_data = []
for message in selectdocs:
    tmp_message_flattened_parent_dict = message
    tmp_message_flattened_child_dict = message['headers']
    del tmp_message_flattened_parent_dict['headers']
    del tmp_message_flattened_parent_dict['_id']
    tmp_message_flattened_parent_dict.\
        update(tmp_message_flattened_child_dict)
    list_of_emails_dict_data.\
        append(tmp_message_flattened_parent_dict.copy())

print('\nType of object list_of_emails_dict_data',\
    type(list_of_emails_dict_data))
print('\nNumber of items in list_of_emails_dict_data: ',\
    len(list_of_emails_dict_data))  

# we can use Python pandas to explore and analyze these data
# create pandas DataFrame object to begin analysis
enron_email_df = pd.DataFrame(list_of_emails_dict_data)
print('\nType of object enron_email_df', type(enron_email_df))

# set missing data fields
enron_email_df.fillna("", inplace=True)

# user-defined function to create simple date object (no time)
def convert_date_string (date_string):
    try:    
        return(datetime.strptime(str(date_string)[:16].\
            lstrip().rstrip(), '%a, %d %b %Y'))
    except:
        return(None)
        
# apply function to convert string Date to date object
enron_email_df['Date'] = \
    enron_email_df['Date'].apply(lambda d: convert_date_string(d))
    
# date of Enron bankruptcy
BANKRUPTCY = datetime.strptime(str('Sun, 2 Dec 2001'), '%a, %d %b %Y')   
    
print('\nExamine enron_email_df: ', type(enron_email_df))
print('\nNumber of observations:', len(enron_email_df))
print('\nBeginning observations:')
print(enron_email_df.head())


# PARSE dataframe
# split out multiple emails under 'To' column to separate pairs 
# reference: http://stackoverflow.com/questions/12680754/split-pandas-dataframe-string-entry-to-separate-rows
enron_df = enron_email_df[['From','To']]
emails = pd.DataFrame(enron_df.To.str.split(',').tolist(), index=enron_df.From).stack()
emails = emails.reset_index()[['From', 0]]
emails.columns = ['From','To']
# replace unicode escape characters with blank for cleaner email addresses
emails['To'] = emails.To.str.replace('\\r\\n\\t', '')
# sort 'From' column by alphabetical order
emails = emails.sort_values(by='From')
# remove whitespace in the 'To' column
emails['To'] = emails['To'].astype(str)
emails['From'] = emails['From'].astype(str)
emails['To'] = emails['To'].map(str.strip)

# save copy of dataframe to csv file
emails.to_csv("enron_raptor.csv",index=False)

##############################################
# Load saved CSV file for Raptor emails
##############################################

# load the csv file if running at a later session without having to connect to DB
emails = pd.read_csv("enron_raptor.csv")


##############################################
# Data exploration
##############################################

# check value counts -- who sent and who received the most emails regarding 'raptor'
emails['From'].value_counts()
emails['To'].value_counts()

# examine network statistics
nx.degree_centrality(G)
nx.closeness_centrality(G)
nx.betweenness_centrality(G)
nx.eigenvector_centrality(G)


###############################################
# Visualize networks
###############################################
# reference: http://stackoverflow.com/questions/29738288/displaying-networkx-graph-with-labels
# Also see Tom Miller's WNDS Book Ch 6+7

# add nodes 
G = nx.Graph()
G.add_nodes_from(emails.From)
G.nodes()

# Add edges
subset = emails[['To','From']]
tuples = [tuple(x) for x in subset.values] 
G.add_edges_from(tuples)
G.number_of_edges()

# examine degree of each node
print(nx.degree(G))

# plot network and degree distributions
nx.draw(G,node_color='b',node_size=100,with_labels=True)
plt.savefig("enron_baseline_network.pdf")
plt.show()

fig = plt.figure()
plt.hist(nx.degree(G).values())
plt.xlabel('Node Degree')
plt.ylabel('Frequency')
#plt.savefig("node_frequency.pdf")
plt.show()

# adjacency matrix
circle_mat = nx.adjacency_matrix(G, nodelist = G.nodes())
print(circle_mat)
# find the total number of links for this network
print(np.sum(circle_mat))

# ego-centric network on Sara Shackleton
hub_ego=nx.ego_graph(G,G.nodes()[307])
pos=nx.spring_layout(hub_ego)
nx.draw(hub_ego,pos,node_color='b',node_size=100,with_labels=True)
# Draw ego as large and red
nx.draw_networkx_nodes(hub_ego,pos,nodelist=[G.nodes()[307]],node_size=300,node_color='r')
plt.savefig('ego_graph.pdf')
plt.show()

