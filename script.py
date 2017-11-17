import psycopg2
import sys
import pandas as pd
import csv
import config

# Connect to the database
conn_string = config.conn_string
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Load of the journals where the articles need to be embargoed
with open('journals.txt') as f:
    journals = f.readlines()
journals = [x.strip() for x in journals]

# This is a csv of a list of all articles which should be embargoed. We write the header here, and further down write each time we do an authorisation policy deletion. 
with open('tobedeleted.csv', 'w') as csv_file:
    info_writer = csv.writer(csv_file)
    info_writer.writerow(['journal_name', 'article_title', 'article_handle', 'file_name', 'file_resource_id', 'policy_id'])
    
for journal in journals:
    # Get all article resource_ids when they're in a particular journal
    resource_ids_query = """select m2.resource_id, m2.text_value, h.handle from metadatavalue as m1 
                         left join metadatavalue as m2 on m2.resource_id = m1.resource_id 
                         left join handle as h on h.resource_id = m2.resource_id 
                         where m1.text_value = '{0}' and h.handle is not null 
                         and m2.metadata_field_id = 64 and m2.resource_type_id = 2""".format(journal)
    mdv = pd.read_sql_query(resource_ids_query, con=conn)
    mdv['text_value'] = mdv['text_value'].str.encode('utf-8')
    
    # If there's no articles for this journal, wtf... remove it? 
    if len(mdv) == 0:
        print('no articles for ' + str(str(journal).encode('utf-8')))
        continue

    # Search for all bundles which have an item to do with the articles. format 2 is licenses it seems, so exclude them. format 6 is text so might need to exclude those too?
    resource_ids_str = ', '.join(mdv['resource_id'].astype('str'))
    bitstream_ids_query = """select bi.bitstream_id, i2b.item_id, i2b.bundle_id from item2bundle as i2b
            left join bundle2bitstream as b2bi on i2b.bundle_id = b2bi.bundle_id
            left join bitstream as bi on bi.bitstream_id = b2bi.bitstream_id
            where i2b.item_id = ANY(ARRAY[{0}]) and bi.bitstream_format_id != 2""".format(resource_ids_str)
    bitstreams = pd.read_sql_query(bitstream_ids_query, con=conn)
    
    # Weed out files from the metadata table have 'Written by FormatFilter' in metadata_field_id = 55, 
    bitstream_resource_ids_str = ', '.join(bitstreams['bitstream_id'].astype('str'))
    metadata_query = """select m1.resource_id as mrida, m2.resource_id as mridb, m2.text_value as metadata, m1.text_value as file_name from metadatavalue as m1
                        left join metadatavalue as m2 on m2.resource_id = m1.resource_id and m2.metadata_field_id = 55
                        where m1.resource_id = ANY(ARRAY[{0}]) and m1.resource_type_id = 0 and m1.metadata_field_id = 64
                        and m1.text_value NOT ILIKE 'file%.txt' 
                        and (m2.text_value NOT ILIKE 'Written by FormatFilter%' OR m2.text_value IS NULL)""".format(bitstream_resource_ids_str)
    mdva = pd.read_sql_query(metadata_query, con=conn)
    mdva['metadata'] = mdva['metadata'].str.encode('utf-8')
    mdva['file_name'] = mdva['file_name'].str.encode('utf-8')
    
    # If the journal just has txt files (which we're not bothering to remove policies for), it won't have any rel policies so remove it
    if len(mdva) == 0:
        print('only txt files for ' + str(str(journal).encode('utf-8')))
        continue
        
    # Get the relevant resource policies
    final_resource_ids_str = ', '.join(mdva['mrida'].astype('str'))
    policy_query = """select * from resourcepolicy where resource_id = ANY(ARRAY[{0}]) and resource_type_id = 0""".format(final_resource_ids_str)
    policies = pd.read_sql_query(policy_query, con=conn)
    
    # If there aren't any relevant policies, continue
    if len(policies) == 0:
        print('no policies for ' + str(str(journal).encode('utf-8')))
        continue
        
    # Join all the tables together for error checking. 
    merged = mdv.merge(bitstreams[['item_id', 'bundle_id', 'bitstream_id']], left_on='resource_id', right_on='item_id', how='outer')
    merged = merged.merge(mdva, left_on='bitstream_id', right_on='mrida', how='outer')
    merged = merged.merge(policies[['policy_id', 'resource_id']], left_on='mrida', right_on='resource_id', how='right')
    merged['journal'] = journal
        
    # Append the info to the csv
    columns = ['journal', 'text_value', 'handle', 'file_name', 'mrida', 'policy_id']
    merged[columns].to_csv('tobedeleted.csv', mode='a', header=False, index=False)
    
    if len(mdv) > len(policies):
        print('more articles than policies, is this right?')
    
    # Delete the relevant authorisation policy. 
    delete_policy_ids_str = ', '.join(merged['policy_id'].astype('str'))
    delete_query = """delete from resourcepolicy where policy_id = ANY(ARRAY[{0}])""".format(delete_policy_ids_str)
    
    cursor.execute(delete_query)
    conn.commit()
    deletions_count = cursor.rowcount
    
    print('DELETION COUNT - {0} - {1}'.format(journal, deletions_count))
    
cursor.close()
import pdb; pdb.set_trace()