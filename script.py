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
    info_writer.writerow(['policy_id', 'article_title', 'url', 'journal_name'])
    
for journal in journals:
    # Get all article resource_ids when they're in a particular journal
    resource_ids_query = """select policy_id, article_title.text_value, h.handle from metadatavalue as journal
                            left join metadatavalue as article_title on article_title.dspace_object_id = journal.dspace_object_id AND article_title.metadata_field_id = 64
                            left join item2bundle as i2b on i2b.item_id = article_title.dspace_object_id
                            left join bundle2bitstream as b2bi on i2b.bundle_id = b2bi.bundle_id
                            left join bitstream as bi on bi.uuid = b2bi.bitstream_id AND bi.bitstream_format_id != 6
                            left join resourcepolicy as rp on rp.dspace_object = bi.uuid
                            left join handle as h on h.resource_id = article_title.dspace_object_id 
                            left join metadatavalue as file_name on file_name.dspace_object_id = bi.uuid AND file_name.metadata_field_id = 64
                            left join metadatavalue as file_generator on file_generator.dspace_object_id = bi.uuid AND file_generator.metadata_field_id = 55
                            where journal.text_value = '{0}' 
                            and h.handle is not null 
                            and rp.policy_id is not null
                            and file_name.text_value NOT ILIKE 'file%.txt' 
                            and file_generator.text_value NOT ILIKE 'Written by %'""".format(journal)
    mdv = pd.read_sql_query(resource_ids_query, con=conn)
    mdv['text_value'] = mdv['text_value'].str.encode('utf-8')
    
    # If there's no articles for this journal remove it
    if len(mdv) == 0:
        print('no articles for ' + str(str(journal).encode('utf-8')))
        continue
   
    # Append the info to the csv
    mdv['journal'] = journal
    mdv['handle'] = 'http://opus.sanbi.org/handle/' + mdv['handle'].astype(str)
    mdv.to_csv('tobedeleted.csv', mode='a', header=False, index=False)
    
    # Delete the relevant authorisation policy. 
    delete_policy_ids_str = ', '.join(mdv['policy_id'].astype('str'))
    delete_query = """delete from resourcepolicy where policy_id = ANY(ARRAY[{0}])""".format(delete_policy_ids_str)
    cursor.execute(delete_query)
    conn.commit()
    deletions_count = cursor.rowcount
    
    print('DELETION COUNT - {0} - {1}'.format(journal, deletions_count))
    
cursor.close()