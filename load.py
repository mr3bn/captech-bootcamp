import pandas as pd
import pymysql

def lambda_handler(event, context):
    # gather bucket data from event object
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    global_path = 'https://s3.amazonaws.com/' + bucket + '/' + key
    
    # read csv as dataframe
    df = pd.read_csv(global_path)
    
    # instantiate connection to db
    endpoint="bootcamp-open.ctzjfvqmre0y.us-east-1.rds.amazonaws.com"
    port=3306
    dbname="test_db"
    user="ct_master"
    password="captechlistens"
    
    conn = pymysql.connect(host=endpoint, 
                           user=user, 
                           port=port,
                           passwd=password,
                           db=dbname)
    print 'connection established'

    col_str = ', '.join(list(df.columns))
    val_str = ', '.join([str(tuple(r[1].values)) \
                       for r in df.iterrows()])
    update_str = ', '.join([x + '=values({})'.format(x) for x in list(df.columns)])

    insert_str = 'insert into current_campaign({}) values {} \
        on duplicate key update {};'.format(
            col_str, val_str, update_str)
    
    print insert_str

    cursor = conn.cursor()
    rows = cursor.execute(insert_str)
    conn.close()
    
    print 'insert_success'

    return rows