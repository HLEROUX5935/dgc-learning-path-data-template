import os
import time
import json
import base64

from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

from google.cloud.workflows.executions_v1 import ExecutionsClient
from google.cloud.workflows.executions_v1.types import Execution
from google.api_core.exceptions import GoogleAPICallError, RetryError
import time


def receive_messages(event: dict, context: dict):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    Inserts a file into the correct BigQuery raw table. If succedded then 
    archive the file and trigger the Cloud Workflow pipeline else move the 
    file to the reject/ subfolder.
    
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    print(f'     ***************                 Start  Receive message   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

    # rename the variable to be more specific and write it to the logs
    pubsub_event = event
    print(f' pubsub_event : {pubsub_event}')
    pubsub_event_data = pubsub_event['data']
    print(type(pubsub_event_data))
    
    # decode the data giving the targeted table name
    table_name = base64.b64decode(pubsub_event['data']).decode('utf-8')

    # get the blob infos from the attributes
    bucket_name = pubsub_event['attributes']['bucket_name']
    blob_path = pubsub_event['attributes']['blob_path']
    
    #     - connect to the Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    leblob = bucket.blob(blob_path)

    load_completed = True

    if leblob.exists():
        load_completed = False
        try:
            # insert the data into the raw table then archive the file
            insert_into_raw(table_name, bucket_name, blob_path)
            move_file(bucket_name, blob_path, 'archive')
            load_completed = True
            
        except Exception as e:
            print(e)
            move_file(bucket_name, blob_path, 'reject')
        
    else:
        print(f'{blob_path} inexistant dans  in {bucket_name} ')

    # trigger the pipeline if the load is completed 
    # même si pas de fichier pour vider la table 
    if load_completed:
        trigger_worflow(table_name)

    print(f'     ***************                 End  Receive message   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')        

def insert_into_raw(table_name: str, bucket_name: str, blob_path: str):
    """
    Insert a file into the correct BigQuery raw table.
    
    Args:
         table_name (str): BigQuery raw table name.
         bucket_name (str): Bucket name of the file.
         blob_path (str): Path of the blob inside the bucket.
    """

    print(f'     ***************              Start  insert_into_raw   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

    # TODO: 2
    # You have to try to insert the file into the correct raw table using the python BigQuery Library. 
    # Please, refer yourself to the documentation and StackOverflow is still your friend ;)
    # As an help, you can follow those instructions:
    #     - connect to the Cloud Storage client
    storage_client = storage.Client()

    #     - get the util bucket object using the os environments
    project = os.environ["GCP_PROJECT"]
    util_bucket_suffix = os.environ['util_bucket_suffix']
    if util_bucket_suffix is None:
        raise ValueError("La variable d'environnement 'util_bucket_suffix' n'est pas définie.")
    bucket_util_name = f'{project}_{util_bucket_suffix}'
    bucket_util = storage_client.bucket(bucket_util_name)
    
    #     - loads the schema of the table as a json (dictionary) from the bucket    
    blob_util = bucket_util.blob(f'raw_{table_name}_json')
    raw_schema_json = json.loads(blob_util.download_as_string())

    #     - store in a string variable the blob uri path of the data to load (gs://your-bucket/your/path/to/data)
    blob_uri_path = f'gs://{bucket_name}/{blob_path}'
    #gs://vast-verve-469412-c5_magasin_cie_landing/input\store_20220531.csv
    bucket = storage_client.bucket(bucket_name)
    leblob = bucket.blob(blob_path)
    if leblob.exists():

        #     - connect to the BigQuery Client
        bigquery_client = bigquery.Client()  

        #     - store in a string variable the table id with the bigquery client. (project_id.dataset_id.table_name)
        table_id = f'{project}.raw.{table_name}'    

        #     - create your LoadJobConfig object from the BigQuery librairy
        #     - (maybe you will need more variables according to the type of the file - csv, json - so it can be good to see the documentation)

        *_, extension = blob_path.split('.')
        if extension.lower() == 'csv':
            load_job_config = bigquery.LoadJobConfig(
                schema=raw_schema_json,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
            )

        elif extension.lower() == 'json':
            load_job_config = bigquery.LoadJobConfig(
                schema=raw_schema_json,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
        else:
            raise NotImplementedError(f'Extension {extension} not supported')

        #run your loading job from the blob uri to the destination raw table
        print(f'         load_table_from_uri  ')
        load_job = bigquery_client.load_table_from_uri(
            source_uris=blob_uri_path,
            destination=table_id,
            job_config=load_job_config,
        )

        #waits the job to finish and print the number of rows inserted
        load_job.result()

        nb_rows_table = bigquery_client.get_table(table_id).num_rows

        print(f'{nb_rows_table} rows in {table_id} ')
    else:
        print(f'{blob_path} inexistant dans  in {bucket_name} ')
    
    print(f'     ***************              End  insert_into_raw   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

def trigger_workflow_for_a_table(project_id, location, workflow_id, arguments=None):
    client = ExecutionsClient()
    parent = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"

    print(f"[INFO] Déclenchement du workflow '{workflow_id}'")
    print(f"[INFO] parent : {parent}")

    try:
        # Crée une exécution
        execution = Execution(argument=arguments if arguments else "{}")
        response = client.create_execution(request={"parent": parent, "execution": execution})
        execution_id = response.name.split("/")[-1]
        print(f"[SUCCESS] Exécution déclenchée. ID : {execution_id}")
        return execution_id
    except GoogleAPICallError as e:
        print(f"[ERROR] Impossible de déclencher le workflow : {e}")
        return None

def wait_for_execution_completion(project_id, location, workflow_id, execution_id, timeout=300):
    client = ExecutionsClient()
    execution_name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}/executions/{execution_id}"

    start_time = time.time()
    while True:
        try:
            response = client.get_execution(request={"name": execution_name})
            state = response.state.name  # ex: "ACTIVE", "SUCCEEDED", "FAILED"
            print(f"[INFO]  statut : {state}  : {datetime.now().strftime("%H:%M:%S.%f")[:-4]} ")

            if state not in ["ACTIVE"]:
                print(f"[INFO] Exécution terminée avec le statut : {state}")
                if state == "SUCCEEDED":
                    print(f"[RESULT] {response.result}")
                elif state == "FAILED":
                    print(f"[ERROR] {response.error}")
                return state

            if time.time() - start_time > timeout:
                print(f"[ERROR] Timeout atteint après {timeout} secondes.")
                return None

            print(f"[INFO] Exécution en cours... (statut : {state}), on attend 5s")
            time.sleep(5)

        except (GoogleAPICallError, RetryError) as e:
            print(f"[ERROR] Impossible de vérifier le statut de l'exécution : {e}")
            return None

def trigger_worflow(table_name: str):
    print(f'     ***************   Start  trigger_worflow   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

    project_id = os.environ.get('GCP_PROJECT')
    if project_id is None:
        raise ValueError("La variable d'environnement 'GCP_PROJECT' n'est pas définie.")
    print(f'     project_id: {project_id}')

    location = os.environ.get('wkf_location')
    if location is None:
        location = 'europe-west1'
        print(f'On force la variable  "location" a "europe-west1"')
        #raise ValueError("La variable d'environnement 'wkf_location' n'est pas définie.")

    workflow_id = f'{table_name}_wkf'
    arguments = "{}"  # JSON string si besoin

    execution_id = trigger_workflow_for_a_table(project_id, location, workflow_id, arguments)
    if execution_id:
        wait_for_execution_completion(project_id, location, workflow_id, execution_id)

    print(f'     ***************   End  trigger_worflow   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

def move_file(bucket_name, blob_path, new_subfolder):
    """
    Move a file a to new subfolder as root.

    Args:
         bucket_name (str): Bucket name of the file.
         blob_path (str): Path of the blob inside the bucket.
         new_subfolder (str): Subfolder where to move the file.
    """
    print(f'     ***************   Start move_file   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')
    
    
    print(f'     bucket_name: {bucket_name}')
    print(f'     blob_path: {blob_path}')

    # TODO: 1
    # Now you are confortable with the first Cloud Function you wrote. 
    # Inspire youreslf from this first Cloud Function and:
    #     - connect to the Cloud Storage client
    storage_client = storage.Client()

    #     - get the bucket object and the blob object
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    #     - split the blob path to isolate the file name 
    blob_nom_sans_extension, blob_extension = blob_path.rsplit('.', 1)
    print(f' blob_nom_sans_extension : {blob_nom_sans_extension}')
    print(f' blob_extension          : {blob_extension}')
    #     - create your new blob path with the correct new subfolder given from the arguments
    new_blob_path = blob_path.replace('input', new_subfolder)
    print(f' new_blob_path           : {new_blob_path}')
    # the file is moved to the invalid/ folder if one check is failed
    try:
        bucket.rename_blob(blob, new_blob_path)
        print(f'{blob.name} moved to {new_blob_path}')
    except Exception as e:
            print(e)
    #     - move you file inside the bucket to its destination
    #     - print the actual move you made

    print(f'     ***************   End   move_file   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

if __name__ == '__main__':

    # here you can test with mock data the function in your local machine
    # it will have no impact on the Cloud Function when deployed.
    import os
    
    project_id = 'vast-verve-469412-c5'

    # Données à publier en bytes
    data = b'store'
    print(data)

    # Encodage en Base64
    encoded_data = base64.b64encode(data).decode('utf-8')
    print(encoded_data)

    # test your Cloud Function for the store file.
    mock_event = {
        'data': encoded_data.encode('utf-8'),
        'attributes': {
            'bucket_name': f'{project_id}_magasin_cie_landing',
            'blob_path': os.path.join('input', 'store_20220531.csv'),
        }
    }

    mock_context = {}
    receive_messages(mock_event, mock_context)