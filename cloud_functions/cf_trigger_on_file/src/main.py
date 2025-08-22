#import re
from datetime import datetime
import os
#import datetime
from google.cloud import storage
from google.cloud import pubsub_v1

# This dictionary gives your the requirements and the specifications of the kind
# of files you can receive. 
#     - the keys are the names of the files
#     - the values give the required extension for each file 

FILES_AND_EXTENSION_SPEC = {
    'store': 'csv',
    'customer': 'csv',
    'basket': 'json'
}

def verifier_nom_fichier(nom_fichier):
    # Séparer le nom et l'extension
    if '.' not in nom_fichier:
        return False, "Le nom du fichier doit contenir une extension." ,"",""

    nom_sans_extension, extension = nom_fichier.rsplit('.', 1)

    # Vérifier que le nom comporte 2 parties séparées par _
    parties = nom_sans_extension.split('_')
    if len(parties) != 2:
        return False, "Le nom du fichier doit comporter exactement 2 parties séparées par '_'." ,"",""

    prefixe, date_str = parties

    # Vérifier que le préfixe est dans FILES_AND_EXTENSION_SPEC
    if prefixe not in FILES_AND_EXTENSION_SPEC:
        prefixes_autorises = list(FILES_AND_EXTENSION_SPEC.keys())
        return False, f"La première partie '{prefixe}' n'est pas autorisée. Les valeurs autorisées sont : {', '.join(prefixes_autorises)}." ,"",""

    # Vérifier que l'extension correspond à celle attendue pour le préfixe
    extension_attendue = FILES_AND_EXTENSION_SPEC[prefixe]
    if extension != extension_attendue:
        return False, f"L'extension '{extension}' ne correspond pas à l'extension attendue '{extension_attendue}' pour le préfixe '{prefixe}'." ,"",""

    # Vérifier que la deuxième partie est une date au format YYYYMMDD
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        return False, f"La deuxième partie '{date_str}' n'est pas une date valide au format YYYYMMDD." ,"",""

    # Si toutes les vérifications sont passées
    return True, "Le nom du fichier est valide.", parties[0] ,parties[1]



def check_file_format(event: dict, context: dict):
    """
    Triggered by a change to a Cloud Storage bucket.
    Check for the files requirements. Publishes a message to PubSub if the 
    file is verified else movs the files to the invalid/ subfolder.

    Args:
         event (dict): Event payload. 
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # rename the variable to be more specific and write it to the logs
    blob_event = event
    print(f'     ***************                 Start Processing blob: {blob_event["name"]}   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')

    # get the bucket name and the blob path
    bucket_name = blob_event['bucket']
    blob_path = blob_event['name']

    # get the subfolder, the file name and its extension
    *subfolder, file = blob_path.split(os.sep)  
    subfolder =  os.path.join(*subfolder) if subfolder != [] else ''
    file_name, file_extention = file.split('.') 

    print(f'Bucket name: {bucket_name}')
    print(f'File path: {blob_path}')
    print(f'Subfolder: {subfolder}')
    print(f'Full file name: {file}')
    print(f'File name: {file_name}')
    print(f'File Extension: {file_extention}')

    # Check if the file is in the subfolder `input/` to avoid infinite loop
    assert subfolder == 'input', 'File must be in `input/ subfolder to be processed`'
    
    # check if the file name has the good format
    # required format: <table_name>_<date>.<extension>
    try:
        # TODO: 
        # create some assertions here to validate your file. It is:
        #     - required to have two parts.
        #     - the first part is required to be an accepted table name
        #     - the second part is required to be a 'YYYYMMDD'-formatted date 
        #     - required to have the expected extension

        valide, message, part_one ,part_two = verifier_nom_fichier(file)
        #print(f"{part_one}: {message}")
        if not valide:
            print(f"{part_one}: {message}")
            return
            raise Exception(message)

        table_name = part_one

        # if all checks are succesful then publish it to the PubSub topic
        publish_to_pubsub(
            data=table_name.encode('utf-8'),
            attributes={
                'bucket_name': bucket_name, 
                'blob_path': blob_path
            }
        )

    except Exception as e:
        print(e)
        # the file is moved to the invalid/ folder if one check is failed
        move_to_invalid_file_folder(bucket_name, blob_path)

    print(f'     ***************                 End   Processing blob: {blob_event["name"]}   : {datetime.now().strftime("%H:%M:%S.%f")[:-4]}')    



def publish_to_pubsub(data: bytes, attributes: dict):
    """
    Publish a message to the pubsub topic to insert the file.

    Args:
         data (bytes): Encoded string as data for the message.
         attributes (dict): Custom attributes for the message.
    """
    ## this small part is here to be able to simulate the function but
    ## remove this part when you are ready to deploy your Cloud Function. 
    ## [start simulation]
    print('Your file is considered as valid. It will be published to Pubsub.')
    #print(f'     data: {data}')
    #print(f'     attributes: {attributes}')
    #return
    ## [end simulation]


    # retrieve the GCP_PROJECT from the reserved environment variables
    # more: https://cloud.google.com/functions/docs/configuring/env-var#python_37_and_go_111
    import os

    project_id = os.environ.get('GCP_PROJECT')
    if project_id is None:
        raise ValueError("La variable d'environnement 'GCP_PROJECT' n'est pas définie.")
    print(f'     project_id: {project_id}')
    topic_id = os.environ['pubsub_topic_id']
    if topic_id is None:
        raise ValueError("La variable d'environnement 'topic_id' n'est pas définie.")
    print(f'     topic_id: {topic_id}')
    
    # connect to the PubSub client
    publisher = pubsub_v1.PublisherClient()

    # publish your message to the topic
    topic_path = publisher.topic_path(project_id, topic_id)
    future = publisher.publish(topic_path, data, **attributes)

    future.result()  # Bloque jusqu'à ce que le message soit publié
    print(f"Message publié avec ID : {future.result()}")
    print(f'Published messages with custom attributes to {topic_path}.')

def move_to_invalid_file_folder(bucket_name: str, blob_path: str):
    """
    Move an invalid file from the input/ to the invalid/ subfolder.

    Args:
         bucket_name (str): Bucket name of the file.
         blob_path (str): Path of the blob inside the bucket.
    """

    ## this small part is here to be able to simulate the function but
    ## remove this part when you are ready to deploy your Cloud Function. 
    ## [start simulation]
    print('Your file is considered as invalid. It will be moved to invalid/.')
    #print(f'     bucket_name: {bucket_name}')
    #print(f'     blob_path: {blob_path}')
    #return
    ## [end simulation]
        
    # connect to the Cloud Storage client
    storage_client = storage.Client()

    # move the file to the invalid/ subfolder
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    new_blob_path = blob_path.replace('input', 'invalid')
    try:
        bucket.rename_blob(blob, new_blob_path)
        print(f'{blob.name} moved to {new_blob_path}')
    except Exception as e:
            print(e)
            # the file is moved to the invalid/ folder if one check is failed



if __name__ == '__main__':
    
    # here you can test with mock data the function in your local machine
    # it will have no impact on the Cloud Function when deployed.
    import os
    
    project_id = 'vast-verve-469412-c5'

    realpath = os.path.realpath(__file__)
    material_path = os.sep.join(['', *realpath.split(os.sep)[:-4], '__materials__'])
    init_files_path = os.path.join(material_path, 'data', 'init')

    # test your Cloud Function with each of the given files.
    for file_name in os.listdir(init_files_path):
        print(f'\nTesting your file {file_name}')
        mock_event = {
            'bucket': f'{project_id}_magasin_cie_landing',
            'name': os.path.join('input', file_name)
        }

        mock_context = {}
        check_file_format(mock_event, mock_context)
