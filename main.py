import requests
import dotenv
import os
import csv
import logging
import sys
import argparse
import json

logging.basicConfig(format="%(asctime)s %(levelname)s %(process)d: %(filename)s:%(lineno)d %(message)s", level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("data_downloader")


def fetch_object_list(endpoint, limit=10, starting_after=None, project_id=None, project_name=None, headers=None):
    """
    Fetches a paginated list of objects from the Braintrust API.

    Args:
        endpoint (str): The API endpoint to fetch objects from (e.g., 'experiments', 'datasets')
        limit (int, optional): Maximum number of objects to fetch per request. Defaults to 100
        starting_after (str, optional): ID of the object to start fetching after for pagination. Defaults to None
        project_id (str, optional): Filter objects by project ID. Mutually exclusive with project_name. Defaults to None
        project_name (str, optional): Filter objects by project name. Mutually exclusive with project_id. Defaults to None
        headers (dict, optional): HTTP headers to include in the request. Defaults to None

    Returns:
        list: A list of all fetched objects from the API

    Raises:
        requests.exceptions.RequestException: If there is an error with the API request
        Exception: For any other unexpected errors
    """
    if starting_after:
        params = {
            "limit": limit,
            "starting_after": starting_after
        }
    else:
        params = {
            "limit": limit,
        }
    
    if project_id:
        params["project_id"] = project_id
    if project_name:
        params["project_name"] = project_name
        
    all_objects = []
    while True:
        try:
            response = requests.get(
                f'https://api.braintrust.dev/v1/{endpoint}',
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            items = data.get('objects', [])
            all_objects.extend(items)
            
            if len(items) < limit:
                break
                
            params["starting_after"] = items[-1]["id"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching object list from {endpoint}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in fetch_object_list: {str(e)}")
            raise

    return all_objects


def fetch_events(object_id, endpoint, limit=100, headers=None):
    """
    Fetches all events associated with a specific object using cursor-based pagination.

    Args:
        object_id (str): ID of the object (experiment or dataset) to fetch events for
        endpoint (str): The API endpoint to fetch events from (e.g., 'experiments', 'datasets')
        limit (int, optional): Maximum number of events to fetch per request. Defaults to 100
        headers (dict, optional): HTTP headers to include in the request. Defaults to None

    Returns:
        list: A list of all events for the specified object

    Raises:
        requests.exceptions.RequestException: If there is an error with the API request
        Exception: For any other unexpected errors
    """
    object_events = []
    cursor = None
    while True:
        try:
            params = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            response = requests.get(f"https://api.braintrust.dev/v1/{endpoint}/{object_id}/fetch",
                                    headers=headers,
                                    params=params
                        )
            response.raise_for_status()
            data = response.json()
            events = data.get("events", [])
            object_events.extend(events)
            cursor = data.get("cursor")
            if not cursor:
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching events for {endpoint}/{object_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in fetch_events: {str(e)}")
            raise
    return object_events


def normalize_event(event):
    """Normalizes an event by flattening its structure and ensuring consistent keys.

    Args:
        event (dict): The event to normalize.

    Returns:
        dict: The normalized event with:
            - 'input', 'output', 'expected', and 'metadata' as top-level keys
            - Any nested dictionaries or lists converted to JSON strings
            - Original structure preserved for non-nested values

    Note:
        If 'input' is a dictionary, its contents are extracted and placed at the root level.
        All nested dictionaries and lists are converted to JSON strings for CSV compatibility.
    """
    if isinstance(event.get("input"), dict):
        input_data = event.pop("input")
        event["input"] = input_data.get("input")
        event["output"] = input_data.get("output")
        event["expected"] = input_data.get("expected")
        event["metadata"] = input_data.get("metadata")

    for key, value in event.items():
        if isinstance(value, (dict, list)):
            event[key] = json.dumps(value)
    return event 
            

def write_to_csv(events, directory, object_id):
    """
    Writes event data to a CSV file, with normalized events having consistent columns.
    
    Args:
        events (list): List of event dictionaries
        directory (str): Directory path where the CSV file will be created
        object_id (str): ID of the object, used to generate the filename as '{object_id}.csv'

    Returns:
        None

    Raises:
        IOError: If there is an error creating the directory or writing to the file
        Exception: For any other unexpected errors

    Note:
        - Creates the directory if it doesn't exist
        - Skips writing if events list is empty
        - Converts nested dictionaries and lists to string representations
        - Creates CSV file at 'braintrust_data/{directory}/{object_id}.csv'
    """
    try:
        os.makedirs("braintrust_data", exist_ok=True)
        os.makedirs(f"braintrust_data/{directory}", exist_ok=True)
        if not events:
            logger.warning(f"No events found for {object_id}")
            return
        
        filename = f"braintrust_data/{directory}/{object_id}.csv"
        normalized_events = [normalize_event(event) for event in events]
        fieldnames = set()
        for event in normalized_events:
            fieldnames.update(event.keys())
        
        with open(filename, "w", newline='') as file:
            writer = csv.DictWriter(file, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(normalized_events)

    except IOError as e:
        logger.error(f"Error writing to CSV file {filename}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while writing CSV for {object_id}: {str(e)}")
        raise

def download_data(endpoint, project_id=None, project_name=None, headers=None):
    """
    Downloads all events for objects from a specified endpoint and saves them to CSV files.

    Args:
        endpoint (str): The API endpoint to download data from (e.g., 'experiments', 'datasets')
        project_id (str, optional): Filter objects by project ID. Mutually exclusive with project_name. Defaults to None
        project_name (str, optional): Filter objects by project name. Mutually exclusive with project_id. Defaults to None
        headers (dict, optional): HTTP headers to include in the request. Defaults to None

    Returns:
        None

    Raises:
        Exception: If there is an error downloading data or processing objects

    Note:
        - Creates a directory named after the endpoint and saves individual CSV files for each object
        - Continues processing remaining objects if one object fails
        - Logs any failed endpoints at the end of processing
    """
    try:
        failed_endpoints = []
        failed_objects = []
        objects_without_events = []
        objects_list = fetch_object_list(endpoint, project_id=project_id, project_name=project_name, headers=headers)
        for obj in objects_list:
            try:
                object_events = fetch_events(obj['id'], endpoint, headers=headers)
                if not object_events:
                    objects_without_events.append(obj['id'])
                    continue
                write_to_csv(object_events, endpoint, obj['id'])
            except Exception as e:
                logger.error(f"Error processing {endpoint}/{obj['id']}: {str(e)}")
                failed_endpoints.append(endpoint)
                failed_objects.append(obj['id'])
                continue

        logger.info(f"Processing complete for {endpoint}:")
        logger.info(f"Total objects processed: {len(objects_list)}")
        logger.info(f"Objects with no events: {len(objects_without_events)}")
        if objects_without_events:
            logger.info(f"IDs of objects with no events: {objects_without_events}")

    except Exception as e:
        logger.error(f"Error downloading data for {endpoint}: {str(e)}")
        logger.error(f"Failed to download data for {endpoint} objects: {failed_objects}")
        raise
    if failed_endpoints:
        logger.error(f"Failed to download data for endpoints: {failed_endpoints}")

def main(project_id=None, project_name=None):
    """
    Main function to download all experiments and datasets events for a specific project.

    Args:
        project_id (str, optional): The project ID to filter objects. Mutually exclusive with project_name. Defaults to None
        project_name (str, optional): The project name to filter objects. Mutually exclusive with project_id. Defaults to None

    Returns:
        None

    Raises:
        SystemExit: With exit code 1 if there is a fatal error during execution or if API_KEY is not set

    Note:
        - Requires API_KEY to be set in environment variables (can be loaded from .env file)
        - Downloads both experiments and datasets data
        - Creates separate directories for experiments and datasets
        - Requires exactly one of project_id or project_name to be provided
    """
    dotenv.load_dotenv()
    API_KEY = os.getenv('BRAINTRUST_API_KEY')
    if not API_KEY:
        logger.error("BRAINTRUST_API_KEY is not set")
        sys.exit(1)
    HEADERS = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        download_data("experiment", project_id=project_id, project_name=project_name, headers=HEADERS)
        download_data("dataset", project_id=project_id, project_name=project_name, headers=HEADERS)
        logger.info("Data download completed successfully")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download all experiments and datasets for a project as CSVs."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--project-id", help="The project ID to filter objects", type=str)
    group.add_argument("--project-name", help="The project name to filter objects", type=str)
    
    args = parser.parse_args()
    main(project_id=args.project_id, project_name=args.project_name)