import json
import sys
# import ndjson
import requests
# import os
# import subprocess
# from elasticsearch import Elasticsearch
from collections import defaultdict
from argparse import ArgumentParser


# kibana_url = 'https://xxxxxxxxxx.us-east-1.aws.found.io:9243'


kibana_url = sys.argv[1]
API_KEY = sys.argv[2]
space_id = sys.argv[3]

headers = {
    'kbn-xsrf': 'true',
    'Content-Type': 'application/json',
    'Authorization': f'ApiKey {API_KEY}'
}


objects_endpoint = f"{kibana_url}/s/{space_id}/api/saved_objects/_find"


# Function to all data views in the space ID specified
def get_all_dataviews():
    dataview_url = f'{kibana_url}/s/{space_id}/api/data_views'
    response = requests.get(dataview_url, headers=headers, verify=True)
    response = response.json()
    data_views = response['data_view']
    return data_views


# Function to find duplicated data views by title
def find_duplicated_data_views(data_views):
    title_to_ids = defaultdict(list)
    for data_view in data_views:
        title = data_view["title"]
        id = data_view["id"]
        title_to_ids[title].append(id)
    duplicates = {title: ids for title, ids in title_to_ids.items() if len(ids) > 1}
    return duplicates


#
def get_object_references(data_view_ids):
    reference_counts = defaultdict(int)
    # Fetch all saved objects that could link to data views (e.g., dashboards, visualizations)
    object_type = ["search", "visualization", "dashboard", "map", "lens"]
    all_objects = []
    for type in object_type:
        params = {
            'fields': 'references',
            'type': type,
            'per_page': 10000
        }
        response = requests.get(objects_endpoint, headers=headers, params=params, verify=True)
        response.raise_for_status()
        data = response.json()
        all_objects.extend(data.get("saved_objects", []))

    # Count each object's link to a data view
    for object in all_objects:
        references = object.get("references", [])
        for ref in references:
            if ref["type"] == "index-pattern" and ref["id"] in data_view_ids:
                reference_counts[ref["id"]] += 1
    return reference_counts, all_objects


def update_references(object_type, object_id, old_data_view_id, new_data_view_id):
    object_endpoint = f"{kibana_url}/s/{space_id}/api/saved_objects/{object_type}/{object_id}"
    update_payload = {
        "references": [
            {
                "type": ref_type,
                "id": new_data_view_id,
                "name": ref_name
            }
        ],
        "attributes": {}
    }
    response = requests.put(object_endpoint, headers=headers, json=update_payload)
    response.raise_for_status()  # Raise an error if the request failed
    updated_kibana_object = response.json()
    print(f"Successfully updated data view ID for object type {object_type} with ID {object_id}.")
    print(f"Old Data View ID: {old_data_view_id}")
    print(f"New Data View ID: {new_data_view_id}")
    return (updated_kibana_object)


def has_references(data_view_id):
    for object in all_objects:
        references = object.get("references", [])
        for ref in references:
            if ref['type'] == 'index-pattern' and ref['id'] in data_view_id:
                return True
    return False


def delete_dataview_if_no_references(data_view_id):
    if not has_references(data_view_id):
        dataview_url = f'{kibana_url}/s/{space_id}/api/data_views/data_view/{data_view_id}'
        response = requests.delete(dataview_url, headers=headers)
        if response.status_code == 200:
            print("")
            print(f"Data view with ID {data_view_id} successfully DELETED.")
        else:
            print("")
            print(
                f"Failed to delete Old data view {data_view_id} . Status code: {response.status_code}, Response: {response.text}")
    else:
        print("")
        print(f"Data view {data_view_id} has references and was NOT deleted.")


def main():
    updated_objects_count = 0
    data_views_to_be_deleted = []
    objects_config_before_update = []
    updated_objects = []
    data_views = get_all_dataviews()
    # Step 2: Identify duplicated data views by title
    # Step 3 and 4: For each duplicated group, find the data view with the highest reference count
    duplicates = find_duplicated_data_views(data_views)
    print("")
    if not duplicates:
        print("No duplicated data views found.")
    else:
        dup_data_view_ids = []
        print("Duplicated data views found:")
        for title, ids in duplicates.items():
            # Get the reference counts for each data view ID in the duplicated group
            reference_counts, all_objects = get_object_references(ids)
            print(f"Title: {title}")
            for id in ids:
                print(f"  ID: {id}  : {reference_counts[id]}")
                dup_data_view_ids.append(id)
                most_referenced_id = max(reference_counts, key=reference_counts.get)
                if id != most_referenced_id:
                    data_views_to_be_deleted.append(id)
                    for object in all_objects:
                        object_references = object.get("references", [])
                        for ref in object_references:
                            if ref["id"] == id:
                                object_type = object['type']
                                object_id = object['id']
                                old_data_view_id = ref["id"]
                                ref_type = ref["type"]
                                ref_name = ref["name"]
                                new_data_view_id = most_referenced_id
                                print("")
                                update_references(object_type, object_id, old_data_view_id, new_data_view_id)
                                updated_objects.append(object)
                                print("")
                                print("")
                                updated_objects_count += 1
            print("")

    print("")
    if updated_objects:
        print("The following objects were updated:")
        for object in updated_objects:
            print(object)
        print("")
        print(f"{updated_objects_count} objects in total were UPDATED")
        print("")
    else:
        print("No objects were updated")
        print("")
    if duplicates:
        print("REVIEW DUPLICATE DATA VIEWS BEFORE REMOVING DUPLICATES WITH ZERO REFERENCES")
        for title, ids in duplicates.items():
            # Get the reference counts for each data view ID in the duplicated group
            reference_counts, all_objects = get_object_references(ids)
            print(f"Title: {title}")
            for id in ids:
                print(f"  ID: {id}  : {reference_counts[id]}")
                dup_data_view_ids.append(id)
            print("")
    print("")
    if data_views_to_be_deleted:
        print("ID of Data views to be deleted")
        print(data_views_to_be_deleted)
        try:
            data_views_to_be_deleted
        except NameError:
            print("")
            data_views_to_be_deleted = []
        for data_view in data_views_to_be_deleted:
            delete_dataview_if_no_references(data_view)
    else:
        print("There are no Data Views to be deleted")

main()
