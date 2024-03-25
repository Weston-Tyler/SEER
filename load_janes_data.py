import json
import os
import hashlib
import pathlib
import logging
from typing import Optional, List

import tqdm
import geodesic

JANES_JSON_LD_PATH = 'json-ld'

# This loads data into a project/subgraph called "janes"
project = geodesic.create_project(
    name='janes',
    alias='Janes Graph',
    description="a demo project showcasing Janes data imported from JSON-LD"
)

# Set active project for the remainder of this script
geodesic.set_active_project(project)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Predefined mapping from the Janes type ontology into our classes. Type+Class should match up to the Janes ontology
type_class_map = {
    'https://data.janes.com/ontologies/equipment/Equipment': 'Entity',
    'https://data.janes.com/ontologies/orbat/MilitaryGroup': 'Entity',
    'https://data.janes.com/ontologies/geo/Country': 'Entity',
    'https://data.janes.com/ontologies/equipment/EquipmentFamily': 'Property',
    'https://data.janes.com/ontologies/organization/Organization': 'Entity',
    'https://data.janes.com/ontologies/classification/Classification': 'Property',
    'https://data.janes.com/ontologies/specifications/Condition': 'Property',
    'https://data.janes.com/ontologies/equipment/EquipmentVariant': 'Property',
    'https://data.janes.com/ontologies/specifications/Specification': 'Property',
    'https://data.janes.com/ontologies/installation/Installation': 'Entity',
    'https://data.janes.com/ontologies/inventory/InServiceInventory': 'Entity',
    'https://data.janes.com/ontologies/agent/Organization': 'Entity',
    'https://data.janes.com/ontologies/inventory/AcquisitionInventory': 'Entity',
    'https://data.janes.com/ontologies/installation/Runway': 'Entity',
    'https://data.janes.com/ontologies/geo/Country': 'Entity',
    'https://data.janes.com/ontologies/agent/JointVenture': 'Concept',
    'https://data.janes.com/ontologies/unit/Unit': 'Concept'
}


# Define a few helper functions below. This first one loads the JSON-LD data
def load(obj_type: str) -> dict:
    '''loads the JSON-LD files and context
    '''
    data, data_ctx = {}, {}
    with open(pathlib.Path(JANES_JSON_LD_PATH) / f'{obj_type}.json', 'r') as fp:
        data = json.load(fp)
    with open(pathlib.Path(JANES_JSON_LD_PATH) / f'{obj_type}-context.json', 'r') as fp:
        data_ctx = json.load(fp)

    return data, data_ctx


def convert_predicate_name(p: str) -> str:
    '''converts from a camelCase predicate from the Jane's ontology to Entanglement's required hyphen-delimited
    '''
    new = []
    for i, char in enumerate(p):
        # if uppercase, append a '-' and lowercase it
        if char == char.upper():
            if i > 0:
                new.append('-')
            new.append(char.lower())
        else:
            new.append(char)

    return ''.join(new)


# Not used, but an example of reversing some of our required name changes
def unconvert_predicate_name(p: str) -> str:
    '''
    converts from Entanglement's required hyphen-delimited predicate name to a
    camelCase predicate from the Jane's ontology
    '''
    new = []
    to_upper = False
    for char in p:
        if char == '-':
            to_upper = True
            continue
        if to_upper:
            char = char.upper()
            to_upper = False

        new.append(char)
    return ''.join(new)


def extract_location_info(obj: dict) -> dict:
    '''converts Janes location info into a standard geometry object and any location triples
    '''
    if 'locatedAt' not in obj:
        return {}

    la = obj['locatedAt']
    lat = la.get('lat')
    lon = la.get('long')

    geom = None
    if lat is not None and lon is not None:
        geom = f'POINT({lon} {lat})'

    location_country = la.get('locationCountry')
    geoprecision = geoprecision = la.get('geoprecision')

    return {
        k: v for k, v in
        [
            ('geometry', geom),
            ('location_country', location_country),
            ('geoprecision', geoprecision)
        ] if v is not None
    }


# Converts the Jane's IRI-based ID into a 'name' id that we use within Entanglement. Objects can be looked up
# using the Janes ID with the "xid" field in Entanglement
def convert_id(id: str) -> str:
    iri, uid = os.path.split(id)
    prefix = iri.split('/')[-1]

    h = hashlib.sha1(id.encode())
    h = h.hexdigest()
    return f'janes-{prefix}-{uid.lower()}-{h[:8]}'


# Convert the Janes type into a Entanglement type qualifier
def convert_type(id: str) -> str:
    _, uid = os.path.split(id)
    uid = convert_predicate_name(uid)
    return uid.lower()


def parse_props(name: str, subj: dict, objects: dict = {}, connections: list = []) -> dict:
    '''
    This loops through all the key/value pairs in an object, identifying which are nodes and which are properties.

    Args:
        name: the Entanglement name of the Object
        subj: the JSON-LD object to parse
        objects: a dictionary of objects so far
        connections: a list of connections so far

    Returns:
        a dict of properties for this object
    '''
    props = {}
    for predicate_or_key, obj_or_list in subj.items():
        if predicate_or_key == 'id':
            continue

        if not isinstance(obj_or_list, (dict, list)):
            # not a dict or a list, so is a scalar property of some sort
            props[predicate_or_key] = obj_or_list
            continue

        if isinstance(obj_or_list, list):
            for sub_obj in obj_or_list:
                prop = traverse(sub_obj, objects, connections)
                if prop is not None:
                    props[predicate_or_key] = prop
                # valid predicate
                else:
                    obj_name = convert_id(sub_obj['id'])
                    predicate = convert_predicate_name(predicate_or_key)
                    connections.append((name, predicate, obj_name))
        elif isinstance(obj_or_list, dict):
            prop = traverse(obj_or_list, objects, connections)
            if prop is not None:
                props[predicate_or_key] = prop
            else:
                obj_name = convert_id(obj_or_list['id'])
                predicate = convert_predicate_name(predicate_or_key)
                connections.append((name, predicate, obj_name))

    return props


# Big function here gets called recursively on the objects, extracting everything with an ID (an RDF node) and creating
# list of objects and connections
def traverse(subj: dict, objects: dict = {}, connections: list = []) -> Optional[dict]:
    """traverses a JSON-LD graph to extract geodesic.entanglement.Object and connections

    Args:
        subj: the root node to start traversal
        objects: the dictionary of objects so far
        connections: the list of connections so far

    Returns:
        The last object that it touched
    """

    if 'id' not in subj:
        return subj

    xid = subj['id']
    name = convert_id(xid)
    try:
        _type = subj['type']
        janesType = convert_type(_type)
    except Exception:
        _type = ''
        janesType = "*"

    loc_info = extract_location_info(subj)

    object_class = type_class_map.get(_type, 'Entity')

    props = parse_props(name, subj, objects=objects, connections=connections)

    objects[name] = dict(
        alias=subj.get('label', name),
        domain='military',
        object_class=object_class,
        name=name,
        xid=xid,
        type=janesType,
        item=props,
        project=project.uid
    )

    geom = loc_info.get('geometry')
    if geom is not None:
        objects[name]['geometry'] = geom

    lc = loc_info.get('location_country')
    if lc is not None:
        traverse(lc, objects, connections=connections)
        name = convert_id(subj['id'])
        obj_name = convert_id(lc['id'])
        connections.append((name, 'location-country', obj_name))

    gp = loc_info.get('geoprecision')
    if gp is not None:
        traverse(gp, objects, connections=connections)
        name = convert_id(subj['id'])
        obj_name = convert_id(gp['id'])
        connections.append((name, 'geoprecision', obj_name))

    obj = geodesic.entanglement.Object(**objects[name])
    objects[name] = obj


def write_connections(conn_objects: List[geodesic.entanglement.Connection], batch_size=100):
    batch_size = 100
    i = 0
    j = i + batch_size
    j = min(j, len(conn_objects))
    progress = tqdm.tqdm(total=len(conn_objects))
    while j <= len(conn_objects) and (i != j):
        geodesic.entanglement.add_connections(conn_objects[i:j], overwrite=True)

        i = j
        j = i + batch_size
        j = min(j, len(conn_objects))
        progress.update(j - i)
    progress.close()


def main():
    logger.info("parsing JSON-LD")

    # Load all the Janes JSON-LD data
    equip, _ = load('equipment')
    installations, _ = load('installations')
    inventory, _ = load('inventory')
    military_groups, _ = load('military-groups')
    organizations, _ = load('organizations')
    units, _ = load('units')

    objects = {}
    connections = []

    # Traverse all shapes/subgraphs
    for x in equip:
        traverse(x, objects, connections)
    for x in installations:
        traverse(x, objects, connections)
    for x in inventory:
        traverse(x, objects, connections)
    for x in military_groups:
        traverse(x, objects, connections)
    for x in organizations:
        traverse(x, objects, connections)
    for x in units:
        traverse(x, objects, connections)

    unique_predicates = list(set(p for _, p, _ in connections))

    logger.info(
        "extracted %d objects with %d connections with %d unique predicates",
        len(objects), len(connections), len(unique_predicates))

    # Update Entanglement's ontology
    geodesic.entanglement.add_predicates(trait="JanesOntology", predicates=[{'name': p} for p in unique_predicates])
    exit()   
    # Save each to entanglement - this will take a little because it's one round trip per object. Future enhancements
    # will batch these requests
    logger.info("saving objects to Entanglement")
    for obj in tqdm.tqdm(objects.values()):
        obj.save()

    # Convert the triples into Entanglement Connections
    conn_objects = [
        geodesic.entanglement.Connection(
            subject=objects[sub],
            predicate=predicate,
            object=objects[obj]
        )
        for sub, predicate, obj in connections if sub != obj
    ]

    logger.info("saving connections to Entanglement")
    write_connections(conn_objects=conn_objects, batch_size=10)


if __name__ == '__main__':
    main()
