import json
import requests
import sys
from datetime import datetime, timezone, timedelta
import time
import random
import string
import os

#
# --- Configuration and Authentication Functions ---
#

def load_config(file_path='config.json'):
    """
    Loads a JSON configuration file.
    """
    if not os.path.exists(file_path):
        sys.exit(f"ERROR: Configuration file '{file_path}' not found.")
    try:
        with open(file_path) as f:
            return json.load(f)
    except json.JSONDecodeError:
        sys.exit(f"ERROR: File '{file_path}' is not valid JSON.")
    except Exception as exc:
        sys.exit(f"ERROR loading '{file_path}': {exc}")

def get_auth_token(client_id, client_secret, base_url):
    """
    Gets an API authentication token from the Rubrik cluster.
    """
    try:
        url = f"{base_url}/api/client_token"
        response = requests.post(url, json={"client_id": client_id, "client_secret": client_secret}, timeout=30, verify=False)
        response.raise_for_status()
        token_data = response.json()
        if 'access_token' not in token_data:
             sys.exit(f"Authentication error: 'access_token' not found in response. Response: {token_data}")
        return token_data['access_token']
    except requests.exceptions.Timeout:
        sys.exit(f"ERROR: Authentication request timed out connecting to {url}")
    except requests.HTTPError as exc:
        response_text = f"HTTP {exc.response.status_code}"
        try:
            response_json = exc.response.json()
            response_text = json.dumps(response_json, indent=2)
        except json.JSONDecodeError:
            response_text = exc.response.text
        sys.exit(f"ERROR: Authentication HTTP error: {exc}\nResponse body:\n{response_text}")
    except requests.exceptions.RequestException as exc:
        sys.exit(f"ERROR: Authentication network error: {exc}")

def graphql_query(token, base_url, query, variables=None, debug=False):
    """
    Executes a GraphQL query and returns the data, raising an exception on API errors.
    """
    headers = {"Authorization": f"Bearer {token}"}
    api_url = f"{base_url}/api/graphql"
    
    if debug:
        print("\n" + "="*20 + " GraphQL Query " + "="*20)
        print(f"URL: {api_url}")
        print("--- Query ---")
        print(query)
        print("--- Variables ---")
        print(json.dumps(variables, indent=2))
        print("="*55 + "\n")

    try:
        response = requests.post(api_url, json={"query": query, "variables": variables or {}}, headers=headers, timeout=120, verify=False)
        if debug:
            print("\n" + "="*20 + " GraphQL Response " + "="*20)
            print(f"Status Code: {response.status_code}")
            print("--- Full Response Body ---")
            print(response.text)
            print("="*58 + "\n")

        response.raise_for_status()
        result = response.json()
        
        if 'errors' in result and result['errors']:
            error_messages = []
            for err in result['errors']:
                if isinstance(err, dict):
                    error_messages.append(err.get('message', 'Unknown GraphQL error'))
                else:
                    error_messages.append(str(err))
            raise Exception(f"GraphQL API errors returned:\n- " + "\n- ".join(error_messages))
        return result.get("data", {})
    except requests.HTTPError as exc:
        response_text = f"HTTP {exc.response.status_code}"
        try:
            response_json = exc.response.json()
            response_text = json.dumps(response_json, indent=2)
        except json.JSONDecodeError:
            response_text = exc.response.text
        raise Exception(f"GraphQL HTTP error: {exc}\nResponse body:\n{response_text}")
    except Exception as exc:
        raise Exception(f"An unexpected error occurred during GraphQL query: {exc}")

def get_all_paginated_nodes(token, base_url, query_string, variables, connection_path_keys, debug_mode):
    """
    Handles paginated GraphQL queries to fetch all nodes from a connection.
    """
    all_nodes = []
    current_variables = variables.copy()
    current_variables['first'] = 50
    current_variables['after'] = None
    while True:
        page_data_root = graphql_query(token, base_url, query_string, current_variables, debug=debug_mode)
        connection = page_data_root
        for key in connection_path_keys:
            connection = connection.get(key, {})
            if connection is None:
                connection = {}
                break

        nodes_on_page = connection.get('nodes')
        if nodes_on_page is None:
            edges = connection.get('edges', [])
            nodes_on_page = [edge.get('node') for edge in edges if edge.get('node')]

        if nodes_on_page:
             all_nodes.extend(nodes_on_page)
        
        page_info = connection.get('pageInfo', {})
        if page_info.get('hasNextPage') and page_info.get('endCursor'):
            current_variables['after'] = page_info['endCursor']
        else:
            break
    return all_nodes

#
# --- Recovery Object Processing Functions ---
#

def generate_mount_name(base_name):
    """
    Generates a unique name for a mounted object by appending a random suffix.
    """
    safe_base_name = "".join(c if c.isalnum() or c in ['_','-'] else '_' for c in base_name)[:40]
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{safe_base_name}-{random_suffix}"

def generate_oracle_mount_name(base_name):
    """
    Generates a unique Oracle DB name that is 8 characters or less.
    Format: <base_3_char>-<rand_4_char>
    """
    safe_base_name = base_name[:3].lower()
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f"{safe_base_name}-{random_suffix}"


def initiate_vm_mount(token, base_url, vm_object, vm_inventory, rename_vms, debug_mode):
    """
    Initiates a Live Mount for a Hyper-V VM and returns info needed for monitoring.
    """
    vm_name = vm_object['name']
    print(f"\n>> Preparing mount for VM: '{vm_name}'...")
    
    target_vm = next((vm for vm in vm_inventory if vm.get('name') == vm_name), None)
    if not target_vm: return False, f"VM '{vm_name}' not found in the Rubrik inventory.", None
    
    vm_id = target_vm.get('id')
    vm_cluster_id = target_vm.get('cluster', {}).get('id')
    print(f"  OK: Found VM. ID: {vm_id}")

    snapshot_fid = get_latest_snapshot_for_hyperv_vm(token, base_url, vm_id, debug_mode)
    
    available_hosts = get_connected_hyperv_servers_for_cluster(token, base_url, vm_cluster_id, debug_mode)
    if not available_hosts: return False, f"No connected Hyper-V hosts found for cluster ID {vm_cluster_id}.", None
    
    selected_host = random.choice(available_hosts)
    host_id = selected_host.get('id')
    
    mount_vm_name = generate_mount_name(vm_name) if rename_vms else vm_name
    print(f"  -> Will be mounted as '{mount_vm_name}' on host '{selected_host.get('name')}'.")

    mutation=''' mutation CreateHypervMount($input: CreateHypervVirtualMachineSnapshotMountInput!) { createHypervVirtualMachineSnapshotMount(input: $input) { id } }'''
    variables = {"input": {"id": snapshot_fid, "config": { "hostId": host_id, "vmName": mount_vm_name, "powerOn": True, "removeNetworkDevices": True }}}
    result = graphql_query(token, base_url, mutation, variables, debug=debug_mode).get('createHypervVirtualMachineSnapshotMount', {})
    
    if not result or not result.get('id'): return False, f"Mount initiation failed for '{vm_name}'. API Response: {result}", None

    job_info = { "type": "VM", "name": vm_name, "mount_name": mount_vm_name, "status": "INITIATED" }
    return True, f"  -> Mount initiation for '{vm_name}' started.", job_info

def initiate_oracle_live_mount(token, base_url, db_object, db_inventory, rename_oracle_dbs, debug_mode):
    """
    Initiates a Live Mount for an Oracle DB and returns info needed for monitoring.
    """
    db_name = db_object['name']
    source_hostname = db_object.get('hostname')
    recovery_target_hostname = db_object.get('recovery_target_hostname')

    if not recovery_target_hostname: return False, f"Recovery plan for DB '{db_name}' is missing the 'recovery_target_hostname' field.", None
    
    print(f"\n>> Preparing mount for Oracle DB: '{db_name}'...")
    target_db = next((db for db in db_inventory if db.get('name') == db_name), None)
    if not target_db: return False, f"Oracle DB '{db_name}' not found in the Rubrik inventory.", None

    db_id = target_db.get('id')
    db_cluster_id = target_db.get('cluster', {}).get('id')
    print(f"  OK: Found DB. ID: {db_id}")

    snapshot = get_latest_oracle_snapshot(token, base_url, db_id, debug_mode)
    snapshot_id = snapshot.get('id')

    all_hosts = get_oracle_hosts_for_cluster(token, base_url, db_cluster_id, debug_mode)
    target_host = next((h for h in all_hosts if h.get('name') == recovery_target_hostname), None)
    if not target_host: return False, f"Designated recovery host '{recovery_target_hostname}' was not found or is not available.", None
    
    host_id = target_host.get('id')
    print(f"  -> Will be mounted on designated host '{recovery_target_hostname}'.")
    
    config = { "targetOracleHostOrRacId": host_id, "shouldMountFilesOnly": False, "recoveryPoint": { "snapshotId": snapshot_id }, "shouldSkipDropDbInUndo": False }
    mount_db_name = db_name

    if rename_oracle_dbs:
        mount_db_name = generate_oracle_mount_name(db_name)
        print(f"  -> Generated new DB name for mount: '{mount_db_name}'")
        config['mountedDatabaseName'] = mount_db_name
        config['shouldAllowRenameToSource'] = False
    else:
        if source_hostname == recovery_target_hostname:
            return False, f"Cannot mount Oracle DB to source host '{source_hostname}' without renaming. Enable object renaming or choose a different target.", None
        config['shouldAllowRenameToSource'] = True

    mutation = '''
        mutation OracleDatabaseMountMutation($input: MountOracleDatabaseInput!) {
            mountOracleDatabase(input: $input) {
                id
            }
        }
    '''
    variables = { "input": { "request": { "id": db_id, "config": config }, "advancedRecoveryConfigMap": [] } }
    
    result = graphql_query(token, base_url, mutation, variables, debug=debug_mode).get('mountOracleDatabase', {})
    
    job_id = result.get('id')
    if not job_id: return False, f"Oracle Live Mount initiation for '{db_name}' failed. API Response: {result}", None
        
    job_info = { "type": "ORACLE_DB", "name": db_name, "mount_name": mount_db_name, "status": "INITIATED" }
    return True, f"  -> Mount initiation for '{db_name}' started.", job_info

def initiate_mssql_live_mount(token, base_url, db_object_from_config, rename_sql_dbs, debug_mode):
    """
    Initiates a Live Mount for a SQL Server DB and returns info needed for monitoring.
    """
    db_name = db_object_from_config.get('name')
    source_location = db_object_from_config.get('hostname')
    recovery_target_fqdn = db_object_from_config.get('recovery_target_hostname') 

    if not recovery_target_fqdn: return False, f"Recovery plan for DB '{db_name}' is missing 'recovery_target_hostname'.", None

    print(f"\n>> Preparing mount for SQL DB: '{db_name}' on '{source_location}'...")

    target_db = find_mssql_db_by_location(token, base_url, db_name, source_location, debug_mode)
    if not target_db: return False, f"SQL DB '{db_name}' at location '{source_location}' not found.", None
    
    db_id = target_db.get('id')
    print(f"  OK: Found DB. ID: {db_id}")

    recovery_point_date = get_latest_snapshot_for_mssql_db(token, base_url, db_id, debug_mode)
    print(f"  OK: Found latest recovery point: {recovery_point_date}")

    compatible_instances = get_compatible_mssql_instances(token, base_url, db_id, recovery_point_date, debug_mode)
    
    target_instance = None
    for inst in compatible_instances:
        host_name = inst.get('rootProperties', {}).get('rootName')
        inst_name = inst.get('name')
        full_path = f"{host_name}\\{inst_name}"
        
        if recovery_target_fqdn == host_name or recovery_target_fqdn == full_path:
            target_instance = inst
            break

    if not target_instance: return False, f"Designated recovery instance '{recovery_target_fqdn}' is not a compatible target for this database and snapshot.", None
    
    target_instance_id = target_instance.get('id')
    print(f"  OK: Designated recovery instance is compatible.")
    
    mount_db_name = db_name
    if rename_sql_dbs:
        mount_db_name = generate_mount_name(db_name)
    print(f"  -> Will be mounted as '{mount_db_name}'.")

    config = {
        "recoveryPoint": {"date": recovery_point_date},
        "targetInstanceId": target_instance_id,
        "mountedDatabaseName": mount_db_name
    }
    
    mutation = '''
        mutation MssqlDatabaseMountMutation($input: CreateMssqlLiveMountInput!) {
            createMssqlLiveMount(input: $input) {
                id
            }
        }
    '''
    variables = { "input": { "id": db_id, "config": config }}
    
    result = graphql_query(token, base_url, mutation, variables, debug=debug_mode).get('createMssqlLiveMount', {})
    job_id = result.get('id')
    if not job_id: return False, f"SQL Live Mount initiation for '{db_name}' failed. API Response: {result}", None
        
    job_info = { "type": "SQL_DB", "name": db_name, "mount_name": mount_db_name, "job_id": job_id, "status": "INITIATED" }
    return True, f"  -> Mount initiation for '{db_name}' started.", job_info


#
# --- Supporting API Functions ---
#

def get_protected_hyperv_vms(token, base_url, debug_mode):
    query = ''' query GetHyperVVms($first: Int, $after: String) { hypervVirtualMachines( filter: [{field: IS_RELIC, texts: ["false"]}, {field: IS_REPLICATED, texts: ["false"]}], first: $first, after: $after ) { nodes { id name cluster { id name } } pageInfo { hasNextPage endCursor } } }'''
    return get_all_paginated_nodes(token, base_url, query, {}, ['hypervVirtualMachines'], debug_mode)

def get_latest_snapshot_for_hyperv_vm(token, base_url, vm_id, debug_mode):
    query = ''' query GetVmSnapshots($workloadId: String!, $first: Int, $after: String) { snapshotOfASnappableConnection(workloadId: $workloadId, first: $first, after: $after) { nodes { id date isExpired isQuarantined } pageInfo { hasNextPage endCursor } } }'''
    all_snapshot_nodes = get_all_paginated_nodes(token, base_url, query, {"workloadId": vm_id}, ['snapshotOfASnappableConnection'], debug_mode)
    if not all_snapshot_nodes: raise Exception(f"No snapshots found for VM ID: {vm_id}.")
    valid_snaps = [s for s in all_snapshot_nodes if s.get('id') and not s.get('isExpired', False) and not s.get('isQuarantined', False)]
    if not valid_snaps: raise Exception(f"Found snapshots for VM ID {vm_id}, but none are valid.")
    try:
        latest_snap = max(valid_snaps, key=lambda s: datetime.strptime(s['date'], '%Y-%m-%dT%H:%M:%S.%fZ'))
    except (ValueError, TypeError): raise Exception(f"Could not determine latest snapshot date for VM ID {vm_id}.")
    snapshot_fid = latest_snap.get('id')
    if not snapshot_fid: raise Exception(f"Latest valid snapshot for VM ID {vm_id} is missing its 'id'.")
    print(f"  OK: Found latest valid snapshot FID: {snapshot_fid}")
    return snapshot_fid

def get_connected_hyperv_servers_for_cluster(token, base_url, cluster_id, debug_mode):
    query = ''' query GetHyperVHosts($filters: [Filter!]) { hypervServersPaginated( filter: $filters ) { nodes{ id name status { connectivity } } } }'''
    variables = { "filters": [ {"field": "IS_REPLICATED", "texts": ["false"]}, {"field": "IS_RELIC", "texts": ["false"]}, {"field": "CLUSTER_ID", "texts": [cluster_id]} ] }
    all_servers = graphql_query(token, base_url, query, variables, debug=debug_mode).get('hypervServersPaginated', {}).get('nodes', [])
    return [s for s in all_servers if s.get('status', {}).get('connectivity') == "Connected"]

def check_hyperv_mount_status(token, base_url, mount_name, debug_mode):
    query = """ query CheckHypervMountStatus($filters: [HypervLiveMountFilterInput!]) { hypervMounts(first: 1, filters: $filters) { nodes { mountedVmStatus } } } """
    variables = {"filters": [{"field": "MOUNT_NAME", "texts": [mount_name]}]}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('hypervMounts', {}).get('nodes', [])
    if nodes: return nodes[0].get('mountedVmStatus')
    return None

# --- Oracle Functions ---
def get_protected_oracle_dbs(token, base_url, debug_mode):
    query = ''' query GetOracleDBs($first: Int, $after: String) { oracleDatabases( filter:[ {field:IS_RELIC, texts:"false"}, {field:IS_REPLICATED, texts:"false"} ], first: $first, after: $after ) { nodes { id name cluster{id name} } pageInfo { hasNextPage endCursor } } }'''
    nodes = get_all_paginated_nodes(token, base_url, query, {}, ['oracleDatabases'], debug_mode)
    return nodes

def get_latest_oracle_snapshot(token, base_url, oracle_db_id, debug_mode):
    query = ''' query GetOracleDbSnapshot($fid: UUID!){ oracleDatabase(fid:$fid){ newestSnapshot{id date isExpired isQuarantined} } }'''
    data = graphql_query(token, base_url, query, {"fid": oracle_db_id}, debug=debug_mode).get('oracleDatabase', {})
    snapshot = data.get('newestSnapshot')
    if not snapshot or snapshot.get('isExpired') or snapshot.get('isQuarantined'): raise Exception(f"No valid newest snapshot found for DB {oracle_db_id}")
    return snapshot

def get_oracle_hosts_for_cluster(token, base_url, cluster_id, debug_mode):
    query = ''' query GetOracleHosts($filters: [Filter!]) { oracleTopLevelDescendants(filter: $filters) { nodes { id name objectType cluster { id name } } } }'''
    variables = { "filters": [ {"field": "IS_RELIC", "texts": ["false"]}, {"field": "IS_REPLICATED", "texts": ["false"]} ] }
    all_descendants = graphql_query(token, base_url, query, variables, debug=debug_mode).get('oracleTopLevelDescendants', {}).get('nodes', [])
    return [h for h in all_descendants if h.get('cluster', {}).get('id') == cluster_id]

def check_oracle_mount_status(token, base_url, mount_name, debug_mode):
    query = """ query FindOracleMount($filters: [OracleLiveMountFilterInput!]) { oracleLiveMounts(first: 1, filters: $filters) { nodes { status } } } """
    variables = { "filters": [{"field": "NAME", "texts": [mount_name]}]}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('oracleLiveMounts', {}).get('nodes', [])
    if nodes:
        return "AVAILABLE" if nodes[0].get('status') == "AVAILABLE" else "MOUNTING"
    return "MOUNTING"

# --- SQL Server Functions ---
def find_mssql_db_by_location(token, base_url, db_name, location, debug_mode):
    """
    Finds a single MSSQL Database by its name and location using API filters.
    """
    query = '''
        query getMssqlDb($filter: [Filter!]) {
            mssqlDatabases(filter: $filter) {
                nodes {
                    id
                    name
                }
            }
        }
    '''
    variables = {
        "filter": [
            {"field": "NAME", "texts": [db_name]},
            {"field": "LOCATION", "texts": [location]},
            {"field": "IS_RELIC", "texts": ["false"]},
            {"field": "IS_ARCHIVED", "texts": ["false"]}
        ]
    }
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('mssqlDatabases', {}).get('nodes', [])
    if nodes:
        return nodes[0]
    return None

def get_latest_snapshot_for_mssql_db(token, base_url, db_id, debug_mode):
    """
    Gets the date of the latest recovery point for a MSSQL database.
    """
    query = '''
        query GetMssqlDbSnapshot($fid: UUID!) {
            mssqlDatabase(fid: $fid) {
                cdmNewestSnapshot {
                    date
                }
            }
        }
    '''
    variables = {"fid": db_id}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    
    snapshot = data.get('mssqlDatabase', {}).get('cdmNewestSnapshot')
    
    if not snapshot or 'date' not in snapshot:
        raise Exception(f"No valid newest snapshot found for MSSQL DB {db_id}")
        
    return snapshot['date']

def get_compatible_mssql_instances(token, base_url, db_id, recovery_time, debug_mode):
    """
    Gets a list of compatible SQL instances for a given DB and recovery time.
    """
    query = '''
        query MssqlDatabaseCompatibleInstancesQuery($input: GetCompatibleMssqlInstancesV1Input!) {
            mssqlCompatibleInstances(input: $input) {
                data {
                    id
                    name
                    rootProperties {
                        rootName
                    }
                }
            }
        }
    '''
    variables = {
        "input": {
            "id": db_id,
            "recoveryType": "V1_GET_COMPATIBLE_MSSQL_INSTANCES_V1_REQUEST_RECOVERY_TYPE_MOUNT",
            "recoveryTime": recovery_time
        }
    }
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    return data.get('mssqlCompatibleInstances', {}).get('data', [])

def check_mssql_mount_status(token, base_url, mount_name, debug_mode):
    """
    Checks the status of a SQL mount object directly.
    """
    query = """ 
        query MssqlDatabaseLiveMountsQuery($filters: [MssqlDatabaseLiveMountFilterInput!]) {
            mssqlDatabaseLiveMounts(filters: $filters, first: 1) {
                nodes {
                    isReady
                }
            }
        }
    """
    variables = {"filters": [{"field": "MOUNTED_DATABASE_NAME", "texts": [mount_name]}]}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('mssqlDatabaseLiveMounts', {}).get('nodes', [])
    if not nodes:
        return "MOUNTING"
    
    return "AVAILABLE" if nodes[0].get('isReady') else "MOUNTING"


# --- Unmount and Cleanup Functions ---

def find_hyperv_mount_id(token, base_url, mount_name, debug_mode):
    """
    Finds the functional ID (fid) of a Hyper-V mount by its name.
    """
    query = """ 
        query FindSpecificHyperVMount($filters: [HypervLiveMountFilterInput!]) { 
            hypervMounts(first: 1, filters: $filters) { 
                nodes { 
                    fid 
                } 
            } 
        }
    """
    variables = { "filters": [{"field": "MOUNT_NAME", "texts": [mount_name]}] }
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('hypervMounts', {}).get('nodes', [])
    
    if not nodes: 
        print(f"\n  WARNING: No Hyper-V mount found named '{mount_name}'.")
        return None
    
    mount_fid = nodes[0].get('fid')
    if not mount_fid:
        print(f"\n  WARNING: Found Hyper-V mount '{mount_name}' but it is missing the required 'fid' for unmounting.")
        return None
        
    return mount_fid

def find_oracle_mount_id(token, base_url, mount_name, debug_mode):
    query = """ query FindOracleMount($filters: [OracleLiveMountFilterInput!]) { oracleLiveMounts(first: 1, filters: $filters) { nodes { id } } } """
    variables = { "filters": [{"field": "NAME", "texts": [mount_name]}]}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('oracleLiveMounts', {}).get('nodes', [])
    if nodes: return nodes[0].get('id')
    return None

def find_mssql_mount_id(token, base_url, mount_name, debug_mode):
    query = """ 
        query MssqlDatabaseLiveMountsQuery($filters: [MssqlDatabaseLiveMountFilterInput!]) {
            mssqlDatabaseLiveMounts(filters: $filters, first: 1) {
                nodes {
                    fid
                }
            }
        }
    """
    variables = {"filters": [{"field": "MOUNTED_DATABASE_NAME", "texts": [mount_name]}]}
    data = graphql_query(token, base_url, query, variables, debug=debug_mode)
    nodes = data.get('mssqlDatabaseLiveMounts', {}).get('nodes', [])
    if nodes: return nodes[0].get('fid')
    return None

def unmount_vm(token, base_url, mount_id, mount_name, debug_mode):
    print(f"\n  -> Attempting to unmount VM: '{mount_name}' (ID: {mount_id})...")
    mutation = """ mutation HyperVLiveMountUnmountMutation($input: DeleteHypervVirtualMachineSnapshotMountInput!) { deleteHypervVirtualMachineSnapshotMount(input: $input) { error { message } } } """
    variables = {"input": {"id": mount_id, "force": True}}
    max_retries = 3
    for attempt in range(max_retries):
        try:
            data = graphql_query(token, base_url, mutation, variables, debug=debug_mode)
            result = data.get('deleteHypervVirtualMachineSnapshotMount')
            if result and result.get('error') is None:
                print(" OK: Unmount task initiated.")
                return True
            else:
                error_msg = result.get('error', {}).get('message', 'Unknown error during unmount.')
                print(f"  Attempt {attempt + 1}: Failed to initiate unmount. Reason: {error_msg}")
        except Exception as e: print(f"  Attempt {attempt + 1}: Error during unmount API call: {e}")
        if attempt < max_retries - 1: time.sleep(10)
        else: print(f"  ERROR: All {max_retries} unmount attempts failed for {mount_name}.")
    return False

def unmount_oracle_db(token, base_url, mount_id, mount_name, debug_mode):
    print(f"  -> Attempting to unmount Oracle DB: '{mount_name}' (ID: {mount_id})...", end="")
    mutation = """ mutation OracleLiveMountUnmountMutation($input: DeleteOracleMountInput!) { deleteOracleMount(input: $input) { id } } """
    variables = {"input": {"id": mount_id, "force": True}}
    try:
        graphql_query(token, base_url, mutation, variables, debug=debug_mode)
        print(" OK: Unmount task initiated.")
        return True
    except Exception as e:
        print(f" Failure: {e}")
        return False

def unmount_mssql_db(token, base_url, mount_id, mount_name, debug_mode):
    print(f"  -> Attempting to unmount SQL DB: '{mount_name}' (ID: {mount_id})...", end="")
    mutation = """ mutation MssqlLiveMountUnmountMutation($input: DeleteMssqlLiveMountInput!) { deleteMssqlLiveMount(input: $input) { id } } """
    variables = {"input": {"id": mount_id, "force": True}}
    try:
        graphql_query(token, base_url, mutation, variables, debug=debug_mode)
        print(" OK: Unmount task initiated.")
        return True
    except Exception as e:
        print(f" Failure: {e}")
        return False

#
# --- Main Execution Logic ---
#

def main():
    """
    Main function to orchestrate the recovery process.
    """
    script_start_time = time.time()
    
    print("--- Rubrik Recovery Automation Script ---")
    
    api_creds = load_config('config.json')
    print(">> Authenticating with Rubrik cluster...")
    token = get_auth_token(api_creds['RUBRIK_CLIENT_ID'], api_creds['RUBRIK_CLIENT_SECRET'], api_creds['RUBRIK_BASE_URL'])
    print("   OK: Authentication successful.\n")

    recovery_plan_filename = 'recovery-template.json'
    if not os.path.exists(recovery_plan_filename):
        print(f">> Notice: '{recovery_plan_filename}' not found.")
        sample_config = {
          "recovery_plan_name": "Sample Auto-Generated Recovery Plan",
          "settings": {
              "debug_mode": False,
              "rename_vms": False,
              "rename_sql_dbs": True,
              "rename_oracle_dbs": True
          },
          "phases": [
            { "phase_number": 1, "description": "Databases", "wait_after_previous_seconds": 0,
              "objects_to_recover": [
                  {"name": "ORCL_DB_NAME", "type": "DB", "hostname": "oracle-host.example.com", "recovery_target_hostname": "target-oracle-host.example.com"},
                  {"name": "SQL_DB_NAME", "type": "DB", "hostname": "sql-host\\instance", "recovery_target_hostname": "target-sql-host\\instance"}
                ]
            },
            { "phase_number": 2, "description": "Application Servers", "wait_after_previous_seconds": 300,
              "objects_to_recover": [{"name": "SAMPLE_VM_NAME", "type": "VM"}]
            }
          ]
        }
        try:
            with open(recovery_plan_filename, 'w') as f:
                json.dump(sample_config, f, indent=2)
            print(f"   OK: A sample '{recovery_plan_filename}' has been created.")
            print(f"   Please edit it with your actual object names and run the script again.\n")
            sys.exit(0)
        except Exception as e:
            sys.exit(f"ERROR: Could not create sample config file: {e}")
            
    recovery_plan = load_config(recovery_plan_filename)
    plan_name = recovery_plan.get('recovery_plan_name', 'Unnamed Plan')
    print(f">> Loaded Recovery Plan: '{plan_name}'\n")

    # Load settings from the recovery plan, with defaults.
    plan_settings = recovery_plan.get('settings', {})
    debug_mode = plan_settings.get('debug_mode', False)
    rename_vms = plan_settings.get('rename_vms', False)
    rename_sql_dbs = plan_settings.get('rename_sql_dbs', False)
    rename_oracle_dbs = plan_settings.get('rename_oracle_dbs', False)

    try:
        print(">> Caching protected object inventories from Rubrik...")
        vm_inventory = get_protected_hyperv_vms(token, api_creds['RUBRIK_BASE_URL'], debug_mode)
        print(f"   - Found {len(vm_inventory)} protected Hyper-V VMs.")
        oracle_db_inventory = get_protected_oracle_dbs(token, api_creds['RUBRIK_BASE_URL'], debug_mode)
        print(f"   - Found {len(oracle_db_inventory)} protected Oracle DBs.")
        print("   OK: Inventories cached successfully.\n")
    except Exception as e:
        sys.exit(f"ERROR: Critical error during inventory caching: {e}")
        
    oracle_db_names = {db['name'] for db in oracle_db_inventory}
    
    mounted_objects = []
    
    phases = sorted(recovery_plan.get('phases', []), key=lambda p: p.get('phase_number', 0))
    for i, phase in enumerate(phases):
        phase_num = phase.get('phase_number')
        phase_desc = phase.get('description', 'No description')
        print(f"--- Starting Phase {phase_num}: {phase_desc} ---")

        initiated_jobs = []
        print("  -> Initiating all recovery jobs for this phase...")
        for recovery_object in phase.get('objects_to_recover', []):
            obj_type = recovery_object.get('type')
            obj_name = recovery_object.get('name')
            success, message, job_info = False, "Unknown object type", None

            try:
                if "SAMPLE_" in obj_name.upper():
                    print(f"   WARNING: Skipping sample object '{obj_name}'. Please update your '{recovery_plan_filename}'.")
                    continue
                
                if obj_type == "VM":
                    success, message, job_info = initiate_vm_mount(token, api_creds['RUBRIK_BASE_URL'], recovery_object, vm_inventory, rename_vms, debug_mode)
                elif obj_type == "DB":
                    if obj_name in oracle_db_names:
                        success, message, job_info = initiate_oracle_live_mount(token, api_creds['RUBRIK_BASE_URL'], recovery_object, oracle_db_inventory, rename_oracle_dbs, debug_mode)
                    else:
                        success, message, job_info = initiate_mssql_live_mount(token, api_creds['RUBRIK_BASE_URL'], recovery_object, rename_sql_dbs, debug_mode)
                else:
                    print(f"   WARNING: Skipping object '{obj_name}' with unknown type '{obj_type}'.")
                    continue
                
                if not success:
                    print(f"\nCRITICAL ERROR during initiation for '{obj_name}'.")
                    print(f"   Reason: {message}")
                    sys.exit(1)
                else:
                    print(message)
                    initiated_jobs.append(job_info)

            except Exception as e:
                print(f"\nCRITICAL SCRIPT ERROR in Phase {phase_num} while processing '{obj_name}'.")
                print(f"   Error details: {e}")
                sys.exit(1)
        
        if initiated_jobs:
            print(f"\n  .. Monitoring {len(initiated_jobs)} in-progress jobs for Phase {phase_num}...")
            max_polls = 60
            is_first_poll = True
            completed_messages = []

            while initiated_jobs:
                if not is_first_poll:
                    # Move cursor up to overwrite previous status block
                    print(f"\x1b[A" * (len(initiated_jobs) + len(completed_messages) + 1), end="")
                
                print(f"--- Monitoring Phase {phase_num} (Updated: {time.strftime('%H:%M:%S')}) ---")
                for msg in completed_messages:
                    print(msg)
                is_first_poll = False

                for job in initiated_jobs[:]:
                    try:
                        status = job['status']
                        if status not in ["POWEREDON", "AVAILABLE", "FAILED", "MOUNT_FAILED", "FAILURE"]:
                            if job['type'] == 'VM':
                                status = check_hyperv_mount_status(token, api_creds['RUBRIK_BASE_URL'], job['mount_name'], debug_mode) or "MOUNTING"
                            elif job['type'] == 'ORACLE_DB':
                                status = check_oracle_mount_status(token, api_creds['RUBRIK_BASE_URL'], job['mount_name'], debug_mode)
                            elif job['type'] == 'SQL_DB':
                                status = check_mssql_mount_status(token, api_creds['RUBRIK_BASE_URL'], job['mount_name'], debug_mode)
                            job['status'] = status

                        print(f"  - {job['name']:<20} ({job['type']}): {status:<15}")

                        if (job['type'] == 'VM' and status == 'POWEREDON') or \
                           (job['type'] in ['ORACLE_DB', 'SQL_DB'] and status == 'AVAILABLE'):
                            
                            success_message = f"  - {job['name']:<20} ({job['type']}): OK: SUCCESS         "
                            completed_messages.append(success_message)
                            
                            mount_id = None
                            mount_name_for_cleanup = job.get('mount_name', job.get('name'))
                            if job['type'] == 'VM': mount_id = find_hyperv_mount_id(token, api_creds['RUBRIK_BASE_URL'], mount_name_for_cleanup, debug_mode)
                            elif job['type'] == 'ORACLE_DB': mount_id = find_oracle_mount_id(token, api_creds['RUBRIK_BASE_URL'], mount_name_for_cleanup, debug_mode)
                            elif job['type'] == 'SQL_DB': mount_id = find_mssql_mount_id(token, api_creds['RUBRIK_BASE_URL'], mount_name_for_cleanup, debug_mode)
                            
                            if mount_id: mounted_objects.append({'type': job['type'], 'id': mount_id, 'name': mount_name_for_cleanup})
                            else: print(f"    -> WARNING: Could not find mount ID for '{job['name']}' for cleanup.")
                            
                            initiated_jobs.remove(job)

                        elif status in ['FAILED', 'FAILURE', 'MOUNT_FAILED']:
                            print(f"\nCRITICAL ERROR: Job for '{job['name']}' failed with status {status}.")
                            sys.exit(1)

                    except Exception as e:
                        print(f"\nCRITICAL SCRIPT ERROR while monitoring '{job['name']}'.")
                        print(f"   Error details: {e}")
                        sys.exit(1)
                
                if not initiated_jobs: break
                
                max_polls -= 1
                if max_polls <= 0: break
                time.sleep(10)
            
            if initiated_jobs:
                print(f"\nCRITICAL ERROR: Phase {phase_num} timed out.")
                for job in initiated_jobs: print(f"  - {job['name']} did not complete.")
                sys.exit(1)

        print(f"--- OK: Phase {phase_num} Completed Successfully ---")

        if i < len(phases) - 1:
            wait_time = phase.get('wait_after_previous_seconds', 0)
            if wait_time > 0:
                print(f"\n.. Waiting for {wait_time} seconds before starting the next phase...")
                time.sleep(wait_time)
                print("   OK: Wait complete.\n")

    print("\n\n*** All recovery phases completed successfully! ***")
    
    script_end_time = time.time()
    duration_seconds = script_end_time - script_start_time
    minutes, seconds = divmod(duration_seconds, 60)
    print(f"\n>> Total recovery runtime: {int(minutes)} minutes and {seconds:.2f} seconds.")
    
    if mounted_objects:
        print("\n--- Starting Cleanup Phase: Unmounting all live mounts ---")
        for mount in mounted_objects:
            if mount['type'] == 'VM': unmount_vm(token, api_creds['RUBRIK_BASE_URL'], mount['id'], mount['name'], debug_mode)
            elif mount['type'] == 'ORACLE_DB': unmount_oracle_db(token, api_creds['RUBRIK_BASE_URL'], mount['id'], mount['name'], debug_mode)
            elif mount['type'] == 'SQL_DB': unmount_mssql_db(token, api_creds['RUBRIK_BASE_URL'], mount['id'], mount['name'], debug_mode)
        print("--- OK: Cleanup Phase Complete ---")
    
    sys.exit(0)


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    main()
