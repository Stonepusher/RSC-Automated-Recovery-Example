# -*- coding: utf-8 -*-
"""
Rubrik Recovery Orchestrator

This script automates the process of mounting Azure Local VMs, Oracle Databases,
and Microsoft SQL Server databases from Rubrik backups based on a defined
recovery plan. It features phased execution, status monitoring, and automated
cleanup.
"""
import json
import requests
import sys
import time
import random
import string
import os
import logging
from datetime import datetime

# --- Constants ---
#
# Character set for generating unique suffixes for mounted objects.
RANDOM_SUFFIX_CHARS = string.ascii_lowercase + string.digits

#
# --- Logging Setup ---
#

def setup_logging(debug_mode=False):
    """
    Configures logging for the script.

    Logs are printed to the console. The level is set to DEBUG if debug_mode is
    True, otherwise it's set to INFO. The script does not log to a file by
    default, but can be configured to do so by adding the 'filename' parameter
    to basicConfig.
    """
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Disable noisy logs from the requests library
    logging.getLogger("urllib3").setLevel(logging.WARNING)

#
# --- Configuration and Authentication ---
#

class ConfigError(Exception):
    """Custom exception for configuration-related errors."""
    pass

def load_json_file(file_path):
    """
    Loads and validates a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.

    Raises:
        ConfigError: If the file is not found, is not valid JSON, or cannot be read.
    """
    logging.info(f"Loading configuration from '{file_path}'...")
    print(f"🔄 [STATUS] Loading configuration from '{file_path}'...")
    if not os.path.exists(file_path):
        raise ConfigError(f"Configuration file '{file_path}' not found.")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            logging.info(f"Successfully loaded and validated '{file_path}'.")
            print(f"✅ [SUCCESS] Successfully loaded and validated '{file_path}'.\n")
            return data
    except json.JSONDecodeError as e:
        raise ConfigError(f"File '{file_path}' is not a valid JSON file. Error: {e}")
    except Exception as e:
        raise ConfigError(f"Failed to load '{file_path}': {e}")

def get_auth_token(client_id, client_secret, base_url):
    """
    Authenticates with the Rubrik cluster and retrieves an API token.
    """
    logging.info("Requesting API authentication token...")
    print("🔐 [STATUS] Authenticating with Rubrik cluster...")
    api_url = f"{base_url}/api/client_token"
    payload = {"client_id": client_id, "client_secret": client_secret}
    try:
        response = requests.post(api_url, json=payload, timeout=30, verify=False)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get('access_token')
        if not access_token:
            raise ConfigError("Authentication failed: 'access_token' not found in response.")
        logging.info("Successfully authenticated with Rubrik cluster.")
        print("✅ [SUCCESS] Authentication successful.\n")
        return access_token
    except requests.exceptions.RequestException as e:
        raise ConfigError(f"Authentication network error connecting to {api_url}: {e}")

#
# --- GraphQL API Communication ---
#

class GqlError(Exception):
    """Custom exception for GraphQL API errors."""
    pass

def execute_graphql_query(token, base_url, query, variables=None, debug=False):
    """
    Executes a GraphQL query against the Rubrik API.

    Args:
        token (str): The API authentication token.
        base_url (str): The base URL of the Rubrik cluster.
        query (str): The GraphQL query string.
        variables (dict, optional): Variables for the GraphQL query.
        debug (bool): If True, prints verbose query and response details.

    Returns:
        dict: The "data" portion of the API response.

    Raises:
        GqlError: If the API returns an error or the request fails.
    """
    api_url = f"{base_url}/api/graphql"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"query": query, "variables": variables or {}}

    if debug:
        print("\n" + "="*20 + " GraphQL Query " + "="*20)
        print(f"URL: {api_url}")
        print("--- Query ---")
        print(query)
        print("--- Variables ---")
        print(json.dumps(variables, indent=2))
        print("="*55 + "\n")

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=180, verify=False)
        
        if debug:
            print("\n" + "="*20 + " GraphQL Response " + "="*20)
            print(f"Status Code: {response.status_code}")
            print("--- Full Response Body ---")
            try:
                print(json.dumps(response.json(), indent=2))
            except json.JSONDecodeError:
                print(response.text)
            print("="*60 + "\n")

        response.raise_for_status()
        result = response.json()

        if 'errors' in result and result['errors']:
            error_messages = [err.get('message', 'Unknown GraphQL error') for err in result['errors']]
            raise GqlError(f"GraphQL API returned errors: {'; '.join(error_messages)}")

        return result.get("data", {})

    except requests.HTTPError as e:
        response_details = f"HTTP {e.response.status_code}"
        try:
            response_details = e.response.json()
        except json.JSONDecodeError:
            response_details = e.response.text
        raise GqlError(f"GraphQL HTTP error: {e}. Response: {response_details}")
    except requests.exceptions.RequestException as e:
        raise GqlError(f"Network error during GraphQL request: {e}")

def get_all_paginated_nodes(token, base_url, query, variables, connection_path, debug_mode):
    """
    Handles paginated GraphQL queries to fetch all nodes from a connection.
    """
    all_nodes = []
    current_vars = variables.copy()
    current_vars['first'] = 100
    current_vars['after'] = None

    while True:
        data = execute_graphql_query(token, base_url, query, current_vars, debug=debug_mode)
        connection = data
        for key in connection_path.split('.'):
            connection = connection.get(key, {})
        
        nodes = connection.get('nodes', [])
        all_nodes.extend(nodes)

        page_info = connection.get('pageInfo', {})
        if page_info.get('hasNextPage') and page_info.get('endCursor'):
            current_vars['after'] = page_info['endCursor']
        else:
            break
    return all_nodes

#
# --- Object Name Generation ---
#

def generate_mount_name(base_name, prefix="rec"):
    """
    Generates a unique name for a mounted object by appending a random suffix.
    """
    safe_base = "".join(c for c in base_name if c.isalnum() or c in ['_','-'])[:35]
    random_suffix = ''.join(random.choices(RANDOM_SUFFIX_CHARS, k=5))
    return f"{prefix}-{safe_base}-{random_suffix}"

def generate_oracle_mount_name(base_name):
    """
    Generates a unique Oracle DB name (<= 8 chars). Format: <base_3>-<rand_4>
    """
    safe_base = base_name[:3].lower()
    random_suffix = ''.join(random.choices(RANDOM_SUFFIX_CHARS, k=4))
    return f"{safe_base}-{random_suffix}"

#
# --- Recovery Object Classes ---
#

class RecoveryObject:
    """Base class for a recoverable object."""
    def __init__(self, token, base_url, config_obj, debug_mode=False):
        self.token = token
        self.base_url = base_url
        self.config = config_obj
        self.name = config_obj['name']
        self.mount_name = None
        self.mount_id = None
        self.status = "PENDING"
        self.debug = debug_mode

    def initiate_mount(self, inventory, settings):
        raise NotImplementedError

    def check_status(self):
        raise NotImplementedError

    def unmount(self):
        raise NotImplementedError

    def _get_latest_valid_snapshot(self, workload_id):
        """Finds the most recent, valid snapshot for a workload."""
        query = """
            query GetVmSnapshots($workloadId: String!, $first: Int, $after: String) {
                snapshotOfASnappableConnection(workloadId: $workloadId, first: $first, after: $after) {
                    nodes { id date isExpired isQuarantined }
                    pageInfo { hasNextPage endCursor }
                }
            }
        """
        all_snaps = get_all_paginated_nodes(self.token, self.base_url, query, {"workloadId": workload_id}, 'snapshotOfASnappableConnection', self.debug)
        valid_snaps = [s for s in all_snaps if s.get('id') and not s.get('isExpired') and not s.get('isQuarantined')]
        if not valid_snaps:
            raise GqlError(f"No valid snapshots found for workload ID: {workload_id}.")
        latest_snap = max(valid_snaps, key=lambda s: datetime.strptime(s['date'], '%Y-%m-%dT%H:%M:%S.%fZ'))
        return latest_snap

class AzureLocalRecovery(RecoveryObject):
    """Handles Azure Local VM recovery."""
    
    TYPE = "AZURE_LOCAL_VM"

    def initiate_mount(self, inventory, settings):
        logging.info(f"Preparing mount for Azure Local VM: '{self.name}'")
        target_vm = next((vm for vm in inventory if vm.get('name') == self.name), None)
        if not target_vm:
            raise ValueError(f"Azure Local VM '{self.name}' not found in Rubrik inventory.")

        vm_id = target_vm['id']
        cluster_id = target_vm['cluster']['id']

        snapshot = self._get_latest_valid_snapshot(vm_id)
        snapshot_id = snapshot['id']
        logging.info(f"  Using latest snapshot from: {snapshot['date']}")

        available_hosts = self._get_connected_hosts(cluster_id)
        if not available_hosts:
            raise ValueError(f"No connected Hyper-V hosts found for cluster ID {cluster_id}.")
        
        selected_host = random.choice(available_hosts)
        host_id = selected_host['id']
        
        self.mount_name = self.name
        if settings.get('rename_vms'):
            self.mount_name = generate_mount_name(self.name)
            print(f"    🏷️  [RENAME] Azure Local VM '{self.name}' will be mounted as '{self.mount_name}'.")
        
        logging.info(f"  Will be mounted as '{self.mount_name}' on host '{selected_host['name']}'.")

        mutation = """
            mutation CreateHypervMount($input: CreateHypervVirtualMachineSnapshotMountInput!) {
                createHypervVirtualMachineSnapshotMount(input: $input) { id }
            }
        """
        variables = {"input": {"id": snapshot_id, "config": { "hostId": host_id, "vmName": self.mount_name, "powerOn": True, "removeNetworkDevices": True }}}
        result = execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug)
        
        if not result.get('createHypervVirtualMachineSnapshotMount', {}).get('id'):
            raise GqlError(f"Mount initiation failed for '{self.name}'. API Response: {result}")
        
        self.status = "INITIATED"
        logging.info(f"  Successfully initiated mount for '{self.name}'.")

    def _get_connected_hosts(self, cluster_id):
        query = """
            query GetHyperVHosts($filters: [Filter!]) {
                hypervServersPaginated(filter: $filters) { nodes{ id name status { connectivity } } }
            }
        """
        variables = {"filters": [{"field": "IS_REPLICATED", "texts": ["false"]}, {"field": "IS_RELIC", "texts": ["false"]}, {"field": "CLUSTER_ID", "texts": [cluster_id]}]}
        all_servers = execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('hypervServersPaginated', {}).get('nodes', [])
        return [s for s in all_servers if s.get('status', {}).get('connectivity') == "Connected"]

    def check_status(self):
        query = """
            query CheckHypervMountStatus($filters: [HypervLiveMountFilterInput!]) {
                hypervMounts(first: 1, filters: $filters) { nodes { id, mountedVmStatus } }
            }
        """
        variables = {"filters": [{"field": "MOUNT_NAME", "texts": [self.mount_name]}]}
        data = execute_graphql_query(self.token, self.base_url, query, variables, self.debug)
        nodes = data.get('hypervMounts', {}).get('nodes', [])
        
        if not nodes:
            self.status = "MOUNTING"
            return self.status

        mount = nodes[0]
        self.status = mount.get('mountedVmStatus', 'MOUNTING')
        if self.status == 'POWEREDON':
            self.mount_id = mount.get('id')
        
        return self.status

    def unmount(self):
        if not self.mount_id:
            logging.warning(f"Could not find a completed mount ID for '{self.mount_name}' to unmount. The mount may have failed to create.")
            return False
        
        logging.info(f"Unmounting Azure Local VM '{self.mount_name}' with mount ID: {self.mount_id}...")
        print(f"🧹 [STATUS] Initiating unmount for Azure Local VM '{self.mount_name}'...")
        mutation = "mutation Unmount($input:DeleteHypervVirtualMachineSnapshotMountInput!){deleteHypervVirtualMachineSnapshotMount(input:$input){error{message}}}"
        variables = {"input": {"id": self.mount_id, "force": True}}
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug).get('deleteHypervVirtualMachineSnapshotMount', {})
                if result and result.get('error') is None:
                    logging.info(f"  Successfully initiated unmount for '{self.mount_name}'.")
                    return True
                
                error_msg = result.get('error', {}).get('message', 'Unknown error.')
                logging.warning(f"  Attempt {attempt + 1} to unmount '{self.mount_name}' failed. Reason: {error_msg}. Retrying in 10s...")
            except GqlError as e:
                logging.warning(f"  Attempt {attempt + 1} to unmount '{self.mount_name}' failed with an exception: {e}. Retrying in 10s...")

            if attempt < max_retries - 1:
                time.sleep(10)

        logging.error(f"  All {max_retries} unmount attempts failed for '{self.mount_name}'.")
        return False

class OracleRecovery(RecoveryObject):
    """Handles Oracle Database recovery."""

    TYPE = "ORACLE_DB"

    def initiate_mount(self, inventory, settings):
        logging.info(f"Preparing mount for Oracle DB: '{self.name}'")
        target_db = next((db for db in inventory if db.get('name') == self.name), None)
        if not target_db:
            raise ValueError(f"Oracle DB '{self.name}' not found in Rubrik inventory.")
            
        recovery_host = self.config.get('recovery_target_hostname')
        if not recovery_host:
            raise ValueError("Recovery plan is missing 'recovery_target_hostname'.")

        db_id = target_db['id']
        cluster_id = target_db['cluster']['id']

        snapshot = self._get_latest_oracle_snapshot(db_id)
        logging.info(f"  Using latest snapshot from: {snapshot['date']}")

        all_hosts = self._get_oracle_hosts(cluster_id)
        target_host = next((h for h in all_hosts if h.get('name') == recovery_host), None)
        if not target_host:
            raise ValueError(f"Designated recovery host '{recovery_host}' was not found or is not available.")

        config = {
            "targetOracleHostOrRacId": target_host['id'],
            "shouldMountFilesOnly": False,
            "recoveryPoint": {"snapshotId": snapshot['id']},
            "shouldSkipDropDbInUndo": False
        }

        if settings.get('rename_oracle_dbs'):
            self.mount_name = generate_oracle_mount_name(self.name)
            print(f"    🏷️  [RENAME] Oracle DB '{self.name}' will be mounted as '{self.mount_name}'.")
            config['mountedDatabaseName'] = self.mount_name
            config['shouldAllowRenameToSource'] = False
        else:
            self.mount_name = self.name
            if self.config.get('hostname') == recovery_host:
                raise ValueError("Cannot mount Oracle DB to source host without renaming. Enable renaming or choose a different target.")
            config['shouldAllowRenameToSource'] = True
        
        logging.info(f"  Will be mounted as '{self.mount_name}' on host '{target_host['name']}'.")
        
        mutation = """
            mutation OracleMount($input: MountOracleDatabaseInput!) {
                mountOracleDatabase(input: $input) { id }
            }
        """
        variables = {"input": {"request": {"id": db_id, "config": config}, "advancedRecoveryConfigMap": []}}
        result = execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug)

        if not result.get('mountOracleDatabase', {}).get('id'):
            raise GqlError(f"Oracle mount initiation failed for '{self.name}'. API Response: {result}")
        
        self.status = "INITIATED"
        logging.info(f"  Successfully initiated mount for '{self.name}'.")

    def _get_latest_oracle_snapshot(self, db_id):
        query = "query GetSnap($fid: UUID!){oracleDatabase(fid:$fid){newestSnapshot{id date isExpired isQuarantined}}}"
        data = execute_graphql_query(self.token, self.base_url, query, {"fid": db_id}, self.debug).get('oracleDatabase', {})
        snapshot = data.get('newestSnapshot')
        if not snapshot or snapshot.get('isExpired') or snapshot.get('isQuarantined'):
            raise GqlError(f"No valid newest snapshot found for DB ID {db_id}")
        return snapshot

    def _get_oracle_hosts(self, cluster_id):
        query = "query GetHosts($f:[Filter!]){oracleTopLevelDescendants(filter:$f){nodes{id name objectType cluster{id name}}}}"
        variables = {"f": [{"field": "IS_RELIC", "texts": ["false"]}, {"field": "IS_REPLICATED", "texts": ["false"]}]}
        all_desc = execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('oracleTopLevelDescendants', {}).get('nodes', [])
        return [h for h in all_desc if h.get('cluster', {}).get('id') == cluster_id]

    def check_status(self):
        query = "query CheckMount($f:[OracleLiveMountFilterInput!]){oracleLiveMounts(first:1,filters:$f){nodes{id, status}}}"
        variables = {"f": [{"field": "NAME", "texts": [self.mount_name]}]}
        nodes = execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('oracleLiveMounts', {}).get('nodes', [])
        
        if not nodes:
            self.status = "MOUNTING"
            return self.status

        mount = nodes[0]
        self.status = mount.get('status', 'MOUNTING')
        if self.status == 'AVAILABLE':
            self.mount_id = mount.get('id')
        
        return self.status

    def unmount(self):
        if not self.mount_id:
            logging.warning(f"Could not find a completed mount ID for '{self.mount_name}' to unmount. The mount may have failed to create.")
            return False
        
        logging.info(f"Unmounting Oracle DB: '{self.mount_name}' (ID: {self.mount_id})...")
        print(f"🧹 [STATUS] Initiating unmount for Oracle DB '{self.mount_name}'...")
        mutation = "mutation Unmount($input:DeleteOracleMountInput!){deleteOracleMount(input:$input){id}}"
        variables = {"input": {"id": self.mount_id, "force": True}}
        try:
            execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug)
            logging.info(f"  Successfully initiated unmount for '{self.mount_name}'.")
            return True
        except GqlError as e:
            logging.error(f"  Failed to unmount '{self.mount_name}'. Reason: {e}")
            return False

class MSSQLRecovery(RecoveryObject):
    """Handles Microsoft SQL Server Database recovery."""

    TYPE = "MSSQL_DB"

    def initiate_mount(self, inventory, settings):
        logging.info(f"Preparing mount for SQL DB: '{self.name}'")
        source_loc = self.config.get('hostname')
        recovery_target = self.config.get('recovery_target_hostname')
        if not source_loc or not recovery_target:
            raise ValueError("Recovery plan for SQL DB must include 'hostname' and 'recovery_target_hostname'.")

        target_db = self._find_mssql_db(self.name, source_loc)
        if not target_db:
            raise ValueError(f"SQL DB '{self.name}' at location '{source_loc}' not found.")
        
        db_id = target_db['id']
        recovery_point = self._get_latest_snapshot_date(db_id)
        logging.info(f"  Using latest recovery point from: {recovery_point}")
        
        compatible_instances = self._get_compatible_instances(db_id, recovery_point)
        target_instance = self._find_target_instance(compatible_instances, recovery_target)
        if not target_instance:
            raise ValueError(f"Recovery instance '{recovery_target}' is not a compatible target.")
        
        self.mount_name = self.name
        if settings.get('rename_sql_dbs'):
            self.mount_name = generate_mount_name(self.name)
            print(f"    🏷️  [RENAME] SQL DB '{self.name}' will be mounted as '{self.mount_name}'.")

        logging.info(f"  Will be mounted as '{self.mount_name}' on instance '{target_instance['name']}'.")

        config = {
            "recoveryPoint": {"date": recovery_point},
            "targetInstanceId": target_instance['id'],
            "mountedDatabaseName": self.mount_name
        }
        mutation = """
            mutation MssqlMount($input: CreateMssqlLiveMountInput!) {
                createMssqlLiveMount(input: $input) { id }
            }
        """
        variables = {"input": {"id": db_id, "config": config}}
        result = execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug)
        
        if not result.get('createMssqlLiveMount', {}).get('id'):
            raise GqlError(f"SQL mount initiation failed for '{self.name}'. API Response: {result}")

        self.status = "INITIATED"
        logging.info(f"  Successfully initiated mount for '{self.name}'.")

    def _find_mssql_db(self, db_name, location):
        query = "query getDb($f:[Filter!]){mssqlDatabases(filter:$f){nodes{id name}}}"
        variables = {"f": [{"field": "NAME", "texts": [db_name]}, {"field": "LOCATION", "texts": [location]}, {"field": "IS_RELIC", "texts": ["false"]}]}
        nodes = execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('mssqlDatabases', {}).get('nodes', [])
        return nodes[0] if nodes else None

    def _get_latest_snapshot_date(self, db_id):
        query = "query getSnap($fid:UUID!){mssqlDatabase(fid:$fid){cdmNewestSnapshot{date}}}"
        snapshot = execute_graphql_query(self.token, self.base_url, query, {"fid": db_id}, self.debug).get('mssqlDatabase', {}).get('cdmNewestSnapshot')
        if not snapshot or 'date' not in snapshot:
            raise GqlError(f"No valid newest snapshot found for MSSQL DB {db_id}")
        return snapshot['date']

    def _get_compatible_instances(self, db_id, recovery_time):
        query = "query getInst($input:GetCompatibleMssqlInstancesV1Input!){mssqlCompatibleInstances(input:$input){data{id name rootProperties{rootName}}}}"
        variables = {"input": {"id": db_id, "recoveryType": "V1_GET_COMPATIBLE_MSSQL_INSTANCES_V1_REQUEST_RECOVERY_TYPE_MOUNT", "recoveryTime": recovery_time}}
        return execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('mssqlCompatibleInstances', {}).get('data', [])
        
    def _find_target_instance(self, instances, recovery_target_fqdn):
        for inst in instances:
            host = inst.get('rootProperties', {}).get('rootName')
            inst_name = inst.get('name')
            if recovery_target_fqdn == host or recovery_target_fqdn == f"{host}\\{inst_name}":
                return inst
        return None

    def check_status(self):
        query = "query checkMount($f:[MssqlDatabaseLiveMountFilterInput!]){mssqlDatabaseLiveMounts(filters:$f,first:1){nodes{fid, isReady}}}"
        variables = {"filters": [{"field": "MOUNTED_DATABASE_NAME", "texts": [self.mount_name]}]}
        nodes = execute_graphql_query(self.token, self.base_url, query, variables, self.debug).get('mssqlDatabaseLiveMounts', {}).get('nodes', [])
        
        if not nodes:
            self.status = 'MOUNTING'
            return self.status

        mount = nodes[0]
        if mount.get('isReady'):
            self.status = 'AVAILABLE'
            self.mount_id = mount.get('fid')
        else:
            self.status = 'MOUNTING'
            
        return self.status

    def unmount(self):
        if not self.mount_id:
            logging.warning(f"Could not find a completed mount ID for '{self.mount_name}' to unmount. The mount may have failed to create.")
            return False

        logging.info(f"Unmounting SQL DB: '{self.mount_name}' (ID: {self.mount_id})...")
        print(f"🧹 [STATUS] Initiating unmount for SQL DB '{self.mount_name}'...")
        mutation = "mutation Unmount($input:DeleteMssqlLiveMountInput!){deleteMssqlLiveMount(input:$input){id}}"
        variables = {"input": {"id": self.mount_id, "force": True}}
        try:
            execute_graphql_query(self.token, self.base_url, mutation, variables, self.debug)
            logging.info(f"  Successfully initiated unmount for '{self.mount_name}'.")
            return True
        except GqlError as e:
            logging.error(f"  Failed to unmount '{self.mount_name}'. Reason: {e}")
            return False

#
# --- Factory and Inventory Functions ---
#

RECOVERY_CLASSES = {
    AzureLocalRecovery.TYPE: AzureLocalRecovery,
    OracleRecovery.TYPE: OracleRecovery,
    MSSQLRecovery.TYPE: MSSQLRecovery
}

def recovery_object_factory(token, base_url, obj_config, debug_mode):
    """Creates a recovery object instance based on its type."""
    obj_type = obj_config.get('type')
    if obj_type in RECOVERY_CLASSES:
        return RECOVERY_CLASSES[obj_type](token, base_url, obj_config, debug_mode)
    raise ValueError(f"Unknown recovery object type: '{obj_type}'")

def cache_inventories(token, base_url, debug_mode):
    """Fetches and caches protected object inventories from Rubrik."""
    logging.info("Caching protected object inventories from Rubrik...")
    print("📦 [STATUS] Caching protected object inventories from Rubrik...")
    inv_start_time = time.time()
    inventories = {}

    vm_query = "query Vms($f:Int,$a:String){hypervVirtualMachines(filter:[{field:IS_RELIC,texts:[\"false\"]},{field:IS_REPLICATED,texts:[\"false\"]}],first:$f,after:$a){nodes{id name cluster{id name}}pageInfo{hasNextPage endCursor}}}"
    inventories[AzureLocalRecovery.TYPE] = get_all_paginated_nodes(token, base_url, vm_query, {}, 'hypervVirtualMachines', debug_mode)
    logging.info(f"  - Found {len(inventories[AzureLocalRecovery.TYPE])} protected Azure Local VMs.")

    oracle_query = "query Orcl($f:Int,$a:String){oracleDatabases(filter:[{field:IS_RELIC,texts:\"false\"},{field:IS_REPLICATED,texts:\"false\"}],first:$f,after:$a){nodes{id name cluster{id name}}pageInfo{hasNextPage endCursor}}}"
    inventories[OracleRecovery.TYPE] = get_all_paginated_nodes(token, base_url, oracle_query, {}, 'oracleDatabases', debug_mode)
    logging.info(f"  - Found {len(inventories[OracleRecovery.TYPE])} protected Oracle databases.")

    inv_duration = time.time() - inv_start_time
    logging.info(f"Inventories cached successfully in {inv_duration:.2f} seconds.")
    print(f"✅ [TIMER] Inventories cached successfully in {inv_duration:.2f} seconds.\n")
    return inventories

#
# --- Main Execution Logic ---
#

def create_sample_recovery_plan(filename):
    """Creates a sample recovery plan file if one doesn't exist."""
    if os.path.exists(filename):
        return
    logging.warning(f"'{filename}' not found. A sample file will be created.")
    sample_plan = {
      "recovery_plan_name": "Sample Auto-Generated Plan",
      "settings": {
          "debug_mode": False,
          "halt_on_error": True,
          "cleanup_after_run": True,
          "rename_vms": True,
          "rename_sql_dbs": True,
          "rename_oracle_dbs": True
      },
      "phases": [
        { "phase_number": 1, "description": "Core Databases",
          "objects_to_recover": [
              {"name": "SAMPLE_ORCL_DB", "type": "ORACLE_DB", "hostname": "orcl-source.domain.com", "recovery_target_hostname": "orcl-target.domain.com"},
              {"name": "SAMPLE_SQL_DB", "type": "MSSQL_DB", "hostname": "sql-source\\inst", "recovery_target_hostname": "sql-target\\inst"}
            ]
        },
        { "phase_number": 2, "description": "Application Servers",
          "objects_to_recover": [{"name": "SAMPLE_AZURE_VM_NAME", "type": "AZURE_LOCAL_VM"}]
        }
      ]
    }
    try:
        with open(filename, 'w') as f:
            json.dump(sample_plan, f, indent=2)
        logging.info(f"A sample '{filename}' has been created. Please edit it and run the script again.")
    except IOError as e:
        raise ConfigError(f"Could not create sample config file '{filename}': {e}")
    sys.exit(0)

def main():
    """Main function to orchestrate the recovery process."""
    # --- Initial Setup ---
    try:
        # Load configs and authenticate
        create_sample_recovery_plan('recovery-plan.json')
        plan = load_json_file('recovery-plan.json')
        creds = load_json_file('config.json')

        plan_settings = plan.get('settings', {})
        debug_mode = plan_settings.get('debug_mode', False)
        setup_logging(debug_mode)
        
        print(f"--- Starting Rubrik Recovery Plan: '{plan.get('recovery_plan_name', 'Unnamed Plan')}' ---")

        token = get_auth_token(creds['RUBRIK_CLIENT_ID'], creds['RUBRIK_CLIENT_SECRET'], creds['RUBRIK_BASE_URL'])
        inventories = cache_inventories(token, creds['RUBRIK_BASE_URL'], debug_mode)
    
    except (ConfigError, GqlError) as e:
        logging.critical(f"A critical error occurred during setup: {e}")
        sys.exit(1)

    # --- Phase Execution ---
    all_mounted_objects = []
    execution_failed = False
    
    phases = sorted(plan.get('phases', []), key=lambda p: p.get('phase_number', 0))
    
    # Start the overall timer just before the first phase
    script_start_time = time.time()

    for phase in phases:
        phase_start_time = time.time()
        phase_num = phase.get('phase_number')
        logging.info(f"--- Starting Phase {phase_num}: {phase.get('description', 'N/A')} ---")
        print(f"\n--- ▶️  Starting Phase {phase_num}: {phase.get('description', 'N/A')} ---")

        # 1. Initiation
        jobs_in_phase = []
        for obj_conf in phase.get('objects_to_recover', []):
            if "SAMPLE_" in obj_conf.get('name', '').upper():
                logging.warning(f"Skipping sample object '{obj_conf['name']}'. Please edit 'recovery-plan.json'.")
                continue
            try:
                recovery_obj = recovery_object_factory(token, creds['RUBRIK_BASE_URL'], obj_conf, debug_mode)
                recovery_obj.initiate_mount(inventories.get(recovery_obj.TYPE, []), plan_settings)
                jobs_in_phase.append(recovery_obj)
            except (ValueError, GqlError, KeyError) as e:
                logging.error(f"Failed to initiate mount for '{obj_conf.get('name')}': {e}", exc_info=debug_mode)
                if plan_settings.get('halt_on_error', True):
                    logging.critical("Halting execution due to error.")
                    execution_failed = True
                    break
        if execution_failed: break

        # 2. Monitoring
        if jobs_in_phase:
            logging.info(f"Monitoring {len(jobs_in_phase)} jobs for Phase {phase_num}...")
            active_jobs = list(jobs_in_phase)
            max_wait_seconds = 1800  # 30 minutes
            
            is_first_poll = True
            completed_messages = []

            while active_jobs:
                if time.time() - phase_start_time > max_wait_seconds:
                    timeout_msg = f"Phase {phase_num} timed out after {max_wait_seconds / 60} minutes."
                    print(f"\n❌ ERROR: {timeout_msg}", file=sys.stderr)
                    logging.error(timeout_msg)
                    execution_failed = True
                    break
                
                logging.info(f"--- Monitoring Update (Phase {phase_num}) ---")

                if not is_first_poll:
                    sys.stdout.write(f"\x1b[A" * (len(active_jobs) + len(completed_messages) + 1))
                
                print(f"--- ⌛ Monitoring Phase {phase_num} (Updated: {time.strftime('%H:%M:%S')}) ---")
                for msg in completed_messages:
                    print(msg)
                is_first_poll = False

                for job in active_jobs[:]:
                    try:
                        job.check_status()
                        
                        display_name = job.name
                        if job.name != job.mount_name and job.mount_name is not None:
                            display_name = f"{job.name} -> {job.mount_name}"

                        logging.info(f"  - {display_name:<65} ({job.TYPE:<15}): {job.status}")
                        print(f"  - {display_name:<65} ({job.TYPE:<15}): {job.status:<15}")

                        if (job.TYPE == AzureLocalRecovery.TYPE and job.status == 'POWEREDON') or \
                           (job.TYPE in [OracleRecovery.TYPE, MSSQLRecovery.TYPE] and job.status == 'AVAILABLE'):
                            
                            success_message = f"  - {display_name:<65} ({job.TYPE:<15}): ✅ SUCCESS          "
                            completed_messages.append(success_message)
                            logging.info(f"SUCCESS: Mount for '{job.name}' is available.")
                            all_mounted_objects.append(job)
                            active_jobs.remove(job)

                        elif job.status in ['FAILED', 'FAILURE', 'MOUNT_FAILED']:
                            failure_message = f"  - {display_name:<65} ({job.TYPE:<15}): ❌ FAILED ({job.status})"
                            completed_messages.append(failure_message)
                            logging.error(f"Mount job for '{job.name}' failed with status '{job.status}'.")
                            active_jobs.remove(job)
                            if plan_settings.get('halt_on_error', True):
                                execution_failed = True

                    except GqlError as e:
                        display_name = job.name
                        if job.name != job.mount_name and job.mount_name is not None:
                            display_name = f"{job.name} -> {job.mount_name}"
                        
                        failure_message = f"  - {display_name:<65} ({job.TYPE:<15}): ❌ ERROR (Check logs)"
                        completed_messages.append(failure_message)
                        logging.error(f"Error monitoring '{job.name}': {e}", exc_info=debug_mode)
                        active_jobs.remove(job)
                        if plan_settings.get('halt_on_error', True):
                            execution_failed = True
                
                if execution_failed: break
                if not active_jobs: break
                time.sleep(15)
            
            if execution_failed: break

        phase_duration = time.time() - phase_start_time
        phase_duration_str = f"{int(phase_duration // 60)}m {phase_duration % 60:.2f}s"
        logging.info(f"--- Phase {phase_num} Completed in {phase_duration_str} ---")
        print(f"\n--- ✅ Phase {phase_num} Completed ---")
        for msg in completed_messages:
            logging.info(msg.strip())
            print(msg)
        print(f"📊 [TIMER] Phase {phase_num} duration: {phase_duration_str}")

    # --- Final Summary ---
    if execution_failed:
        logging.critical("\nRecovery plan execution FAILED.")
        print("\n❌ [FAILURE] Recovery plan execution FAILED. Check logs for details.")
    else:
        logging.info("\n🎉🎉🎉 All recovery phases completed successfully! 🎉🎉🎉")
        print("\n🎉🎉🎉 [SUCCESS] All recovery phases completed successfully! 🎉🎉🎉")

    script_duration = time.time() - script_start_time
    duration_str = f"{int(script_duration // 60)}m {script_duration % 60:.2f}s"
    logging.info(f"Total recovery runtime: {duration_str}.")
    print(f"\n📊 [TIMER] Total recovery runtime: {duration_str}.")

    # --- Cleanup ---
    if plan_settings.get('cleanup_after_run', True):
        logging.info("\n--- Starting Cleanup Phase: Unmounting all live mounts ---")
        print("\n--- 🧹 Starting Cleanup Phase ---")
        if not all_mounted_objects:
            logging.info("No successful mounts to clean up.")
            print("🧹 [STATUS] No successful mounts to clean up.")
        for mount in all_mounted_objects:
            try:
                mount.unmount()
            except (GqlError, Exception) as e:
                logging.error(f"Failed during cleanup for '{mount.mount_name}': {e}")
        logging.info("--- Cleanup Phase Complete ---")
        print("--- ✅ Cleanup Phase Complete ---")
    else:
        logging.info("\nSkipping cleanup as per 'cleanup_after_run' setting.")
        print("\n--- ⏭️  Skipping Cleanup Phase ---")
        for mount in all_mounted_objects:
             logging.info(f"  - Mount active: '{mount.mount_name}' ({mount.TYPE})")

    sys.exit(1 if execution_failed else 0)


if __name__ == "__main__":
    # Suppress InsecureRequestWarning for self-signed certificates
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    main()
