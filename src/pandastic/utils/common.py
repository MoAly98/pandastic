#!python3

from collections import defaultdict
import re
# =============================================================
# ========================  Classes  ==========================
# =============================================================
class RulesAndReplicasReq:
    '''
    Class to hold the rules and replicas requirements.
    '''
    def __init__(self, rule_on_rse, replica_on_rse, rule_or_replica_on_rse, cont_rule_req, norulehistory_on_rse):
        self.rule_on_rse = rule_on_rse
        self.replica_on_rse = replica_on_rse
        self.rule_or_replica_on_rse = rule_or_replica_on_rse
        self.cont_rule_req = cont_rule_req
        self.norulehistory_on_rse = norulehistory_on_rse

    def __repr__(self):
        return f"RulesAndReplicasReq(rule_on_rse={self.rule_on_rse}, replica_on_rse={self.replica_on_rse}, rule_or_replica_on_rse={self.rule_or_replica_on_rse}, cont_rule_req={self.cont_rule_req}, norulehistory_on_rse={self.norulehistory_on_rse})"
    def __str__(self):
        return f"RulesAndReplicasReq(rule_on_rse={self.rule_on_rse}, replica_on_rse={self.replica_on_rse}, rule_or_replica_on_rse={self.rule_or_replica_on_rse}, cont_rule_req={self.cont_rule_req}, norulehistory_on_rse={self.norulehistory_on_rse})"

# =============================================================
# =============== Methods for Rucio rule checking  =============
# =============================================================

def has_rule_on_rse(did, scope, rse, didcl):
    '''
    Method to check if a dataset has a rule on a given RSE

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_rule: bool
        True if the dataset has a rule on the RSE, False otherwise
    '''

    # Find existing rules for the given did
    rules    = list(didcl.list_did_rules(scope, did.replace('/','')))
    for rule in rules:
        if re.match(rse, rule.get("rse_expression")) is not None:
            return True
    # If here, no rule found on RSE
    return False

def has_replica_on_rse(did, scope, rse, replicacl):
    '''
    Method to check if a dataset has a replica on a given RSE. This
    is done by checking if all the files in the dataset have a replica
    on the RSE.

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_replica: bool
        True if the dataset has a replica on the RSE, False otherwise
    '''

    file_replicas = list(replicacl.list_replicas([{'scope': scope, 'name': did.replace('/','')}]))

    file_replica_is_on_req_rse = []

    for file_replica in file_replicas:
        fname = file_replica.get("name")
        file_rses = list(file_replica.get("rses").keys())
        if any(re.match(rse, frse) is not None for frse in file_rses):
            file_replica_is_on_req_rse.append(True)
        else:
            file_replica_is_on_req_rse.append(False)
    if all(file_replica_is_on_req_rse): return True
    else: return False

def has_rulehist_on_rse(did, scope, rse, rulecl):
    '''
    Method to check if a dataset has ever had a rule  on a given RSE
    by checking replication rule history.

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check
    replcl: rucio.client.replicaclient.ReplicaClient
        Replica client to use to get the list of replicas

    Returns
    -------
    has_rulehist: bool
        True if the dataset has ever had a rule on the RSE, False otherwise
    '''

    rulehist = list(rulecl.list_replication_rule_full_history(scope, did.replace('/','')))
    for rule in rulehist:
        if re.match(rse, rule.get("rse_expression")) is not None:
            return True

    # If here, no rule found on RSE
    return False


def get_rses_from_regex(rse_regex, rsecl):
    '''
    Method to get a list of RSEs from a list of RSEs that match a regex

    Parameters
    ----------
    rse_regex: str
        Regex to match RSEs against
    rsecl: rucio.client.rseclient.RSEClient
        RSE client to use to get the list of RSEs

    Returns
    -------
    matching_rses: list
        set of RSEs that match the regex
    '''

    matching_rses = set()

    available_rses = rsecl.list_rses()
    for avail_rse in available_rses:
        if re.match(rse_regex, avail_rse.get('rse')) is not None:
            matching_rses.add(avail_rse.get('rse'))
    return matching_rses