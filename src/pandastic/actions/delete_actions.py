import re

def get_ruleids_to_delete(did, rses_to_delete_from, rse_regexes, scope, didcl):
    '''
    Method to get the rule IDs to delete for a dataset and associated RSEs

    Parameters
    ----------
    did: str
        dataset name
    rses_to_delete_from: list
        list of RSEs to delete from
    rse_regexes: list
        list of RSE regexes to delete from
    scope: str
        scope of the dataset
    didcl: rucio.client.didclient.DIDClient
        Rucio DID client
    Returns
    -------
    rule_ids_rses_zip: generator
        zipped list of rule IDs and RSEs to delete from
    '''

    ruleids_to_delete = []
    found_rses_to_delete_from = []

    # Get the rules for the dataset
    rules = list(didcl.list_did_rules(scope, did.replace('/','')))
    # Get the rule IDs to delete
    for rule in rules:
        rse_for_rule = rule.get('rse_expression')
        rule_id = rule['id']
        if rse_for_rule in rses_to_delete_from or any(re.match(rse_rgx, rse_for_rule) for rse_rgx in rse_regexes):
            ruleids_to_delete.append(rule_id)
            found_rses_to_delete_from.append(rse_for_rule)

    rule_ids_rses_zip = zip(ruleids_to_delete,found_rses_to_delete_from)
    return rule_ids_rses_zip

def delete_rule(ruleid, rulecl):
    '''
    Method to delete a rule for a dataset

    Parameters
    ----------
    ruleid: str
        rule ID to delete
    rulecl: rucio.client.ruleclient.RuleClient
        Rucio rule client

    Returns
    -------
    ruleid: str
        The rule ID if the rule was successfully deleted, else None
    '''
    try:
        rulecl.delete_replication_rule(ruleid, purge_replicas=True)
        return True
    except:
        print(f"WARNING:: Rule deletion failed for rule ID {ruleid} ...  skipping!")
        return False