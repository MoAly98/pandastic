import re
import datetime

def get_ruleids_to_update(did, rses_to_update_on, rse_regexes, scope, max_timetodeath, didcl):
    '''
    Method to get the rule IDs to update for a dataset and associated RSEs

    Parameters
    ----------
    did: str
        dataset name
    rses_to_update_on: list
        list of RSEs to update rules on
    rse_regexes: list
        list of RSE regexes to update rules on
    scope: str
        scope of the dataset
    didcl: rucio.client.didclient.DIDClient
        Rucio DID client

    Returns
    -------
    rule_ids_rses_zip: generator
        zipped list of rule IDs and RSEs to update on
    '''

    ruleids_to_update = []
    found_rses_to_update_on = []

    # Get the rules for the dataset
    rules = list(didcl.list_did_rules(scope, did.replace('/','')))
    # Get the rule IDs to delete
    for rule in rules:
        rse_for_rule = rule.get('rse_expression')
        rule_id = rule['id']
        rule_timetodeath = rule['expires_at']-datetime.datetime.now()
        if max_timetodeath is not None:
            if rule_timetodeath.total_seconds() >= float(max_timetodeath):
                print(f"INFO:: Rule {rule_id} on {rse_for_rule} has a lifetime of {rule_timetodeath.total_seconds()}s, which is more than the requested max lifetime of {max_timetodeath}s. Skipping rule.")
                continue
        rule_timetodeath = rule['expires_at']-datetime.datetime.now()

        if rse_for_rule in rses_to_update_on or any(re.match(rse_rgx, rse_for_rule) for rse_rgx in rse_regexes):
            ruleids_to_update.append(rule_id)
            found_rses_to_update_on.append(rse_for_rule)

    rule_ids_rses_zip = zip(ruleids_to_update,found_rses_to_update_on)

    return rule_ids_rses_zip

def update_rule(ruleid, lifetime, rulecl):
    '''
    Method to update a rule for a dataset

    Parameters
    ----------
    ruleid: str
        rule ID to update
    lifetime: int
        Lifetime of the rule
    rulecl: rucio.client.ruleclient.RuleClient
        Rucio rule client

    Returns
    -------
    ruleid: str
        The rule ID if the rule was successfully updated, else None
    '''

    try:
        rulecl.update_replication_rule(ruleid, {'lifetime': lifetime})
        return True
    except Exception as e:
        print(f"WARNING:: Rule update failed for rule ID {ruleid} ...  skipping!")
        return False

