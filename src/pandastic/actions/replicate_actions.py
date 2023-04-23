import rucio
import re

def add_rule(ds, rse, lifetime, scope, rulecl):
    '''
    Method to add a rule for a dataset

    Parameters
    ----------
    ds: str
        Dataset to add a rule for
    rse: str
        RSE to add the rule to
    lifetime: int
        Lifetime of the rule
    scope: str
        Scope of the dataset

    Returns
    -------
    rule: str
        The rule ID if the rule was successfully added, else None
    '''

    try:
        rule = rulecl.add_replication_rule([{'scope':scope, 'name': ds.replace('/','')}], 1, rse, lifetime = lifetime)
        print(f'INFO:: DS = {ds} \n RuleID: {rule[0]}')
        return rule[0]

    except rucio.common.exception.DuplicateRule as de:
        print(f"WARNING:: Duplication already done for \n {ds} \n to {rse} ...  skipping!")
        return None

    except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
        print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse} ...  skipping, try again later!")
        return None
