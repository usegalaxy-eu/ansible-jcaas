from galaxy.jobs import JobDestination
from .runners.condor import Condor
from .runners.sge import Sge
# from galaxy.jobs.mapper import JobMappingException

import backoff
import copy
import json
import logging
import math
import os
import requests
import subprocess
import time
import yaml

log = logging.getLogger(__name__)

# The default / base specification for the different environments.
SPECIFICATION_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'destination_specifications.yaml')
with open(SPECIFICATION_PATH, 'r') as handle:
    SPECIFICATIONS = yaml.load(handle)

TOOL_DESTINATION_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tool_destinations.yaml')
with open(TOOL_DESTINATION_PATH, 'r') as handle:
    TOOL_DESTINATIONS = yaml.load(handle)

TRAINING_MACHINES = {}


def get_tool_id(tool_id):
    """
    Convert ``toolshed.g2.bx.psu.edu/repos/devteam/column_maker/Add_a_column1/1.1.0``
    to ``Add_a_column``

    :param str tool_id: a tool id, can be the short kind (e.g. upload1) or the long kind with the full TS path.

    :returns: a short tool ID.
    :rtype: str
    """
    if tool_id.count('/') == 0:
        # E.g. upload1, etc.
        return tool_id

    # what about odd ones.
    if tool_id.count('/') == 5:
        (server, _, owner, repo, name, version) = tool_id.split('/')
        return name

    # No idea what this is.
    log.warning("Strange tool ID (%s), runner was not sure how to handle it.\n", tool_id)
    return tool_id


def name_it(tool_spec):
    if 'cores' in tool_spec:
        name = '%scores_%sG' % (tool_spec.get('cores', 1), tool_spec.get('mem', 4))
    elif len(tool_spec.keys()) == 0 or (len(tool_spec.keys()) == 1 and 'runner' in tool_spec):
        name = '%s_default' % tool_spec.get('runner', 'sge')
    else:
        name = '%sG_memory' % tool_spec.get('mem', 4)

    if tool_spec.get('tmp', None) == 'large':
        name += '_large'

    if 'name' in tool_spec:
        name += '_' + tool_spec['name']

    return name


def build_spec(tool_spec):
    destination = tool_spec.get('runner', 'sge')

    env = dict(SPECIFICATIONS.get(destination, {'env': {}})['env'])
    params = dict(SPECIFICATIONS.get(destination, {'params': {}})['params'])
    # A dictionary that stores the "raw" details that went into the template.
    raw_allocation_details = {}

    if destination == 'sge':
        runner = Sge()
    elif 'condor' in destination:
        runner = Condor()

    # We define the default memory and cores for all jobs. This is
    # semi-internal, and may not be properly propagated to the end tool
    tool_memory = tool_spec.get('mem', 4)
    tool_cores = tool_spec.get('cores', 1)
    # We apply some constraints to these values, to ensure that we do not
    # produce unschedulable jobs, requesting more ram/cpu than is available in a
    # given location. Currently we clamp those values rather than intelligently
    # re-scheduling to a different location due to TaaS constraints.
    tool_memory = min(tool_memory, runner.MAX_MEM)
    tool_cores = min(tool_cores, runner.MAX_CORES)

    kwargs = {
        # Higher numbers are lower priority, like `nice`.
        'PRIORITY': tool_spec.get('priority', 128),
        'MEMORY': str(tool_memory) + 'G',
        'PARALLELISATION': "",
        'NATIVE_SPEC_EXTRA': "",
    }
    # Allow more human-friendly specification
    if 'nativeSpecification' in params:
        params['nativeSpecification'] = params['nativeSpecification'].replace('\n', ' ').strip()

    # We have some destination specific kwargs. `nativeSpecExtra` and `tmp` are only defined for SGE
    kw_update, raw_alloc_update, params_update = runner.custom_spec(tool_spec, params, kwargs, tool_memory, tool_cores)
    kwargs.update(kw_update)
    raw_allocation_details.update(raw_alloc_update)
    params.update(params_update)

    # Update env and params from kwargs.
    env.update(tool_spec.get('env', {}))
    env = {k: str(v).format(**kwargs) for (k, v) in env.items()}
    params.update(tool_spec.get('params', {}))
    params = {k: str(v).format(**kwargs) for (k, v) in params.items()}

    if destination == 'sge':
        runner_name = 'drmaa'
    elif 'condor' in destination:
        runner_name = 'condor'
    else:
        runner_name = 'local'

    env = [dict(name=k, value=v) for (k, v) in env.items()]
    return env, params, runner_name, raw_allocation_details


def _finalize_tool_spec(tool_id, user_roles, memory_scale=1.0):
    # Find the 'short' tool ID which is what is used in the .yaml file.
    tool = get_tool_id(tool_id)
    # Pull the tool specification (i.e. job destination configuration for this tool)
    tool_spec = copy.deepcopy(TOOL_DESTINATIONS.get(tool, {}))
    # Update the tool specification with any training resources that are available
    tool_spec.update(reroute_to_dedicated(tool_spec, user_roles))

    tool_spec['mem'] = tool_spec.get('mem', 4) * memory_scale

    # Only two tools are truly special.
    if tool_id == 'upload1':
        tool_spec = {
            'mem': 0.3,
            'runner': 'condor',
            'requirements': prefer_machines(['upload'], machine_group='upload'),
            'env': {
                'TEMP': '/data/1/galaxy_db/tmp/'
            }
        }
    elif tool_id == '__SET_METADATA__':
        tool_spec = {
            'mem': 0.3,
            'runner': 'condor',
            'requirements': prefer_machines(['metadata'], machine_group='metadata')
        }
    return tool_spec


def convert_condor_to_sge(tool_spec):
    # Send this to SGE
    tool_spec['runner'] = 'sge'
    # SGE does not support partials
    tool_spec['mem'] = int(math.ceil(tool_spec['mem']))
    return tool_spec


def convert_sge_to_condor(tool_spec):
    tool_spec['runner'] = 'condor'
    return tool_spec


def handle_downed_runners(tool_spec):
    # In the event that it was going to condor and condor is unavailable, re-schedule to sge
    avail_condor = condor_is_available()
    avail_drmaa = drmaa_is_available()

    if not avail_condor and not avail_drmaa:
        raise Exception("Both clusters are currently down")

    if tool_spec.get('runner', 'local') == 'condor':
        if avail_drmaa and not avail_condor:
            tool_spec = convert_condor_to_sge(tool_spec)

    elif tool_spec.get('runner', 'local') == 'sge':
        if avail_condor and not avail_drmaa:
            tool_spec = convert_condor_to_sge(tool_spec)

    return tool_spec


def _gateway(tool_id, user_roles, user_email, memory_scale=1.0):
    tool_spec = handle_downed_runners(_finalize_tool_spec(tool_id, user_roles, memory_scale=memory_scale))

    # Send special users to condor temporarily.
    if 'gx-admin-force-jobs-to-condor' in user_roles:
        tool_spec = convert_sge_to_condor(tool_spec)
    if 'gx-admin-force-jobs-to-drmaa' in user_roles:
        tool_spec = convert_condor_to_sge(tool_spec)

    if tool_id == 'echo_main_env':
        if user_email != 'hxr@informatik.uni-freiburg.de':
            raise Exception("Unauthorized")
        else:
            tool_spec = convert_sge_to_condor(tool_spec)

    # Now build the full spec
    env, params, runner, _ = build_spec(tool_spec)

    return env, params, runner, tool_spec


@backoff.on_exception(backoff.fibo,
                      # Parent class of all requests exceptions, should catch
                      # everything.
                      requests.exceptions.RequestException,
                      # https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                      jitter=backoff.full_jitter,
                      max_tries=8)
def _gateway2(tool_id, user_roles, user_email, memory_scale=1.0):
    payload = {
        'tool_id': tool_id,
        'user_roles': user_roles,
        'email': user_email,
    }
    r = requests.post('http://127.0.0.1:8090', data=json.dumps(payload), timeout=1, headers={'Content-Type': 'application/json'})
    data = r.json()
    return data['env'], data['params'], data['runner'], data['spec']


def gateway(tool_id, user, memory_scale=1.0):
    # And run it.
    if user:
        user_roles = [role.name for role in user.all_roles() if not role.deleted]
        email = user.email
    else:
        user_roles = []
        email = ''

    try:
        env, params, runner, spec = _gateway2(tool_id, user_roles, email, memory_scale=memory_scale)
    except requests.exceptions.RequestException:
        # We really failed, so fall back to old algo.
        env, params, runner, spec = _gateway(tool_id, user_roles, email, memory_scale=memory_scale)

    name = name_it(spec)
    return JobDestination(
        id=name,
        runner=runner,
        params=params,
        env=env,
        resubmit=[{
            'condition': 'any_failure',
            'destination': 'resubmit_gateway',
        }]
    )


def resubmit_gateway(tool_id, user):
    """Gateway to handle jobs which have been resubmitted once.

    We don't want to try re-running them forever so the ONLY DIFFERENCE in
    these methods is that this one doesn't include a 'resubmission'
    specification in the returned JobDestination
    """

    job_destination = gateway(tool_id, user, memory_scale=1.5)
    job_destination['resubmit'] = []
    job_destination['id'] = job_destination['id'] + '_resubmit'
    return job_destination


def toXml(env, params, runner, spec):
    name = name_it(spec)

    print('        <destination id="%s" runner="%s">' % (name, runner))
    for (k, v) in params.items():
        print('            <param id="%s">%s</param>' % (k, v))
    for k in env:
        print('            <env id="%s">%s</env>' % (k['name'], k['value']))
    print('        </destination>')
    print("")


if __name__ == '__main__':
    seen_destinations = []
    for tool in TOOL_DESTINATIONS:
        if TOOL_DESTINATIONS[tool] not in seen_destinations:
            seen_destinations.append(TOOL_DESTINATIONS[tool])

    for spec in seen_destinations:
        (env, params, runner, _) = build_spec(spec)
        toXml(env, params, runner, spec)
