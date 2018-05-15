import os

from . import Runner


class Sge(Runner):
    MAX_CORES = 24
    MAX_MEM = 256 - 2

    def custom_spec(self, tool_spec, params, kwargs, tool_memory, tool_cores):
        raw_allocation_details = {}

        if 'cores' in tool_spec:
            kwargs['PARALLELISATION'] = '-pe "pe*" %s' % tool_cores
            # memory is defined per-core, and the input number is in gigabytes.
            real_memory = int(1024 * tool_memory / tool_spec['cores'])
            # Supply to kwargs with M for megabyte.
            kwargs['MEMORY'] = '%sM' % real_memory
            raw_allocation_details['mem'] = tool_memory
            raw_allocation_details['cpu'] = tool_cores

        if 'nativeSpecExtra' in tool_spec:
            kwargs['NATIVE_SPEC_EXTRA'] = tool_spec['nativeSpecExtra']

        # Large TMP dir
        if tool_spec.get('tmp', None) == 'large':
            kwargs['NATIVE_SPEC_EXTRA'] += '-l has_largetmp=1'

        # Environment variables, SGE specific.
        if 'env' in tool_spec and '_JAVA_OPTIONS' in tool_spec['env']:
            params['nativeSpecification'] = params['nativeSpecification'].replace('-v _JAVA_OPTIONS', '')
        return kwargs, raw_allocation_details, params

    def is_available(self):
        try:
            os.stat('/usr/local/galaxy/temporarily-disable-drmaa')
            return False
        except OSError:
            return True
