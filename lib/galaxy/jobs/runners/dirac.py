"""Job runner used to execute Galaxy jobs through DIRAC.
"""

import logging
import os.path
import uuid

from galaxy import model
from galaxy.job_execution.output_collect import default_exit_code_file
from galaxy.job_execution.compute_environment import (
    ComputeEnvironment,
)
from galaxy.jobs.command_factory import build_command
from galaxy.jobs.runners import (
    AsynchronousJobRunner,
    AsynchronousJobState,
)
from galaxy.util import specs

log = logging.getLogger(__name__)

from DIRAC.Core.Utilities.ReturnValues import isSError

DIRAC_PARAM_SPECS = dict(
    dirac_cfg=dict(map=specs.to_str_or_none, default="hello")
)

#import DIRAC
DIRAC_HEADER= """set -x
BASE_DIR="$PWD"
mkdir -p $BASE_DIR/{inner_job_dir}/metadata
"""

# See job status here:
# https://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/WorkloadManagement/Jobs/index.html#jobs
DIRAC_STATES_MAPPING = {
        "Submitting": model.Job.states.WAITING,
        "Received": model.Job.states.WAITING,
        "Checking": model.Job.states.WAITING,
        "Rescheduled": model.Job.states.QUEUED,
        "Waiting": model.Job.states.QUEUED,
        "Staging": model.Job.states.QUEUED,
        "Matched": model.Job.states.QUEUED,
        "Running": model.Job.states.RUNNING,
        "Stalled": model.Job.states.QUEUED,
        "Completing": model.Job.states.RUNNING,
        "Completed": model.Job.states.RUNNING,
        "Failed": model.Job.states.ERROR,
        "Done": model.Job.states.OK,
}

# Mostly Copy-pasted from ARC, Toberefined
class DIRACComputeEnvironment(ComputeEnvironment):
    def __init__(self, job_wrapper):
        self.job_wrapper = job_wrapper

        self.local_path_config = job_wrapper.default_compute_environment()

        self.path_rewrites_input_extra = {}
        self._working_directory = "."
        self._config_directory = "."
        self._home_directory = ""
        self._tool_dir = "./tool_dir/"
        self._tmp_directory = ""
        self._shared_home_dir = ""
        self._sep = ""
        self._version_path = ""

    def output_names(self):
        # Maybe this should use the path mapper, but the path mapper just uses basenames
        return self.job_wrapper.job_io.get_output_basenames()

    def input_path_rewrite(self, dataset):
        """
        ARC Jobs run in the ARC remote compute clusters workdir - not known to Galaxy at this point.
        But all input-files are all uploaded (by ARC) to this workdir, so a simple relative  path will work for all ARC jobs
        """
        return f"{str(self._working_directory)}/{str(dataset.get_display_name())}"

    def output_path_rewrite(self, dataset):
        """
        ARC Jobs run in the ARC remote compute clusters workdir - not known to Galaxy at this point.
        But all outputfiles are created in this workdir, so a simple relative  path will work for all ARC jobs
        """
        # return f'{str(self._working_directory)}/{str(dataset.get_file_name())}'
        return f"{str(dataset.get_file_name())}"

    def input_extra_files_rewrite(self, dataset):
        """TODO - find out what this is and if I need it"""
        input_path_rewrite = self.input_path_rewrite(dataset)
        remote_extra_files_path_rewrite = dataset_path_to_extra_path(input_path_rewrite)
        self.path_rewrites_input_extra[dataset.extra_files_path] = remote_extra_files_path_rewrite
        return remote_extra_files_path_rewrite

    def output_extra_files_rewrite(self, dataset):
        """TODO - find out what this is and if I need it"""
        output_path_rewrite = self.output_path_rewrite(dataset)
        remote_extra_files_path_rewrite = dataset_path_to_extra_path(output_path_rewrite)
        return remote_extra_files_path_rewrite

    def input_metadata_rewrite(self, dataset, metadata_val):
        """TODO - find out what this is and if I need it"""
        return None

    def unstructured_path_rewrite(self, parameter_value):
        """TODO - find out what this is and if I need it"""
        return self._working_directory

    def working_directory(self):
        return self._working_directory

    def env_config_directory(self):
        return self._config_directory

    def config_directory(self):
        return self._config_directory

    def new_file_path(self):
        return self._working_directory

    def sep(self):
        return self._sep

    def version_path(self):
        return self._version_path

    def tool_directory(self):
        return self._tool_dir

    def home_directory(self):
        return self._home_directory

    def tmp_directory(self):
        return self._tmp_directory

    def galaxy_url(self):
        return self.job_wrapper.get_destination_configuration("galaxy_infrastructure_url")

    def get_file_sources_dict(self):
        return self.job_wrapper.job_io.file_sources_dict


class DIRACJobRunner(AsynchronousJobRunner):
    """The DIRAC job runner."""

    def __init__(self, app, nworkers, **kwargs):
        """Start the job runner."""
        super().__init__(app, nworkers, runner_param_specs=DIRAC_PARAM_SPECS, **kwargs)
        #DIRAC.initialize(extra_config_files=["/home/ubuntu/diracos/etc/dirac.cfg"])
        #from DIRAC.Interfaces.API.Dirac import Dirac
        #self.dirac = Dirac()
        self.dirac = None

    def __command_line(self, job_wrapper):
        """ """
        command_line = job_wrapper.runner_command_line

        job_id = job_wrapper.get_id_tag()
        # Paths in DIRAC should be relative as we do not know yet were
        # the job will land in the filesystem of the remote node
        inner_job_dir = os.path.basename(job_wrapper.working_directory)
        job_file = AsynchronousJobState.default_job_file(job_wrapper.working_directory, job_id)
        exit_code_path = default_exit_code_file(job_wrapper.working_directory, job_id)
        job_script_props = {
            # need to trick as output will be in "metadata"
            "headers": DIRAC_HEADER.format(inner_job_dir=inner_job_dir),
            "command": command_line,
            "exit_code_path": exit_code_path,
            "working_directory": f"$BASE_DIR/{inner_job_dir}",
            "shell": job_wrapper.shell,
        }
        job_file_contents = self.get_job_file(job_wrapper, **job_script_props)
        self.write_executable_script(job_file, job_file_contents, job_io=job_wrapper.job_io)
        return job_file, exit_code_path

    def build_command_line(
        self,
        job_wrapper: "MinimalJobWrapper",
        include_metadata: bool = False,
        include_work_dir_outputs: bool = True,
        modify_command_for_container: bool = True,
        stream_stdout_stderr=False,
    ):
        """Add the remote_job_directory"""
        log.debug("BCL")
        container = self._find_container(job_wrapper)
        if not container and job_wrapper.requires_containerization:
            raise Exception("Failed to find a container when required, contact Galaxy admin.")
        # Fake a pulsar endpoint
        job_dir = os.path.basename(job_wrapper.working_directory)
        remote_command_params = {
            "pulsar_version": "fake",
            "script_directory": f"$BASE_DIR/{job_dir}"
        }
        return build_command(
            self,
            job_wrapper,
            include_metadata=include_metadata,
            include_work_dir_outputs=include_work_dir_outputs,
            modify_command_for_container=modify_command_for_container,
            container=container,
            stream_stdout_stderr=stream_stdout_stderr,
            remote_command_params=remote_command_params,
            remote_job_directory="."
        )

    def queue_job(self, job_wrapper):
        """Add a job to dirac's queue"""
        log.debug("queuing job")
        compute_environment = DIRACComputeEnvironment(job_wrapper)
        if not self.prepare_job(job_wrapper,
                                modify_command_for_container=True,
                                compute_environment=compute_environment):
            return

        job_file, exit_code_path = self.__command_line(job_wrapper)

        remote_job_directory = "."

        log.debug(f"job_wrapper {job_wrapper}")
        for x in dir(job_wrapper):
            try:
                log.debug(f"JW {x}: {getattr(job_wrapper, x)}")
            except:
                log.debug(f"Ignoring {x} as error")

        job_destination = job_wrapper.job_destination
        log.debug(f"{job_wrapper.job_destination} is the destination")

        tool_script = os.path.join(job_wrapper.working_directory, "tool_script.sh")
        #from DIRAC.Interfaces.API.Job import Job
        # or extensions, e.g. from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob for LHCb

        #dirac_job = Job(stdout="tool_stdout", stderr="tool_stderr")
        #dirac_job.setExecutable(tool_script)
        #result = self.dirac.submitJob(dirac_job, mode='local')
        result = {"NO"}
       # log.debug(f"Result: {result}")
        #if isSError(result):
        #    log.error(f"Job submission failure: {result['Message']}")
        #    #job_wrapper.fail("Boom")
        #    return

        #dirac_job_id = result['Value']
        dirac_job_id = 'foobar'
        log.debug(f"Job {dirac_job_id} submittted to DIRAC")
        job_wrapper.set_external_id(dirac_job_id)
        ajs = AsynchronousJobState(
                files_dir=job_wrapper.working_directory,
                job_wrapper=job_wrapper,
                job_id=dirac_job_id,
                job_destination=job_destination,
        )
        self.monitor_queue.put(ajs)

    def check_watched_item(self, job_state):
        """Monitor job in dirac"""
        dirac_job_id = job_state.job_id
        log.error(f"Checking job {dirac_job_id}")
        result = self.dirac.getJobStatus(dirac_job_id)
        if isSError(result):
            log.debug(f"Cannot retrieve status of job {dirac_job_id}: {result['Message']}")
            return None
        log.debug(f"Result: {result}")
        if dirac_job_id not in result['Value']:
            log.debug(f"Job {dirac_job_id} is no longer in DIRAC? Ignoring!")
            return None
        dirac_status = result['Value'][dirac_job_id]
        job_status = dirac_status['Status']
        mapped_status = DIRAC_STATES_MAPPING.get(job_status, None)
        job_state.running = (mapped_status == model.Job.states.RUNNING)
        if job_state.job_wrapper.get_state() != mapped_status:
            job_state.job_wrapper.change_state(mapped_status)
        if job_status == "Failed":
            # XXX this should be changed
            job_state.job_wrapper.fail(dirac_status.get('MinorApplication', ""))
        if job_status in ["Failed", "Done"]:
            # no more job checking as this is terminal
            log.error("NO NO NO NO")
            return None
        log.error("YES")
        return job_state

    def stop_job(self, job_wrapper):
        """Stop an existing job"""
        job = job_wrapper.get_job()
        if not job.job_runner_external_id:
            return
        job_id = job.job_runner_external_id
        log.error(f"Attempt to stop DIRAC job {job_id}")

    def recover(self, job, job_wrapper):
        ajs = AsynchronousJobState(
                files_dir=job_wrapper.working_directory,
                job_wrapper=job_wrapper
        )
        ajs.job_id = job.get_job_runner_external_id()
        log.error(f"RECOVERING JOB {job.get_job_runner_external_id()}")
        self.monitor_queue.put(ajs)
