from pbs4py.job import PBSJob
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

# Path to the qstat bulk output fixture (real NAS qstat -fx output)
FIXTURE_PATH = Path(__file__).parent / "test_output_files" / "qstat_bulk_debug.txt"

# Job IDs in the fixture, in order of appearance
EXPECTED_JOB_IDS = [
    "127067.pbs06a.hsn.ath.nas.nasa.gov",
    "132609.pbs06a.hsn.ath.nas.nasa.gov",
    "132706.pbs06a.hsn.ath.nas.nasa.gov",
    "132707.pbs06a.hsn.ath.nas.nasa.gov",
    "132790.pbs06a.hsn.ath.nas.nasa.gov",
    "174557.pbs06a.hsn.ath.nas.nasa.gov",
    "174559.pbs06a.hsn.ath.nas.nasa.gov",
    "177054.pbs06a.hsn.ath.nas.nasa.gov",
    "178178.pbs06a.hsn.ath.nas.nasa.gov",
    "178186.pbs06a.hsn.ath.nas.nasa.gov",
]


def _load_fixture() -> str:
    return FIXTURE_PATH.read_text()


# ── _split_qstat_output_by_job tests ────────────────────────────────


class TestSplitQstatOutputByJob:
    """Test the _split_qstat_output_by_job static method."""

    def test_splits_into_correct_number_of_jobs(self):
        output = _load_fixture()
        sections = PBSJob._split_qstat_output_by_job(output)

        assert len(sections) == len(EXPECTED_JOB_IDS)

    def test_returns_correct_job_ids(self):
        output = _load_fixture()
        sections = PBSJob._split_qstat_output_by_job(output)

        job_ids = [jid for jid, _ in sections]
        assert job_ids == EXPECTED_JOB_IDS

    def test_each_section_has_lines(self):
        output = _load_fixture()
        sections = PBSJob._split_qstat_output_by_job(output)

        for job_id, lines in sections:
            assert len(lines) > 0, f"Section for {job_id} has no lines"

    def test_empty_output_returns_no_sections(self):
        sections = PBSJob._split_qstat_output_by_job("")
        assert sections == []

    def test_single_job_output(self):
        """A single Job Id: section is parsed correctly."""
        output = "Job Id: 12345\n\tJob_Name = test\n\tjob_state = Q\n"
        sections = PBSJob._split_qstat_output_by_job(output)

        assert len(sections) == 1
        assert sections[0][0] == "12345"

    def test_continuation_lines_are_preserved(self):
        """Lines starting with tab are kept as part of the job's lines."""
        output = _load_fixture()
        sections = PBSJob._split_qstat_output_by_job(output)

        # Find job 127067 — it has continuation lines for Error_Path
        for jid, lines in sections:
            if jid == "127067.pbs06a.hsn.ath.nas.nasa.gov":
                # Should have continuation lines that start with \t
                tab_lines = [l for l in lines if l.startswith("\t")]
                assert len(tab_lines) > 0, "Expected continuation lines in section"
                break


# ── from_ids_bulk tests ─────────────────────────────────────────────


class TestFromIdsBulk:
    """Test the from_ids_bulk class method with real qstat fixture data."""

    @patch("pbs4py.job.subprocess.run")
    def test_returns_correct_number_of_jobs(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        assert len(jobs) == len(EXPECTED_JOB_IDS)

    @patch("pbs4py.job.subprocess.run")
    def test_parses_job_names(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        name_map = {j.id: j.name for j in jobs}
        assert name_map["127067.pbs06a.hsn.ath.nas.nasa.gov"] == "ddes_ctu200"
        assert name_map["132609.pbs06a.hsn.ath.nas.nasa.gov"] == "0.2c_ctu200"
        assert name_map["132706.pbs06a.hsn.ath.nas.nasa.gov"] == (
            "unsteady_ctu50_from_freestream_aoa2.80"
        )
        assert name_map["177054.pbs06a.hsn.ath.nas.nasa.gov"] == "0.2c_ctu200"
        assert name_map["178186.pbs06a.hsn.ath.nas.nasa.gov"] == "0.4c_ctu200"

    @patch("pbs4py.job.subprocess.run")
    def test_parses_job_states(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        state_map = {j.id: j.state for j in jobs}
        # Running jobs
        assert state_map["177054.pbs06a.hsn.ath.nas.nasa.gov"] == "R"
        assert state_map["178186.pbs06a.hsn.ath.nas.nasa.gov"] == "R"
        # Finished jobs
        assert state_map["127067.pbs06a.hsn.ath.nas.nasa.gov"] == "F"
        assert state_map["132706.pbs06a.hsn.ath.nas.nasa.gov"] == "F"

    @patch("pbs4py.job.subprocess.run")
    def test_parses_queue(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        for job in jobs:
            assert job.queue == "long", f"Job {job.id} queue is not 'long'"

    @patch("pbs4py.job.subprocess.run")
    def test_parses_workdir(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Check a couple of workdirs
        workdir_map = {j.id: j.workdir for j in jobs}
        assert "ddes_ctu200" not in workdir_map["127067.pbs06a.hsn.ath.nas.nasa.gov"]
        assert "/nobackupp17/kejacob1/projects/rca/buffet/cases/oat15a/urans" in (
            workdir_map["132706.pbs06a.hsn.ath.nas.nasa.gov"]
        )

    @patch("pbs4py.job.subprocess.run")
    def test_parses_resource_select(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Job 177054: select = 6:ncpus=256:mpiprocs=256:model=tur_ath
        job_177054 = next(j for j in jobs if j.id.startswith("177054"))
        assert job_177054.model == "tur_ath"
        assert job_177054.requested_number_of_nodes == 6
        assert job_177054.ncpus_per_node == 256

    @patch("pbs4py.job.subprocess.run")
    def test_running_job_has_hostname(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Job 178186 is running — should have exec_host parsed
        job_178186 = next(j for j in jobs if j.id.startswith("178186"))
        assert job_178186.state == "R"
        assert job_178186.hostname != ""
        # Hostname is the first node in exec_host
        assert job_178186.hostname == "x1000c2s0b0n0"

    @patch("pbs4py.job.subprocess.run")
    def test_finished_job_with_exec_host_has_hostname(self, mock_run):
        """A finished job that ran should have hostname populated."""
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Job 127067 is finished and ran
        job_127067 = next(j for j in jobs if j.id.startswith("127067"))
        assert job_127067.state == "F"
        assert job_127067.hostname == "x1001c1s5b0n1"

    @patch("pbs4py.job.subprocess.run")
    def test_finished_job_without_exec_host_does_not_crash(self, mock_run):
        """Regression test: job 178178 is state F but never ran — no exec_host.
        This should not raise a KeyError."""
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        # This was the original bug — it raised KeyError: 'exec_host'
        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        job_178178 = next(j for j in jobs if j.id.startswith("178178"))
        assert job_178178.state == "F"
        assert job_178178.hostname == ""  # never ran, so no hostname

    @patch("pbs4py.job.subprocess.run")
    def test_finished_job_without_exec_host_has_no_walltime_used(self, mock_run):
        """A job that never ran should have None for walltime_used."""
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        job_178178 = next(j for j in jobs if j.id.startswith("178178"))
        assert job_178178.walltime_used is None
        assert job_178178.walltime_remaining is None

    @patch("pbs4py.job.subprocess.run")
    def test_running_job_walltime_used(self, mock_run):
        """A running job should have walltime_used populated."""
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Job 178186 is running
        job_178186 = next(j for j in jobs if j.id.startswith("178186"))
        assert job_178186.walltime_used is not None
        assert job_178186.walltime_requested is not None

    @patch("pbs4py.job.subprocess.run")
    def test_finished_job_walltime(self, mock_run):
        """A finished job that ran should have walltime_used."""
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        # Job 132706 finished successfully
        job_132706 = next(j for j in jobs if j.id.startswith("132706"))
        assert job_132706.walltime_used is not None
        assert job_132706.exit_status == 0

    @patch("pbs4py.job.subprocess.run")
    def test_exit_statuses(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        exit_map = {j.id: j.exit_status for j in jobs}
        # Successful completion
        assert exit_map["132609.pbs06a.hsn.ath.nas.nasa.gov"] == 0
        # Exceeded walltime
        assert exit_map["127067.pbs06a.hsn.ath.nas.nasa.gov"] == -29
        # Job that never ran (178178 has Exit_status = -3)
        assert exit_map["178178.pbs06a.hsn.ath.nas.nasa.gov"] == -3
        # Running jobs shouldn't have an exit status
        assert exit_map["177054.pbs06a.hsn.ath.nas.nasa.gov"] is None
        assert exit_map["178186.pbs06a.hsn.ath.nas.nasa.gov"] is None

    @patch("pbs4py.job.subprocess.run")
    def test_mtime_parsed(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = _load_fixture()
        mock_run.return_value = mock_result

        jobs = PBSJob.from_ids_bulk(EXPECTED_JOB_IDS)

        for job in jobs:
            assert job.mtime is not None, f"Job {job.id} mtime was not parsed"
            assert isinstance(job.mtime, datetime)

    @patch("pbs4py.job.subprocess.run")
    def test_empty_job_list_returns_empty(self, mock_run):
        jobs = PBSJob.from_ids_bulk([])
        assert jobs == []
        mock_run.assert_not_called()
