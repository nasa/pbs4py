from unittest.mock import Mock

import pytest

from pbs4py.slurm import SLURM


@pytest.mark.parametrize("memory", ["64G", "512M", "1T", "1024K", "2048", "0"])
def test_memory_request_is_written_once(tmp_path, memory):
    launcher = SLURM(mem=memory, profile_filename="")
    path = tmp_path / "memory.slurm"
    launcher.write_job_file(str(path), "memory", ["true"])

    requested_lines = path.read_text().splitlines()
    assert requested_lines.count(f"#SBATCH --mem={memory}") == 1

    launcher.mem = None
    launcher.write_job_file(str(path), "memory", ["true"])
    default_lines = path.read_text().splitlines()
    assert [line for line in requested_lines if not line.startswith("#SBATCH --mem=")] == default_lines


def test_memory_request_tracks_changes_and_can_be_cleared(tmp_path):
    launcher = SLURM(profile_filename="")
    path = tmp_path / "memory.slurm"

    for memory in [None, "64G", "0", "512M", None]:
        launcher.mem = memory
        launcher.write_job_file(str(path), "memory", ["true"])
        memory_lines = [line for line in path.read_text().splitlines() if line.startswith("#SBATCH --mem=")]
        expected = [] if memory is None else [f"#SBATCH --mem={memory}"]
        assert memory_lines == expected


def test_memory_request_preserves_other_optional_headers(tmp_path):
    launcher = SLURM(mem="64G", profile_filename="")
    launcher.account = "science"
    launcher.array_range = "1-3"
    launcher.mail_options = "END"
    launcher.mail_list = "test@example.org"
    launcher.nodelist = "node01"
    path = tmp_path / "memory.slurm"
    launcher.write_job_file(str(path), "memory", ["true"], dependency="123")

    lines = path.read_text().splitlines()
    assert "#SBATCH --mem=64G" in lines
    for option in ["--account=science", "--array=1-3", "--mail-type=END",
                   "--mail-user=test@example.org", "--dependency=afterok:123", "--nodelist=node01"]:
        assert f"#SBATCH {option}" in lines


def test_launch_passes_script_with_memory_request_to_submission(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    launcher = SLURM(mem="64G", profile_filename="")
    submit = Mock(return_value="Submitted batch job 123")
    monkeypatch.setattr(launcher, "_run_job", submit)

    assert launcher.launch("memory", ["true"], blocking=False) == "Submitted batch job 123"
    submit.assert_called_once_with("memory.slurm", False)
    assert (tmp_path / "memory.slurm").read_text().splitlines().count("#SBATCH --mem=64G") == 1
