from __future__ import annotations

from bench.robosemantic.protocol import (
    RSB_SUITES,
    build_shard_jobs,
    curobo_install_command,
    dp_checkpoint_requirement,
    missing_local_requirements,
    normalized_semantic_grounding,
    parse_result_text,
    pi05_checkpoint_requirement,
    rsb_eval_requirements_install_command,
    suite_data_requirements,
)


def test_suite_matrix_matches_paper_protocol():
    by_name = {suite.name: suite for suite in RSB_SUITES}

    assert set(by_name) == {
        "RSB-Math-4",
        "RSB-Math-10",
        "RSB-HardMath-4",
        "RSB-HardMath-10",
        "RSB-General-4",
        "RSB-General-10",
    }
    assert by_name["RSB-Math-4"].task_name == "rsb_math"
    assert by_name["RSB-Math-4"].eval_config == "rsb_math_train_500"
    assert by_name["RSB-Math-4"].choices == 4
    assert by_name["RSB-General-10"].task_name == "rsb_general_10blocks"
    assert by_name["RSB-General-10"].eval_config == "rsb_general_10blocks_test_500"
    assert by_name["RSB-General-10"].choices == 10


def test_normalized_semantic_grounding_matches_definition():
    assert normalized_semantic_grounding(tsr=0.25, gsr=1.0, choices=4) == 0.0
    assert normalized_semantic_grounding(tsr=0.10, gsr=1.0, choices=10) == 0.0
    assert normalized_semantic_grounding(tsr=1.0, gsr=1.0, choices=4) == 1.0
    assert normalized_semantic_grounding(tsr=0.0, gsr=0.0, choices=4) is None


def test_parse_result_text_extracts_tsr_gsr():
    parsed = parse_result_text(
        """
        Timestamp: now

        Instruction Type: unseen

        Task Success Rate: 0.125
        Grasp Success Rate: 0.5
        """
    )

    assert parsed["task_success_rate"] == 0.125
    assert parsed["grasp_success_rate"] == 0.5


def test_build_shard_jobs_fans_out_suites_and_shards():
    jobs = build_shard_jobs(
        suite_names=["RSB-Math-4", "RSB-General-10"],
        policy_name="DP",
        ckpt_setting="demo",
        run_id="smoke",
        episodes_per_suite=5,
        shards_per_suite=2,
        seed=3,
        policy_seed=9,
    )

    assert [job.suite.name for job in jobs] == [
        "RSB-Math-4",
        "RSB-Math-4",
        "RSB-General-10",
        "RSB-General-10",
    ]
    assert [job.episodes for job in jobs] == [3, 2, 3, 2]
    assert [job.episode_start for job in jobs] == [0, 3, 0, 3]
    assert len({job.job_id for job in jobs}) == 4
    assert jobs[0].seed == 3000
    assert jobs[1].seed == 3001
    assert [job.policy_seed for job in jobs] == [9, 9, 9, 9]


def test_suite_data_requirements_are_specific_to_rsb_task_family():
    by_name = {suite.name: suite for suite in RSB_SUITES}

    assert suite_data_requirements(by_name["RSB-Math-4"]) == ()
    assert suite_data_requirements(by_name["RSB-HardMath-4"]) == (
        "gsm8k/data/test.json",
        "data/rsb_math/rsb_math_train_500/scene_info.json",
    )
    assert suite_data_requirements(by_name["RSB-General-10"]) == (
        "mmluqa2/data/test.json",
        "mmluqa2/data/train.json",
    )


def test_dp_checkpoint_requirement_uses_policy_seed_not_shard_seed():
    by_name = {suite.name: suite for suite in RSB_SUITES}

    requirement = dp_checkpoint_requirement(
        suite=by_name["RSB-Math-4"],
        ckpt_setting="baseline",
        expert_data_num=50,
        checkpoint_num=600,
        policy_seed=0,
    )

    assert requirement == "policy/DP/checkpoints/rsb_math-baseline-50-0/600.ckpt"


def test_pi05_checkpoint_requirement_uses_openpi_layout():
    requirement = pi05_checkpoint_requirement(
        train_config_name="pi05_base_aloha_lora",
        model_name="robotwin_pi05_aloha_agilex_randomized_5tasks_step20000",
        checkpoint_id=20000,
    )

    assert (
        requirement
        == "policy/pi05/checkpoints/pi05_base_aloha_lora/"
        "robotwin_pi05_aloha_agilex_randomized_5tasks_step20000/20000/params"
    )


def test_rsb_eval_requirements_install_uses_uv_and_skips_description_deps():
    command = rsb_eval_requirements_install_command("/rsb")

    assert "uv pip install --system" in command
    assert "pip install -r script/requirements.txt" not in command
    assert "azure==4\\.0\\.0|azure\\-ai\\-inference" in command
    assert "openai" in command
    assert "wandb" in command
    assert "moviepy" in command
    assert "torchvision==0.19.1" in command
    assert "opencv-python==4.11.0.86" in command


def test_curobo_install_pins_cuda_arch_for_modal_image_build():
    command = curobo_install_command("/rsb")

    assert "cd /rsb/envs/curobo" in command
    assert "TORCH_CUDA_ARCH_LIST=8.9" in command
    assert "uv pip install --system -e ." in command


def test_missing_local_requirements_reports_absent_files(tmp_path):
    (tmp_path / "present").write_text("ok", encoding="utf-8")

    assert missing_local_requirements(tmp_path, ("present", "missing/file.txt")) == (
        "missing/file.txt",
    )
