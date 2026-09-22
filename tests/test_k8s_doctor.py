# -*- coding: utf-8 -*-
"""k8s_doctor.py 的离线回归测试（CI 用）。

设计原则
--------
* **不需要真实集群**：所有 K8s 对象都在内存中用 kubernetes client 的模型类构造；
* **不需要 API Key、不联网**：不调用大模型、不连 SMTP；
* **覆盖安全边界**：自愈的各道闸门、默认只读、没有集群时优雅报错而不是抛栈。

本地运行：
    python -m pip install -r requirements-dev.txt
    python -m pytest -q
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import pytest
from kubernetes import client

REPO_ROOT = Path(__file__).resolve().parent.parent

try:  # 正常情况由 pytest.ini 里的 pythonpath=. 注入仓库根目录
    import k8s_doctor as doctor
except ImportError:  # pragma: no cover - 兜底：测试文件被单独执行时
    sys.path.insert(0, str(REPO_ROOT))
    import k8s_doctor as doctor

ALL_REASONS: Sequence[str] = doctor.REQUIRED_FAILURE_REASONS + doctor.EXTENDED_FAILURE_REASONS


@pytest.fixture(autouse=True)
def clean_env(monkeypatch) -> None:
    """隔离本机环境变量，让"默认值/开关"类断言在本地与 CI 上结果一致。"""
    for name in (
        "DEEPSEEK_API_KEY",
        "DEEPSEEK_MODEL",
        "DEEPSEEK_BASE_URL",
        "K8S_NAMESPACE",
        "KUBECONFIG",
        "SMTP_HOST",
        "SMTP_PORT",
        "SMTP_USER",
        "SMTP_PASSWORD",
        "MAIL_TO",
        "MAIL_FROM",
        "HEAL_IMAGE",
        "HEAL_NAMESPACE",
    ):
        monkeypatch.delenv(name, raising=False)



# --------------------------------------------------------------------------- #
# 测试夹具：在内存里造 Pod（不接触集群）
# --------------------------------------------------------------------------- #

def make_pod(
    *,
    name: str = "broken-pod",
    namespace: str = "default",
    image: str = "nginx:does-not-exist",
    reason: Optional[str] = "ImagePullBackOff",
    phase: str = "Pending",
    restart_count: int = 0,
    exit_code: Optional[int] = None,
    owner_kind: Optional[str] = None,
    container_name: str = "broken-pod",
) -> client.V1Pod:
    """构造一个故障 Pod 对象（只用 kubernetes client 的模型类，不需要 apiserver）。"""
    waiting = (
        client.V1ContainerStateWaiting(reason=reason, message=f"Back-off pulling image {image}")
        if reason
        else None
    )
    terminated = (
        client.V1ContainerStateTerminated(exit_code=exit_code, reason=reason)
        if exit_code is not None
        else None
    )
    container_status = client.V1ContainerStatus(
        name=container_name,
        image=image,
        image_id="",
        ready=False,
        restart_count=restart_count,
        state=client.V1ContainerState(waiting=waiting, terminated=terminated),
    )
    owner_references = (
        [client.V1OwnerReference(api_version="apps/v1", kind=owner_kind, name="demo", uid="uid-1")]
        if owner_kind
        else None
    )
    return client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=name, namespace=namespace, owner_references=owner_references),
        spec=client.V1PodSpec(containers=[client.V1Container(name=container_name, image=image)]),
        status=client.V1PodStatus(phase=phase, container_statuses=[container_status]),
    )


def make_pod_json() -> Dict[str, Any]:
    """模拟 `kubectl get pod -o json` 的返回值（用于自愈清单生成测试）。"""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "broken-pod",
            "namespace": "default",
            "uid": "0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
            "resourceVersion": "123456",
            "creationTimestamp": "2026-09-22T16:00:00Z",
            "managedFields": [{"manager": "kubectl"}],
            "labels": {"app": "broken-pod"},
            "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": "{\"kind\":\"Pod\"}",
                "keep-me": "yes",
            },
        },
        "spec": {
            "nodeName": "desktop-control-plane",
            "containers": [{"name": "broken-pod", "image": "nginx:does-not-exist"}],
        },
        "status": {"phase": "Pending", "podIP": "10.244.0.7"},
    }


# --------------------------------------------------------------------------- #
# 1. 故障检测
# --------------------------------------------------------------------------- #

def test_detect_failures_finds_image_pull_backoff():
    assert doctor.detect_failures(make_pod(), ALL_REASONS) == ["ImagePullBackOff"]


def test_detect_failures_only_returns_watched_reasons():
    """--strict 场景：只监控需求里明确要求的三种状态。"""
    pod = make_pod(reason="CreateContainerConfigError")
    assert doctor.detect_failures(pod, doctor.REQUIRED_FAILURE_REASONS) == []
    assert doctor.detect_failures(pod, ALL_REASONS) == ["CreateContainerConfigError"]


def test_detect_failures_reports_failed_pod_phase():
    pod = make_pod(reason=None, phase="Failed", exit_code=1)
    assert "PodPhaseFailed" in doctor.detect_failures(pod, ALL_REASONS)


def test_detect_failures_returns_empty_for_healthy_pod():
    assert doctor.detect_failures(make_pod(reason=None, phase="Running"), ALL_REASONS) == []


def test_detect_failures_handles_pod_without_status():
    pod = make_pod()
    pod.status = None
    assert doctor.detect_failures(pod, ALL_REASONS) == []


def test_state_reason_reads_waiting_and_terminated():
    waiting = client.V1ContainerState(waiting=client.V1ContainerStateWaiting(reason="ErrImagePull"))
    terminated = client.V1ContainerState(
        terminated=client.V1ContainerStateTerminated(exit_code=137, reason="OOMKilled")
    )
    assert doctor.state_reason(waiting) == "ErrImagePull"
    assert doctor.state_reason(terminated) == "OOMKilled"
    assert doctor.state_reason(client.V1ContainerState()) is None


# --------------------------------------------------------------------------- #
# 2. 告警冷却 / 指纹
# --------------------------------------------------------------------------- #

def test_fingerprint_treats_image_pull_states_as_same_failure():
    """ErrImagePull ↔ ImagePullBackOff 的来回抖动不应绕过告警冷却。"""
    one = doctor.failure_fingerprint(make_pod(reason="ErrImagePull"), ["ErrImagePull"])
    two = doctor.failure_fingerprint(make_pod(reason="ImagePullBackOff"), ["ImagePullBackOff"])
    assert one == two


def test_fingerprint_ignores_restart_count():
    """CrashLoopBackOff 的 restartCount 会不断自增，指纹里不能带它，否则冷却形同虚设。"""
    young = doctor.failure_fingerprint(
        make_pod(reason="CrashLoopBackOff", restart_count=0), ["CrashLoopBackOff"]
    )
    old = doctor.failure_fingerprint(
        make_pod(reason="CrashLoopBackOff", restart_count=37), ["CrashLoopBackOff"]
    )
    assert young == old


def test_alarm_state_cooldown_suppresses_repeats():
    alarm = doctor.AlarmState(cooldown_seconds=600)
    fingerprint = doctor.failure_fingerprint(make_pod(), ["ImagePullBackOff"])
    assert alarm.should_analyze("default/broken-pod", fingerprint) is True
    assert alarm.should_analyze("default/broken-pod", fingerprint) is False
    other = doctor.failure_fingerprint(make_pod(reason="CrashLoopBackOff"), ["CrashLoopBackOff"])
    assert alarm.should_analyze("default/broken-pod", other) is True


def test_alarm_state_zero_cooldown_always_analyzes():
    alarm = doctor.AlarmState(cooldown_seconds=0)
    fingerprint = doctor.failure_fingerprint(make_pod(), ["ImagePullBackOff"])
    assert alarm.should_analyze("default/broken-pod", fingerprint) is True
    assert alarm.should_analyze("default/broken-pod", fingerprint) is True


# --------------------------------------------------------------------------- #
# 3. 自愈的次数 / 冷却守卫
# --------------------------------------------------------------------------- #

def test_heal_guard_stops_after_max_attempts():
    guard = doctor.HealGuard(max_attempts=2, cooldown=0)
    assert guard.allow("broken-pod")[0] is True
    guard.record("broken-pod")
    assert guard.allow("broken-pod")[0] is True
    guard.record("broken-pod")
    permitted, why = guard.allow("broken-pod")
    assert permitted is False
    assert "上限" in why


def test_heal_guard_respects_cooldown_window():
    guard = doctor.HealGuard(max_attempts=5, cooldown=900)
    assert guard.allow("broken-pod")[0] is True
    guard.record("broken-pod")
    permitted, why = guard.allow("broken-pod")
    assert permitted is False
    assert "冷却" in why
    assert guard.allow("other-pod")[0] is True  # 不同 Pod 互不影响


# --------------------------------------------------------------------------- #
# 4. 自愈清单生成（镜像注入）
# --------------------------------------------------------------------------- #

def test_image_pull_failing_containers_locates_target():
    assert doctor.image_pull_failing_containers(make_pod()) == ["broken-pod"]
    assert doctor.image_pull_failing_containers(make_pod(reason="CrashLoopBackOff")) == []


def test_build_healed_manifest_replaces_image_and_drops_server_fields():
    manifest, changes = doctor.build_healed_manifest(make_pod_json(), "nginx:latest", ["broken-pod"])
    assert changes == ["容器 broken-pod: nginx:does-not-exist -> nginx:latest"]
    assert manifest["spec"]["containers"][0]["image"] == "nginx:latest"
    assert manifest["spec"]["containers"][0]["imagePullPolicy"] == "IfNotPresent"
    # 服务端字段必须剔除，否则重新 apply 会报错
    assert "status" not in manifest
    assert "nodeName" not in manifest["spec"]
    for field in doctor._MANIFEST_DROP_METADATA_FIELDS:
        assert field not in manifest["metadata"], f"{field} 应被剔除"
    annotations = manifest["metadata"]["annotations"]
    assert "kubectl.kubernetes.io/last-applied-configuration" not in annotations
    assert annotations["keep-me"] == "yes"          # 其它注解保留
    assert manifest["metadata"]["labels"] == {"app": "broken-pod"}


def test_build_healed_manifest_does_not_mutate_input():
    raw = make_pod_json()
    doctor.build_healed_manifest(raw, "nginx:latest", ["broken-pod"])
    assert raw["spec"]["containers"][0]["image"] == "nginx:does-not-exist"
    assert raw["status"]["phase"] == "Pending"


def test_build_healed_manifest_only_touches_target_containers():
    raw = make_pod_json()
    raw["spec"]["containers"].append({"name": "sidecar", "image": "busybox:1.36"})
    manifest, changes = doctor.build_healed_manifest(raw, "nginx:latest", ["broken-pod"])
    images = {item["name"]: item["image"] for item in manifest["spec"]["containers"]}
    assert images == {"broken-pod": "nginx:latest", "sidecar": "busybox:1.36"}
    assert len(changes) == 1


def test_build_healed_manifest_reports_no_change_when_image_already_ok():
    """镜像本来就对（例如误报）时应返回空变更，让上层放弃无意义的重建。"""
    manifest, changes = doctor.build_healed_manifest(
        make_pod_json(), "nginx:does-not-exist", ["broken-pod"]
    )
    assert changes == []
    assert manifest["spec"]["containers"][0]["image"] == "nginx:does-not-exist"


def test_build_healed_manifest_keeps_existing_pull_policy():
    raw = make_pod_json()
    raw["spec"]["containers"][0]["imagePullPolicy"] = "Always"
    manifest, _ = doctor.build_healed_manifest(raw, "nginx:latest", [])
    assert manifest["spec"]["containers"][0]["imagePullPolicy"] == "Always"


# --------------------------------------------------------------------------- #
# 5. HTML 报告与邮件（只渲染/落盘，不联网、不发信）
# --------------------------------------------------------------------------- #

def test_markdown_to_html_renders_table_headings_and_code():
    markdown = (
        "## 故障结论\n\n"
        "| 项目 | 值 |\n| --- | --- |\n| 镜像 | nginx:does-not-exist |\n\n"
        "```bash\nkubectl describe pod broken-pod\n```\n\n"
        "- 第一条建议\n- 第二条建议\n"
    )
    html_doc = doctor.markdown_to_html(markdown)
    assert "<h2>故障结论</h2>" in html_doc
    assert "<table><thead><tr><th>项目</th><th>值</th></tr></thead>" in html_doc
    assert '<pre class="code"><code>kubectl describe pod broken-pod</code></pre>' in html_doc
    assert "<li>第一条建议</li>" in html_doc


def test_render_report_html_contains_summary_and_escapes_report_body():
    """报告正文里的 HTML 必须被转义，避免事件信息/日志注入邮件。"""
    html_doc = doctor.render_report_html(
        make_pod(),
        ["ImagePullBackOff"],
        "## 故障结论\n疑似镜像 tag 不存在。\n\n<script>alert(1)</script>",
        "deepseek-chat",
    )
    assert '<table class="meta">' in html_doc
    assert "故障状态：ImagePullBackOff" in html_doc
    assert "default/broken-pod" in html_doc
    assert "nginx:does-not-exist" in html_doc
    assert "deepseek-chat" in html_doc
    assert "<script>alert(1)</script>" not in html_doc
    assert "&lt;script&gt;" in html_doc


def test_save_html_report_writes_file_into_report_dir(tmp_path):
    args = doctor.parse_args(["--report-dir", str(tmp_path)])
    path = doctor.save_html_report(args, "default/broken-pod", "<html>报告</html>")
    assert path.exists()
    assert path.parent == tmp_path
    assert path.name.endswith("-default_broken-pod.html")  # 非法文件名字符已替换
    assert path.read_text(encoding="utf-8") == "<html>报告</html>"


def test_notify_by_email_is_noop_when_email_disabled(tmp_path, capsys):
    args = doctor.parse_args(["--report-dir", str(tmp_path)])
    args.email_enabled = False
    doctor.notify_by_email(args, make_pod(), ["ImagePullBackOff"], "报告正文", "deepseek-chat")
    assert list(tmp_path.glob("*.html")) == []
    assert capsys.readouterr().out == ""


def test_notify_by_email_dry_run_writes_report_without_smtp(tmp_path, monkeypatch, capsys):
    args = doctor.parse_args(["--email", "--email-dry-run", "--report-dir", str(tmp_path)])
    args.email_enabled = doctor.email_enabled(args)
    assert args.email_enabled is True

    def _explode(*_args, **_kwargs):
        raise AssertionError("--email-dry-run 不应该触碰 SMTP 配置")

    monkeypatch.setattr(doctor, "load_email_config", _explode)
    doctor.notify_by_email(args, make_pod(), ["ImagePullBackOff"], "## 故障结论", "deepseek-chat")

    reports = list(tmp_path.glob("*.html"))
    assert len(reports) == 1
    out = capsys.readouterr().out
    assert "已落盘" in out
    assert "dry-run" in out


def test_email_enabled_switches(monkeypatch):
    assert doctor.email_enabled(doctor.parse_args([])) is False
    assert doctor.email_enabled(doctor.parse_args(["--email"])) is True
    assert doctor.email_enabled(doctor.parse_args(["--email", "--no-email"])) is False
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    assert doctor.email_enabled(doctor.parse_args([])) is True          # 配了 SMTP_HOST 自动开启
    assert doctor.email_enabled(doctor.parse_args(["--no-email"])) is False


def test_load_email_config_requires_complete_credentials():
    assert doctor.load_email_config(doctor.parse_args([])) is None
    incomplete = doctor.parse_args(["--smtp-host", "smtp.qq.com", "--smtp-user", "u@qq.com"])
    assert doctor.load_email_config(incomplete) is None


def test_load_email_config_builds_ssl_config_for_465():
    args = doctor.parse_args(
        [
            "--smtp-host", "smtp.qq.com",
            "--smtp-user", "u@qq.com",
            "--smtp-password", "authorization-code",
            "--mail-to", "a@x.com, b@x.com",
        ]
    )
    config = doctor.load_email_config(args)
    assert config is not None
    assert (config.port, config.use_ssl, config.use_starttls) == (465, True, False)
    assert config.sender == "u@qq.com"
    assert config.recipients == ["a@x.com", "b@x.com"]


def test_load_email_config_uses_starttls_for_587():
    args = doctor.parse_args(
        [
            "--smtp-host", "smtp.qq.com",
            "--smtp-port", "587",
            "--smtp-user", "u@qq.com",
            "--smtp-password", "authorization-code",
        ]
    )
    config = doctor.load_email_config(args)
    assert config is not None
    assert (config.use_starttls, config.use_ssl) == (True, False)


def test_build_email_message_is_multipart_alternative():
    config = doctor.EmailConfig(
        host="smtp.qq.com",
        port=465,
        user="u@qq.com",
        password="authorization-code",
        sender="u@qq.com",
        recipients=["a@x.com", "b@x.com"],
        use_ssl=True,
        use_starttls=False,
        timeout=10,
    )
    message = doctor.build_email_message(
        config, "[K8s 故障告警] default/broken-pod ImagePullBackOff", "<p>HTML 报告</p>", "纯文本报告"
    )
    assert message.get_content_type() == "multipart/alternative"
    assert message["From"] == "u@qq.com"
    assert message["To"] == "a@x.com, b@x.com"
    assert "default/broken-pod" in str(message["Subject"])   # 读取时是解码后的明文
    raw = message.as_string()
    subject_line = next(line for line in raw.splitlines() if line.startswith("Subject:"))
    assert "=?utf-8?" in subject_line                        # 发信时按 RFC2047 编码，兼容各客户端
    assert [part.get_content_subtype() for part in message.get_payload()] == ["plain", "html"]


# --------------------------------------------------------------------------- #
# 6. 自愈安全闸门（默认只读 / 命名空间 / 故障类型 / 控制器托管）
# --------------------------------------------------------------------------- #

def _fail_if_kubectl_called(*_args, **_kwargs):
    raise AssertionError("该场景不应该调用 kubectl")


def test_maybe_heal_never_touches_cluster_without_heal_flag(monkeypatch, capsys):
    """不显式加 --heal 时，即使 Pod 已经镜像故障也绝不执行任何变更。"""
    monkeypatch.setattr(doctor, "run_kubectl", _fail_if_kubectl_called)
    args = doctor.parse_args([])
    doctor.maybe_heal(args, make_pod(), ["ImagePullBackOff"])
    assert capsys.readouterr().out == ""


def test_maybe_heal_skips_namespace_outside_whitelist(monkeypatch, capsys):
    monkeypatch.setattr(doctor, "run_kubectl", _fail_if_kubectl_called)
    args = doctor.parse_args(["--heal", "--heal-namespace", "default"])
    doctor.maybe_heal(args, make_pod(namespace="kube-system"), ["ImagePullBackOff"])
    out = capsys.readouterr().out
    assert "跳过" in out
    assert "命名空间" in out


def test_maybe_heal_skips_non_image_pull_failure(monkeypatch, capsys):
    monkeypatch.setattr(doctor, "run_kubectl", _fail_if_kubectl_called)
    args = doctor.parse_args(["--heal", "--heal-namespace", "default"])
    doctor.maybe_heal(args, make_pod(reason="CrashLoopBackOff"), ["CrashLoopBackOff"])
    out = capsys.readouterr().out
    assert "跳过" in out
    assert "镜像拉取类" in out


def test_maybe_heal_skips_controller_managed_pod(monkeypatch, capsys):
    """Deployment/StatefulSet 管理的 Pod 删了会按原镜像重建，所以只给建议不代删。"""
    monkeypatch.setattr(doctor, "run_kubectl", _fail_if_kubectl_called)
    args = doctor.parse_args(["--heal", "--heal-namespace", "default"])
    doctor.maybe_heal(args, make_pod(owner_kind="Deployment"), ["ImagePullBackOff"])
    out = capsys.readouterr().out
    assert "跳过" in out
    assert "Deployment/demo" in out


# --------------------------------------------------------------------------- #
# 7. 命令行默认值与 CLI 冒烟
# --------------------------------------------------------------------------- #

def test_parse_args_defaults_are_safe():
    """回归保护：默认必须是只读、单命名空间、带倒计时与次数上限。"""
    args = doctor.parse_args([])
    assert args.heal is False                                    # 默认不自愈
    assert args.email is False
    assert args.email_dry_run is False
    assert args.strict is False
    assert args.namespace == "default"
    assert args.heal_namespace == doctor.DEFAULT_HEAL_NAMESPACE
    assert args.heal_image == doctor.DEFAULT_HEAL_IMAGE
    assert args.heal_countdown == 5                              # 留出 Ctrl+C 取消的时间
    assert args.heal_max_attempts == 2
    assert args.heal_cooldown == 900
    assert args.report_dir == "reports"
    assert args.model == "deepseek-chat"


def _run_cli(args, extra_env=None):
    env = os.environ.copy()
    for name in ("DEEPSEEK_API_KEY", "KUBECONFIG", "SMTP_HOST", "HEAL_NAMESPACE"):
        env.pop(name, None)
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "k8s_doctor.py"), *args],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )


def test_cli_help_exits_zero():
    result = _run_cli(["--help"])
    assert result.returncode == 0
    assert "usage:" in result.stdout
    assert "--heal" in result.stdout
    assert "--email-dry-run" in result.stdout


def test_cli_without_cluster_fails_gracefully(tmp_path):
    """没有集群时要给中文提示 + 退出码 1，而不是抛一堆 traceback。"""
    result = _run_cli(["--once", "--no-llm"], {"KUBECONFIG": str(tmp_path / "no-such-kubeconfig")})
    assert result.returncode == 1
    combined = result.stdout + result.stderr
    assert "无法加载 Kubernetes 配置" in combined
    assert "Traceback" not in combined
