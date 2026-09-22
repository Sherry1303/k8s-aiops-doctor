#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""k8s_doctor.py —— Kubernetes 故障自愈机器人（只读诊断 + 大模型分析）。

工作流程
--------
1. 使用 kubernetes 的 watch 模块，持续监听 default 命名空间下所有 Pod 的变化；
2. 一旦发现 Pod 处于 ImagePullBackOff / CrashLoopBackOff / Error 等异常状态，
   立即收集该 Pod 的详情（status / conditions / 容器状态 / spec）、关联 Event 和容器日志；
3. 把收集到的上下文交给大模型（默认 deepseek-chat，DeepSeek 官方 API）分析，
   提示词为："你是一个资深的 K8s 运维专家，请分析这个 Pod 的失败原因，并给出具体的修复建议。"；
4. 在终端打印大模型给出的失败原因分析和修复建议；
5. 生成报告后，可通过 SMTP 异步发送一封排版精美的 HTML 邮件（--email，后台线程发送，不阻塞监听）；
6. 可选开启自愈执行（--heal）：对 ImagePullBackOff 等镜像拉取类故障，自动完成
   “备份清单 -> 修改镜像 -> 删除旧 Pod -> 用修复后的清单重建 Pod”，并打印完整自愈日志。

安全约束（重要）
----------------
默认情况下本脚本是 **只读** 的；只有显式加上 --heal 时才会修改集群：
    * 只调用 list / get / watch / read_namespaced_pod_log 这些读接口；
    * 默认不调用 create / patch / replace / delete 等任何写接口；
    * 只有显式加上 --heal 才进入自愈模式，才可能调用 kubectl 执行删除/重建；
    * 即使开启 --heal，仍有多重安全限制：
        - 只对命名空间白名单（默认仅 default）内的 Pod 执行自愈；
        - 只处理镜像拉取类故障（ImagePullBackOff / ErrImagePull 等）；
        - 控制器（Deployment/StatefulSet）管理的 Pod 只给建议、不代删；
        - 自愈前打印 5 秒倒计时，可随时按 Ctrl+C 取消；
        - 同一个 Pod 有自愈次数上限与冷却时间，避免删除-重建死循环。
    * 所有修复建议也会打印到终端，是否执行最终由人类决定（Human-in-the-loop）。

快速使用
--------
    # PowerShell
    $env:DEEPSEEK_API_KEY = "sk-xxxxxxxx"
    python k8s_doctor.py                    # 持续监听 default 命名空间
    python k8s_doctor.py --once             # 只扫描一次当前已有的 Pod
    python k8s_doctor.py --once --no-llm    # 只采集上下文、不调用大模型（离线自检）
    python k8s_doctor.py --once --heal      # 自愈模式：诊断后自动删除并重建镜像故障 Pod
    python k8s_doctor.py --once --email     # 生成报告后异步发送 HTML 邮件

邮件告警所需环境变量（也可用同名命令行参数）：
    $env:SMTP_HOST = "smtp.qq.com"
    $env:SMTP_PORT = "465"
    $env:SMTP_USER = "your@qq.com"
    $env:SMTP_PASSWORD = "邮箱授权码"
    $env:MAIL_TO = "3243743383@qq.com"        # 可选，默认即为该地址
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import smtplib
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml  # 随 kubernetes 客户端一起安装（kybernetes -> pyyaml）
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from openai import OpenAI

# --------------------------------------------------------------------------- #
# 常量配置
# --------------------------------------------------------------------------- #

#: 需求中明确要求监控的故障状态
REQUIRED_FAILURE_REASONS: Tuple[str, ...] = (
    "ImagePullBackOff",
    "CrashLoopBackOff",
    "Error",
)

#: 顺带覆盖的其它常见故障状态（用 --strict 可以只监控上面三种）
EXTENDED_FAILURE_REASONS: Tuple[str, ...] = (
    "ErrImagePull",
    "InvalidImageName",
    "ImageInspectError",
    "CreateContainerConfigError",
    "CreateContainerError",
    "RunContainerError",
    "ContainerCannotRun",
    "OOMKilled",
    "DeadlineExceeded",
    "PodPhaseFailed",
)

#: 故障状态归一化：把会来回跳变的等价状态归为一类，避免"ErrImagePull ↔ ImagePullBackOff"
#: 这种抖动绕过告警冷却、被反复分析。
REASON_FAMILIES: Dict[str, str] = {
    "ErrImagePull": "ImagePullFailure",
    "ImagePullBackOff": "ImagePullFailure",
    "InvalidImageName": "ImagePullFailure",
    "ImageInspectError": "ImagePullFailure",
    "CrashLoopBackOff": "ContainerCrashLoop",
    "Error": "ContainerCrashLoop",
    "OOMKilled": "ContainerCrashLoop",
    "ContainerCannotRun": "ContainerCrashLoop",
    "DeadlineExceeded": "ContainerCrashLoop",
    "CreateContainerConfigError": "ContainerConfigError",
    "CreateContainerError": "ContainerCreateError",
    "RunContainerError": "ContainerCreateError",
    "PodPhaseFailed": "PodPhaseFailed",
}

#: 大模型系统提示词（按要求固定）
SYSTEM_PROMPT = "你是一个资深的 K8s 运维专家，请分析这个 Pod 的失败原因，并给出具体的修复建议。"

#: 交给大模型的用户提示词模板
USER_PROMPT_TEMPLATE = """下面是我从 Kubernetes 集群中采集到的故障 Pod 上下文信息，请完成诊断。

请严格按以下结构用中文输出 Markdown 报告：
1. 【故障结论】先用一两句话点明最可能的根因；
2. 【证据分析】结合 Events、容器状态（waiting/terminated 的 reason、exitCode、restartCount）和日志逐条说明推理依据，并指出哪些证据支持、哪些证据矛盾；
3. 【修复建议】给出可直接执行的修复步骤（kubectl 命令或 YAML 片段），并说明每条步骤的作用；
4. 【预防措施】给出避免该问题复发的建议；
5. 【风险提示】说明修复时需要关注的风险点（例如回滚、影响范围、是否需要重建 Pod/Deployment）。

注意：你只负责给出分析结论和修复建议，不要假设自己已经执行了任何变更。

===== 故障 Pod 上下文 =====
{context}
===== 上下文结束 ====="""

#: 终端输出分隔线宽度
SEP = "=" * 78
SUB_SEP = "-" * 78

#: 邮件告警默认收件人（可用环境变量 MAIL_TO 或 --mail-to 覆盖）
DEFAULT_MAIL_TO = os.environ.get("MAIL_TO", "3243743383@qq.com")

#: 自愈模式默认替换成的镜像（可用环境变量 HEAL_IMAGE 或 --heal-image 覆盖）
DEFAULT_HEAL_IMAGE = os.environ.get("HEAL_IMAGE", "nginx:latest")

#: 允许执行自愈的命名空间白名单（安全限制，默认只有 default）
DEFAULT_HEAL_NAMESPACE = os.environ.get("HEAL_NAMESPACE", "default")


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #

def log(message: str) -> None:
    """带时间戳的终端日志。"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def force_utf8_stdout() -> None:
    """Windows 控制台默认可能是 GBK，强制 UTF-8 输出避免中文/emoji 报错。"""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # pragma: no cover - 少数被重定向的流不支持
            pass


def fmt_time(value: Any) -> str:
    """把 kubernetes client 返回的 datetime 格式化成可读字符串。"""
    if value is None:
        return "-"
    if isinstance(value, datetime):
        try:
            return value.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def ts_key(value: Any) -> float:
    """用于排序的时间戳；无法解析时返回 0。"""
    if isinstance(value, datetime):
        try:
            return value.timestamp()
        except Exception:
            return 0.0
    return 0.0


# --------------------------------------------------------------------------- #
# 客户端初始化
# --------------------------------------------------------------------------- #

def build_k8s_client() -> client.CoreV1Api:
    """加载 kubeconfig 并返回 CoreV1Api；本地 Docker Desktop 场景优先 kubeconfig。"""
    try:
        config.load_kube_config()
    except Exception as exc:  # 本地没有 kubeconfig 时退化为集群内配置
        log(f"加载本地 kubeconfig 失败（{exc}），尝试集群内 ServiceAccount 配置 …")
        try:
            config.load_incluster_config()
        except Exception as inner:
            raise SystemExit(
                "[错误] 无法加载 Kubernetes 配置。\n"
                "  请确认 Docker Desktop 的 K8s 已启用、kubectl 可正常访问集群，"
                f"或设置 KUBECONFIG 环境变量。原始错误: {inner}"
            ) from inner
    return client.CoreV1Api()


def build_llm_client() -> OpenAI:
    """根据环境变量构造 DeepSeek 客户端（OpenAI 兼容接口）。"""
    api_key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit(
            "[错误] 未检测到环境变量 DEEPSEEK_API_KEY。\n"
            "  PowerShell: $env:DEEPSEEK_API_KEY = \"sk-xxxxxxxx\"\n"
            "  如需先验证采集逻辑，可加 --no-llm 参数跳过调用大模型。"
        )
    base_url = (os.environ.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").strip()
    return OpenAI(api_key=api_key, base_url=base_url)


# --------------------------------------------------------------------------- #
# 故障检测
# --------------------------------------------------------------------------- #

def iter_container_statuses(status: Any) -> List[Any]:
    """汇总普通容器 / init 容器 / 临时容器的状态对象。"""
    if status is None:
        return []
    groups = (
        status.container_statuses or [],
        status.init_container_statuses or [],
        getattr(status, "ephemeral_container_statuses", None) or [],
    )
    result: List[Any] = []
    for group in groups:
        result.extend(group)
    return result


def detect_failures(pod: "client.V1Pod", watched_reasons: Sequence[str]) -> List[str]:
    """返回该 Pod 命中的故障原因列表（没有故障时返回空列表）。"""
    hits: List[str] = []
    status = pod.status
    if status is None:
        return hits

    # Pod 整体已进入 Failed 阶段（例如容器以非 0 退出、被驱逐等）
    if status.phase == "Failed" and "PodPhaseFailed" in watched_reasons:
        hits.append("PodPhaseFailed")

    for cs in iter_container_statuses(status):
        for state in (getattr(cs, "state", None), getattr(cs, "last_state", None)):
            reason = state_reason(state)
            if reason and reason in watched_reasons and reason not in hits:
                hits.append(reason)
    return hits


def state_reason(state: Any) -> Optional[str]:
    """从 ContainerState 中提取 waiting / terminated 的 reason。"""
    if state is None:
        return None
    waiting = getattr(state, "waiting", None)
    if waiting is not None:
        return waiting.reason
    terminated = getattr(state, "terminated", None)
    if terminated is not None:
        return terminated.reason
    return None


def failure_fingerprint(pod: "client.V1Pod", reasons: Sequence[str]) -> str:
    """计算"同一类故障"的指纹，用于告警冷却判断。

    注意：
    * 故障状态先经 REASON_FAMILIES 归一化，所以 ErrImagePull / ImagePullBackOff
      的来回跳变视为同一个故障，不会被反复分析；
    * 指纹里 **不含** restartCount：CrashLoopBackOff 的容器重启次数会不断自增，
      如果带上它，冷却就形同虚设。重复告警统一由 --cooldown 控制。
    """
    status = pod.status
    family_set = {REASON_FAMILIES.get(reason, reason) for reason in reasons}
    families = "+".join(sorted(family_set))

    phase = "-"
    exits: List[str] = []
    if status is not None:
        phase = status.phase or "-"
        for cs in iter_container_statuses(status):
            terminated = getattr(getattr(cs, "last_state", None), "terminated", None)
            if terminated is None:
                terminated = getattr(getattr(cs, "state", None), "terminated", None)
            if terminated is not None:
                exits.append(f"{cs.name}:exit={terminated.exit_code}")

    detail = "/".join(sorted(set(exits))) or "no-exit-code"
    return f"{families}|phase={phase}|{detail}"


class AlarmState:
    """告警去重/冷却，避免同一个 Pod 的同一故障被反复分析（既省钱又少刷屏）。"""

    def __init__(self, cooldown_seconds: int, max_entries: int = 1000) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.max_entries = max_entries
        self._seen: Dict[str, Tuple[str, float]] = {}

    def should_analyze(self, key: str, fingerprint: str) -> bool:
        now = time.time()
        previous = self._seen.get(key)
        if previous is not None and previous[0] == fingerprint:
            if now - previous[1] < self.cooldown_seconds:
                return False
        self._seen[key] = (fingerprint, now)
        if len(self._seen) > self.max_entries:
            # 简单清理：丢掉最旧的一半记录
            ordered = sorted(self._seen.items(), key=lambda item: item[1][1])
            for old_key, _ in ordered[: len(ordered) // 2]:
                self._seen.pop(old_key, None)
        return True


# --------------------------------------------------------------------------- #
# 故障上下文采集（下面所有操作都是只读的 K8s API 调用）
# --------------------------------------------------------------------------- #

def describe_state(state: Any, label: str) -> str:
    """把 ContainerState 渲染成一行可读文本。"""
    if state is None:
        return f"{label}=<none>"
    waiting = getattr(state, "waiting", None)
    if waiting is not None:
        return f"{label}=waiting(reason={waiting.reason}, message={waiting.message})"
    terminated = getattr(state, "terminated", None)
    if terminated is not None:
        return (
            f"{label}=terminated(reason={terminated.reason}, exitCode={terminated.exit_code}, "
            f"signal={terminated.signal}, startedAt={fmt_time(terminated.started_at)}, "
            f"finishedAt={fmt_time(terminated.finished_at)}, message={terminated.message})"
        )
    running = getattr(state, "running", None)
    if running is not None:
        return f"{label}=running(startedAt={fmt_time(running.started_at)})"
    return f"{label}=<unknown>"


def describe_container_status(cs: Any, kind: str) -> List[str]:
    """渲染容器运行时状态。"""
    return [
        f"- [{kind}] {cs.name}",
        f"    image: {cs.image}",
        f"    imageID: {cs.image_id or '-'}",
        f"    ready: {cs.ready}  restartCount: {cs.restart_count}  started: {getattr(cs, 'started', None)}",
        f"    {describe_state(getattr(cs, 'state', None), 'state')}",
        f"    {describe_state(getattr(cs, 'last_state', None), 'lastState')}",
    ]


def describe_container_spec(container: Any, kind: str) -> List[str]:
    """渲染容器声明（镜像、启动命令、环境变量、资源限制等）。"""
    lines = [
        f"- [{kind}] {container.name}: image={container.image}  "
        f"imagePullPolicy={container.image_pull_policy}"
    ]
    if container.command:
        lines.append(f"    command: {container.command}")
    if container.args:
        lines.append(f"    args: {container.args}")
    if container.env:
        lines.append(f"    env(仅名称): {[item.name for item in container.env]}")
    if container.resources:
        lines.append(
            f"    resources: requests={container.resources.requests} "
            f"limits={container.resources.limits}"
        )
    if container.volume_mounts:
        lines.append(
            "    volumeMounts: "
            f"{[(item.name, item.mount_path, item.read_only) for item in container.volume_mounts]}"
        )
    return lines


def collect_events(core_api: client.CoreV1Api, namespace: str, pod_name: str, limit: int) -> List[str]:
    """读取与该 Pod 关联的 Event（只读接口 list_namespaced_event）。"""
    try:
        events = core_api.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod",
        ).items
    except ApiException as exc:
        return [f"<事件查询失败: HTTP {exc.status} {exc.reason}>"]
    except Exception as exc:  # pragma: no cover - 网络抖动等
        return [f"<事件查询失败: {type(exc).__name__}: {exc}>"]

    events.sort(
        key=lambda event: ts_key(
            event.last_timestamp or event.event_time or event.metadata.creation_timestamp
        ),
        reverse=True,
    )

    lines: List[str] = []
    for event in events[:limit]:
        source = event.source.component if event.source is not None else "-"
        lines.append(
            f"- [{fmt_time(event.last_timestamp or event.event_time)}] "
            f"{event.type}/{event.reason} x{event.count or 1}（source={source}）: {event.message}"
        )
    return lines or ["<该 Pod 没有关联 Event>"]


def collect_container_logs(
    core_api: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str,
    tail_lines: int,
    max_chars: int,
) -> str:
    """读取容器日志；当前实例拿不到就尝试 --previous（上一个崩溃实例）。"""
    last_message = "<该容器暂时没有日志>"
    for previous, label in ((False, "当前实例"), (True, "上一个崩溃实例")):
        try:
            text = core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container_name,
                previous=previous,
                tail_lines=tail_lines,
                timestamps=True,
            )
        except ApiException as exc:
            last_message = f"<{label}日志不可用: HTTP {exc.status} {exc.reason}>"
            continue
        except Exception as exc:  # pragma: no cover - 网络抖动等
            last_message = f"<{label}日志不可用: {type(exc).__name__}: {exc}>"
            continue

        if text and text.strip():
            if len(text) > max_chars:
                text = f"…（日志过长，仅保留最后 {max_chars} 个字符）\n" + text[-max_chars:]
            return text
        last_message = f"<{label}日志为空>"
    return last_message


def build_pod_context(
    core_api: client.CoreV1Api,
    pod: "client.V1Pod",
    reasons: Sequence[str],
    tail_lines: int,
    max_log_chars: int,
    events_limit: int = 30,
) -> str:
    """把 Pod 详情 + Event + 日志整理成一段 Markdown 文本，作为大模型的输入。"""
    md = pod.metadata
    status = pod.status
    spec = pod.spec
    namespace = md.namespace
    name = md.name

    lines: List[str] = []
    lines.append("### 1. Pod 基本信息")
    lines.append(f"- 名称: {namespace}/{name}")
    lines.append(f"- UID: {md.uid}")
    lines.append(f"- 所在节点: {spec.node_name or '<尚未调度>'}")
    lines.append(f"- 创建时间: {fmt_time(md.creation_timestamp)}")
    lines.append(f"- 当前时间: {fmt_time(datetime.now())}")
    lines.append(f"- 标签: {md.labels}")
    owners = (
        [(owner.kind, owner.name, owner.controller) for owner in md.owner_references]
        if md.owner_references
        else "无（裸 Pod，未被 Deployment/StatefulSet 等控制器管理）"
    )
    lines.append(f"- ownerReferences: {owners}")
    lines.append(f"- 命中的故障状态: {', '.join(reasons)}")
    if status is not None:
        lines.append(
            f"- phase={status.phase}  reason={status.reason or '-'}  message={status.message or '-'}"
        )
        lines.append(
            f"- QoS={status.qos_class or '-'}  PodIP={status.pod_ip or '-'}  "
            f"HostIP={status.host_ip or '-'}"
        )
    lines.append(f"- restartPolicy: {spec.restart_policy}")

    lines.append("")
    lines.append("### 2. 容器声明（spec）")
    for container in spec.containers:
        lines.extend(describe_container_spec(container, "container"))
    for container in spec.init_containers or []:
        lines.extend(describe_container_spec(container, "initContainer"))

    lines.append("")
    lines.append("### 3. 容器运行状态（status）")
    status_lines_before = len(lines)
    if status is not None:
        for cs in status.init_container_statuses or []:
            lines.extend(describe_container_status(cs, "initContainer"))
        for cs in status.container_statuses or []:
            lines.extend(describe_container_status(cs, "container"))
        for cs in getattr(status, "ephemeral_container_statuses", None) or []:
            lines.extend(describe_container_status(cs, "ephemeralContainer"))
    if len(lines) == status_lines_before:
        lines.append("- <暂无容器状态>")

    lines.append("")
    lines.append("### 4. Pod Conditions")
    conditions_added = False
    if status is not None:
        for cond in status.conditions or []:
            lines.append(
                f"- {cond.type}={cond.status}  reason={cond.reason}  "
                f"lastTransitionTime={fmt_time(cond.last_transition_time)}  message={cond.message}"
            )
            conditions_added = True
    if not conditions_added:
        lines.append("- <暂无 Conditions>")

    lines.append("")
    lines.append(f"### 5. 关联 Event（按时间倒序，最多 {events_limit} 条）")
    lines.extend(collect_events(core_api, namespace, name, events_limit))

    lines.append("")
    lines.append(f"### 6. 容器日志（每个容器最多保留 {max_log_chars} 字符）")
    log_targets: List[str] = [container.name for container in spec.init_containers or []]
    log_targets.extend(container.name for container in spec.containers)
    if not log_targets:
        lines.append("- <未声明任何容器>")
    for container_name in log_targets:
        lines.append(f"--- 容器 {container_name} 的日志 ---")
        lines.append(
            collect_container_logs(
                core_api, namespace, name, container_name, tail_lines, max_log_chars
            )
        )

    context = "\n".join(lines)
    return context


# --------------------------------------------------------------------------- #
# 大模型分析
# --------------------------------------------------------------------------- #

def analyze_with_llm(llm: OpenAI, model: str, context: str) -> str:
    """把上下文发给大模型，返回分析报告文本。"""
    response = llm.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(context=context)},
        ],
        temperature=0.2,
        stream=False,
    )
    if not response.choices:
        return "<大模型没有返回任何结果>"
    return response.choices[0].message.content or "<大模型返回内容为空>"


# --------------------------------------------------------------------------- #
# 诊断流程
# --------------------------------------------------------------------------- #

def diagnose_and_report(
    core_api: client.CoreV1Api,
    llm: Optional[OpenAI],
    args: argparse.Namespace,
    pod: "client.V1Pod",
    reasons: Sequence[str],
) -> None:
    """采集上下文 -> 调用大模型 -> 打印报告。全程只读，不修改集群。"""
    meta = pod.metadata
    pod_id = f"{meta.namespace}/{meta.name}"
    log(f"检测到故障 Pod: {pod_id} -> {', '.join(reasons)}，开始采集上下文 …")

    try:
        context = build_pod_context(
            core_api, pod, reasons, args.tail_lines, args.max_log_chars, args.events_limit
        )
    except Exception as exc:  # 采集失败不应中断监听
        log(f"采集 {pod_id} 上下文失败: {type(exc).__name__}: {exc}")
        return

    if llm is None:
        print("\n" + SUB_SEP)
        print(f"[采集结果] {pod_id}（--no-llm 模式：只采集，不调用大模型）")
        print(SUB_SEP)
        print(context)
        print(SUB_SEP + "\n", flush=True)
        report_text = f"（--no-llm 模式：以下为本地采集到的上下文，未经大模型分析）\n\n```\n{context}\n```"
        report_model = "<未调用大模型 --no-llm>"
    else:
        log(f"上下文采集完成（{len(context)} 字符），正在请求大模型 {args.model} 分析 …")
        try:
            report_text = analyze_with_llm(llm, args.model, context)
        except Exception as exc:
            log(f"大模型调用失败: {type(exc).__name__}: {exc}")
            report_text = (
                "【大模型调用失败】\n"
                f"错误信息: {type(exc).__name__}: {exc}\n"
                "排查建议：确认 DEEPSEEK_API_KEY 是否有效、网络是否可达 "
                "https://api.deepseek.com，或使用 --no-llm 先查看本地采集到的上下文。"
            )

        print("\n" + SEP)
        print(f"[AI 诊断报告] Pod: {pod_id} | 故障状态: {', '.join(reasons)} | 模型: {args.model}")
        print(SEP)
        print(report_text)
        print(SEP)
        if args.heal:
            print("提示：--heal 已开启，接下来对该 Pod 执行自愈（删除并用修复后的清单重建）。\n", flush=True)
        else:
            print("提示：以上仅为分析建议。本脚本默认不会修改集群，请人工评估后再执行修复命令。\n", flush=True)
        report_model = args.model

    # 邮件告警：生成 HTML 报告并异步发送（后台线程，不阻塞诊断/监听主流程）
    notify_by_email(args, pod, reasons, report_text, report_model)

    # 自愈执行：只有显式 --heal 时才会真正删除/重建 Pod，否则此函数立即 return
    maybe_heal(args, pod, reasons)


def maybe_diagnose(
    core_api: client.CoreV1Api,
    llm: Optional[OpenAI],
    args: argparse.Namespace,
    alarm: AlarmState,
    watched_reasons: Sequence[str],
    pod: "client.V1Pod",
) -> None:
    """判断 Pod 是否处于目标故障状态，是则触发一次诊断。"""
    if pod is None or pod.metadata is None or pod.status is None:
        return
    if pod.status.phase == "Succeeded":  # Job 正常完成，不算故障
        return

    reasons = detect_failures(pod, watched_reasons)
    if not reasons:
        return

    key = f"{pod.metadata.namespace}/{pod.metadata.name}#{pod.metadata.uid}"
    fingerprint = failure_fingerprint(pod, reasons)
    if not alarm.should_analyze(key, fingerprint):
        return

    try:
        diagnose_and_report(core_api, llm, args, pod, reasons)
    except Exception as exc:  # pragma: no cover - 极端情况保底
        log(f"诊断 {pod.metadata.name} 时出现异常: {type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------- #
# 主循环：watch 监听 Pod 变化
# --------------------------------------------------------------------------- #

def sync_all_pods(
    core_api: client.CoreV1Api,
    llm: Optional[OpenAI],
    args: argparse.Namespace,
    alarm: AlarmState,
    watched_reasons: Sequence[str],
) -> Optional[str]:
    """全量同步一次：处理当前已经处于故障态的 Pod，并返回集群的 resourceVersion。"""
    try:
        pod_list = core_api.list_namespaced_pod(namespace=args.namespace)
    except ApiException as exc:
        raise SystemExit(
            f"[错误] 无法列出 {args.namespace} 命名空间的 Pod: HTTP {exc.status} {exc.reason}\n"
            f"{exc.body}"
        ) from exc

    pods = pod_list.items or []
    log(f"全量同步完成：命名空间 {args.namespace} 共有 {len(pods)} 个 Pod。")
    for pod in pods:
        maybe_diagnose(core_api, llm, args, alarm, watched_reasons, pod)
    return pod_list.metadata.resource_version if pod_list.metadata else None


def run_doctor(args: argparse.Namespace) -> None:
    """程序主入口：初始化客户端 -> 全量同步 -> 持续 watch。"""
    core_api = build_k8s_client()
    llm = None if args.no_llm else build_llm_client()
    alarm = AlarmState(args.cooldown)

    if args.strict:
        watched_reasons: List[str] = list(REQUIRED_FAILURE_REASONS)
    else:
        watched_reasons = list(REQUIRED_FAILURE_REASONS + EXTENDED_FAILURE_REASONS)

    args.email_enabled = email_enabled(args)

    if args.heal:
        log("K8s 故障自愈机器人启动（自愈模式：--heal 已开启，会对镜像故障 Pod 执行删除并重建）")
    else:
        log("K8s 故障自愈机器人启动（只读模式，不会修改集群）")
    log(f"命名空间: {args.namespace} | 监控状态: {', '.join(watched_reasons)}")
    log(f"大模型: {'<已禁用 --no-llm>' if args.no_llm else args.model} | 告警冷却: {args.cooldown}s")

    if args.heal:
        heal_namespaces = [item.strip() for item in str(args.heal_namespace).split(",") if item.strip()]
        log(
            f"自愈模式: 已开启 | 替换镜像: {args.heal_image} | 命名空间白名单: {heal_namespaces} | "
            f"倒计时: {args.heal_countdown}s | 次数上限: {args.heal_max_attempts} 次 / {args.heal_cooldown}s 冷却"
        )
        log(f"自愈清单落盘目录: {Path(args.heal_dir)}")
        if args.namespace not in heal_namespaces:
            log(f"注意: --namespace={args.namespace} 不在自愈白名单 {heal_namespaces} 内，该命名空间的 Pod 不会被自愈。")
    else:
        log("自愈模式: 关闭（只读诊断；需要自愈请加 --heal）")

    if args.email_enabled:
        email_config = load_email_config(args)
        if args.email_dry_run:
            log(f"邮件告警: 已开启（--email-dry-run：报告落盘到 {args.report_dir}，不实际发信）")
        elif email_config is not None:
            log(
                f"邮件告警: 已开启 | 收件人: {', '.join(email_config.recipients)} | "
                f"SMTP: {email_config.host}:{email_config.port}"
                f"（{'SSL' if email_config.use_ssl else 'STARTTLS'}）| 后台线程异步发送"
            )
        else:
            log("邮件告警: 已开启但缺少完整 SMTP 配置（SMTP_HOST/SMTP_USER/SMTP_PASSWORD），将只落盘 HTML 报告。")
    else:
        log("邮件告警: 关闭（发信请加 --email 并配置 SMTP_*；配置了 SMTP_HOST 时会自动开启，--no-email 强制关闭）")

    resource_version = sync_all_pods(core_api, llm, args, alarm, watched_reasons)

    if args.once:
        log("--once 模式：单次扫描结束，退出。")
        return

    log("开始 watch 监听 Pod 事件 …（Ctrl+C 退出）")
    while True:
        try:
            watcher = watch.Watch()
            for event in watcher.stream(
                core_api.list_namespaced_pod,
                namespace=args.namespace,
                resource_version=resource_version,
                timeout_seconds=args.stream_timeout,
            ):
                event_type = event.get("type")
                pod = event.get("object")
                if not isinstance(pod, client.V1Pod):
                    continue
                if pod.metadata is not None and pod.metadata.resource_version:
                    resource_version = pod.metadata.resource_version
                if event_type not in ("ADDED", "MODIFIED"):
                    continue
                maybe_diagnose(core_api, llm, args, alarm, watched_reasons, pod)
        except KeyboardInterrupt:
            log("收到 Ctrl+C，已退出。")
            break
        except ApiException as exc:
            if exc.status == 410:
                log("watch 的 resourceVersion 已过期（HTTP 410），重新做全量同步 …")
                try:
                    resource_version = sync_all_pods(core_api, llm, args, alarm, watched_reasons)
                except SystemExit as fatal:
                    log(str(fatal))
                    time.sleep(args.retry_delay)
            else:
                log(f"watch 异常: HTTP {exc.status} {exc.reason}，{args.retry_delay}s 后重连。")
                time.sleep(args.retry_delay)
        except Exception as exc:
            log(f"watch 连接中断: {type(exc).__name__}: {exc}，{args.retry_delay}s 后重连。")
            time.sleep(args.retry_delay)


# --------------------------------------------------------------------------- #
# 自愈执行（仅 --heal 时开启；默认只读，不会调用这里面的任何函数）
# --------------------------------------------------------------------------- #

#: 自愈时执行 kubectl 的超时时间（秒）
KUBECTL_TIMEOUT = 60

#: 镜像拉取类故障族（只有这一族故障才允许自愈）
IMAGE_PULL_FAILURE_FAMILY = "ImagePullFailure"

#: 从 Pod 清单里剔除的服务端字段（保留下来重新 apply 会报错）
_MANIFEST_DROP_METADATA_FIELDS: Tuple[str, ...] = (
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "managedFields",
    "selfLink",
    "ownerReferences",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
)


def _clip(text: Optional[str], limit: int = 1200) -> str:
    """限制终端打印长度，避免 kubectl 大段输出刷屏。"""
    if not text:
        return ""
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…（输出已截断，共 {len(text)} 字符）"


def run_kubectl(
    kubectl_args: Sequence[str],
    timeout: int = KUBECTL_TIMEOUT,
    quiet: bool = False,
) -> Tuple[int, str, str]:
    """执行一条 kubectl 命令（自愈专用，会修改集群），返回 (退出码, stdout, stderr)。

    quiet=True 时只打印命令本身、不打印标准输出（用于 kubectl get -o json 这类大段输出）。
    """
    cmd = ["kubectl", *kubectl_args]
    log(f"[自愈执行][kubectl] {' '.join(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("未找到 kubectl 命令，请确认已安装 kubectl 并加入 PATH。") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"kubectl 执行超时（> {timeout}s）: {' '.join(cmd)}") from exc

    out, err = _clip(proc.stdout), _clip(proc.stderr)
    if out and not quiet:
        print(out, flush=True)
    if proc.returncode != 0 and err:
        print(err, flush=True)
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def heal_countdown(seconds: int, pod_id: str) -> bool:
    """自愈前的安全倒计时；返回 False 表示用户按 Ctrl+C 取消了自愈。"""
    if seconds <= 0:
        return True
    log(f"[自愈执行] 即将对 {pod_id} 执行自愈（删除并重建 Pod），按 Ctrl+C 可取消 …")
    try:
        for remaining in range(seconds, 0, -1):
            print(f"\r[自愈执行] 倒计时 {remaining} 秒 …（按 Ctrl+C 取消自愈）", end="", flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\r" + " " * 64 + "\r", end="", flush=True)
        log("[自愈执行] 收到 Ctrl+C：用户已取消本次自愈，集群未被修改（脚本继续运行）。")
        return False
    print("\r" + " " * 64 + "\r", end="", flush=True)
    return True


class HealGuard:
    """同一个 Pod 的自愈次数与冷却控制，避免"删了重建、重建又坏"的死循环。"""

    def __init__(self, max_attempts: int, cooldown: int) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.cooldown = max(0, int(cooldown))
        self._attempts: Dict[str, int] = {}
        self._last_heal_at: Dict[str, float] = {}

    def allow(self, pod_name: str) -> Tuple[bool, str]:
        now = time.time()
        attempts = self._attempts.get(pod_name, 0)
        if attempts >= self.max_attempts:
            return False, f"已达到单次运行的自愈次数上限（{self.max_attempts} 次）"
        last = self._last_heal_at.get(pod_name)
        if last is not None and now - last < self.cooldown:
            return False, f"距上次自愈仅 {int(now - last)}s，仍在 {self.cooldown}s 冷却中"
        return True, ""

    def record(self, pod_name: str) -> None:
        self._attempts[pod_name] = self._attempts.get(pod_name, 0) + 1
        self._last_heal_at[pod_name] = time.time()


#: 全局单例（按 pod 名称计数）
_HEAL_GUARD: Optional[HealGuard] = None


def _get_heal_guard(args: argparse.Namespace) -> HealGuard:
    global _HEAL_GUARD
    if _HEAL_GUARD is None:
        _HEAL_GUARD = HealGuard(args.heal_max_attempts, args.heal_cooldown)
    return _HEAL_GUARD


def write_yaml_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    """把 Pod 清单写成 YAML 文件（用于留档和 kubectl apply）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle, allow_unicode=True, sort_keys=False, default_flow_style=False)


def build_healed_manifest(
    pod_json: Dict[str, Any],
    healed_image: str,
    target_containers: Sequence[str],
) -> Tuple[Dict[str, Any], List[str]]:
    """基于当前 Pod 的清单生成“镜像已修复”的可重建清单，返回 (清单, 变更说明列表)。"""
    manifest: Dict[str, Any] = json.loads(json.dumps(pod_json))  # 深拷贝，避免污染原始对象
    manifest.pop("status", None)

    metadata = manifest.setdefault("metadata", {})
    for field in _MANIFEST_DROP_METADATA_FIELDS:
        metadata.pop(field, None)
    annotations = metadata.get("annotations") or {}
    annotations.pop("kubectl.kubernetes.io/last-applied-configuration", None)
    if annotations:
        metadata["annotations"] = annotations
    else:
        metadata.pop("annotations", None)

    spec = manifest.setdefault("spec", {})
    spec.pop("nodeName", None)  # 让调度器重新调度

    changes: List[str] = []
    for container in spec.get("containers") or []:
        name = container.get("name")
        if target_containers and name not in target_containers:
            continue
        old_image = container.get("image")
        if old_image != healed_image:
            container["image"] = healed_image
            changes.append(f"容器 {name}: {old_image} -> {healed_image}")
        if not container.get("imagePullPolicy"):
            container["imagePullPolicy"] = "IfNotPresent"
    return manifest, changes


def image_pull_failing_containers(pod: "client.V1Pod") -> List[str]:
    """找出处于镜像拉取类故障（waiting）的容器名。"""
    names: List[str] = []
    statuses = (pod.status.container_statuses or []) if pod.status else []
    for status in statuses:
        state = getattr(status, "state", None)
        waiting = getattr(state, "waiting", None) if state else None
        reason = getattr(waiting, "reason", None) if waiting else None
        if reason and REASON_FAMILIES.get(reason, reason) == IMAGE_PULL_FAILURE_FAMILY:
            names.append(status.name)
    return names


def describe_pod_status(namespace: str, name: str) -> str:
    """读取重建后 Pod 的当前状态，用于自愈结果校验。"""
    code, stdout, _ = run_kubectl(["get", "pod", name, "-n", namespace, "-o", "json"], timeout=30, quiet=True)
    if code != 0:
        return "<读取 Pod 状态失败>"
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        return "<Pod 状态解析失败>"
    status = data.get("status") or {}
    parts = [f"phase={status.get('phase', 'Unknown')}"]
    for item in status.get("containerStatuses") or []:
        state = item.get("state") or {}
        if state.get("running"):
            parts.append(f"容器 {item.get('name')}: Running（image={item.get('image')}, ready={item.get('ready')}）")
        elif state.get("waiting"):
            parts.append(f"容器 {item.get('name')}: Waiting/{state['waiting'].get('reason')}")
        elif state.get("terminated"):
            parts.append(f"容器 {item.get('name')}: Terminated/{state['terminated'].get('reason')}")
    return " | ".join(parts)


def maybe_heal(args: argparse.Namespace, pod: "client.V1Pod", reasons: Sequence[str]) -> None:
    """--heal 模式下才会真正执行；其它情况直接返回，保证默认只读。"""
    if not getattr(args, "heal", False) or pod is None or pod.metadata is None:
        return

    meta = pod.metadata
    namespace, name = meta.namespace, meta.name
    pod_id = f"{namespace}/{name}"
    allowed_namespaces = [item.strip() for item in str(args.heal_namespace).split(",") if item.strip()]

    # ---- 安全限制 1：命名空间白名单（默认只允许 default）----
    if namespace not in allowed_namespaces:
        log(f"[自愈执行] 跳过 {pod_id}：自愈仅允许在命名空间 {allowed_namespaces} 内执行（安全限制）。")
        return

    # ---- 安全限制 2：只处理镜像拉取类故障 ----
    families = {REASON_FAMILIES.get(reason, reason) for reason in reasons}
    if IMAGE_PULL_FAILURE_FAMILY not in families:
        log(f"[自愈执行] 跳过 {pod_id}：故障状态 {', '.join(reasons)} 不属于镜像拉取类，仅输出诊断建议。")
        return

    # ---- 安全限制 3：控制器管理的 Pod 不能靠“删 Pod”修复 ----
    owners = meta.owner_references or []
    if owners:
        log(
            f"[自愈执行] 跳过 {pod_id}：该 Pod 由 {owners[0].kind}/{owners[0].name} 管理，删除后会按原镜像重建，"
            "请修正控制器的镜像后执行 kubectl rollout restart。"
        )
        return

    # ---- 安全限制 4：同一个 Pod 的自愈次数与冷却 ----
    guard = _get_heal_guard(args)
    permitted, why = guard.allow(name)
    if not permitted:
        log(f"[自愈执行] 跳过 {pod_id}：{why}（防止自愈死循环）。")
        return

    targets = image_pull_failing_containers(pod)
    healed_image = args.heal_image
    log(f"[自愈执行] 检测到镜像拉取故障 {pod_id}（{', '.join(reasons)}），准备自愈：镜像将改为 {healed_image}")
    log(f"[自愈执行] 待修复容器: {', '.join(targets) if targets else '<未能定位，将修复全部容器>'}")

    # ---- 安全限制 5：倒计时，允许用户按 Ctrl+C 取消 ----
    if not heal_countdown(args.heal_countdown, pod_id):
        return

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    artifact_dir = Path(args.heal_dir)
    original_path = artifact_dir / f"{name}-{stamp}-original.yaml"
    healed_path = artifact_dir / f"{name}-{stamp}-healed.yaml"

    try:
        # ① 提取该 Pod 的 YAML/清单
        code, stdout, stderr = run_kubectl(["get", "pod", name, "-n", namespace, "-o", "json"], quiet=True)
        if code != 0:
            log(f"[自愈执行] 提取 Pod 清单失败，放弃自愈: {_clip(stderr, 300)}")
            return
        pod_json = json.loads(stdout)
        write_yaml_manifest(original_path, pod_json)
        log(f"[自愈执行] 已备份当前 Pod 清单: {original_path}")

        # ② 注入修复后的配置（把镜像改成可用镜像）
        log("[自愈执行] 注入修复后的配置...")
        manifest, changes = build_healed_manifest(pod_json, healed_image, targets)
        if not changes:
            log("[自愈执行] 清单中的镜像字段无需修改，放弃自愈（避免无意义的重建）。")
            return
        for change in changes:
            log(f"[自愈执行] 镜像修复: {change}")
        write_yaml_manifest(healed_path, manifest)
        log(f"[自愈执行] 修复后的清单已写入: {healed_path}")

        # ③ 删除故障 Pod（Pod 的 image 字段不可变，只能删除后重建）
        log("[自愈执行] 开始删除故障 Pod...")
        code, _, stderr = run_kubectl(["delete", "pod", name, "-n", namespace, "--wait=true", "--timeout=60s"])
        if code != 0:
            log(f"[自愈执行] 删除返回非零退出码（可能已被其它流程删除），继续尝试重建: {_clip(stderr, 300)}")

        # ④ 重新创建 Pod
        log("[自愈执行] 重新创建 Pod...")
        code, _, stderr = run_kubectl(["apply", "-f", str(healed_path)])
        if code != 0:
            log("[自愈执行] kubectl apply 失败，改用 kubectl create 重试 …")
            code, _, stderr = run_kubectl(["create", "-f", str(healed_path)])
            if code != 0:
                log(f"[自愈执行] 重建 Pod 失败: {_clip(stderr, 300)}")
                log(f"[自愈执行] 请人工执行: kubectl apply -f {healed_path}")
                return

        guard.record(name)

        # ⑤ 校验自愈结果
        log("[自愈执行] 正在校验自愈结果 …")
        log(f"[自愈执行] {pod_id} 当前状态: {describe_pod_status(namespace, name)}")
        log("[自愈执行] 自愈完成！")
        log(
            f"[自愈执行] 提示：若状态仍为 Pending/Waiting，说明新镜像 {healed_image} 正在拉取，"
            f"稍后可用 kubectl get pod {name} -n {namespace} 复查；原始清单已备份到 {original_path}。"
        )
    except json.JSONDecodeError as exc:
        log(f"[自愈执行] 解析 Pod 清单失败: {exc}")
    except Exception as exc:
        log(f"[自愈执行] 自愈过程异常: {type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------- #
# 邮件告警（SMTP + HTML，异步发送，不阻塞 watch 主循环）
# --------------------------------------------------------------------------- #

@dataclass
class EmailConfig:
    """SMTP 配置。"""

    host: str
    port: int
    user: str
    password: str
    sender: str
    recipients: List[str]
    use_ssl: bool
    use_starttls: bool
    timeout: int = 30


def email_enabled(args: argparse.Namespace) -> bool:
    """--email 显式开启，或配置了 SMTP_HOST 时自动开启；--no-email 可强制关闭。"""
    if getattr(args, "no_email", False):
        return False
    return bool(getattr(args, "email", False) or os.environ.get("SMTP_HOST"))


def load_email_config(args: argparse.Namespace) -> Optional[EmailConfig]:
    """从命令行/环境变量读取 SMTP 配置；信息不完整时返回 None（此时只落盘 HTML）。"""
    host = (args.smtp_host or os.environ.get("SMTP_HOST") or "").strip()
    user = (args.smtp_user or os.environ.get("SMTP_USER") or "").strip()
    password = args.smtp_password or os.environ.get("SMTP_PASSWORD") or ""
    raw_recipients = args.mail_to or os.environ.get("MAIL_TO") or DEFAULT_MAIL_TO
    recipients = [item.strip() for item in re.split(r"[,;]", raw_recipients) if item.strip()]
    if not (host and user and password and recipients):
        return None

    port = int(args.smtp_port or os.environ.get("SMTP_PORT") or 465)
    use_starttls = bool(args.smtp_starttls) or port in (25, 587)
    use_ssl = port == 465 and not use_starttls
    sender = args.mail_from or os.environ.get("MAIL_FROM") or user
    return EmailConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        sender=sender,
        recipients=recipients,
        use_ssl=use_ssl,
        use_starttls=use_starttls,
        timeout=args.smtp_timeout,
    )


def _md_inline(text: str) -> str:
    """把一行 Markdown 转成 HTML（先转义再处理行内标记）。"""
    out = html.escape(text, quote=False)
    out = re.sub(r"`([^`]+)`", r"<code>\1</code>", out)
    out = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", out)
    out = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", out)
    out = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r'<a href="\2">\1</a>', out)
    return out


def _md_table_row(line: str) -> List[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def markdown_to_html(text: str) -> str:
    """极简 Markdown -> HTML 转换（标题/段落/列表/代码块/引用/表格/分隔线）。"""
    lines = (text or "").replace("\r\n", "\n").split("\n")
    blocks: List[str] = []
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()

        if stripped.startswith("```"):  # 代码块
            index += 1
            buffer: List[str] = []
            while index < len(lines) and not lines[index].strip().startswith("```"):
                buffer.append(lines[index])
                index += 1
            index += 1
            blocks.append(f'<pre class="code"><code>{html.escape(chr(10).join(buffer))}</code></pre>')
            continue

        is_table = stripped.startswith("|") and index + 1 < len(lines) and re.match(
            r"^\|[\s:\-|]+\|$", lines[index + 1].strip()
        )
        if is_table:  # 表格
            header = _md_table_row(stripped)
            index += 2
            rows: List[List[str]] = []
            while index < len(lines) and lines[index].strip().startswith("|"):
                rows.append(_md_table_row(lines[index]))
                index += 1
            head_html = "".join(f"<th>{_md_inline(cell)}</th>" for cell in header)
            body_html = "".join(
                "<tr>" + "".join(f"<td>{_md_inline(cell)}</td>" for cell in row) + "</tr>" for row in rows
            )
            blocks.append(f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>")
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:  # 标题
            level = len(heading.group(1))
            blocks.append(f"<h{level}>{_md_inline(heading.group(2).strip())}</h{level}>")
            index += 1
            continue

        if re.match(r"^([-*_])\1{2,}$", stripped):  # 分隔线
            blocks.append("<hr>")
            index += 1
            continue

        if stripped.startswith(">"):  # 引用
            buffer = []
            while index < len(lines) and lines[index].strip().startswith(">"):
                buffer.append(lines[index].strip().lstrip(">").strip())
                index += 1
            blocks.append(f"<blockquote>{_md_inline(' '.join(buffer))}</blockquote>")
            continue

        if re.match(r"^([-*+])\s+", stripped) or re.match(r"^\d+\.\s+", stripped):  # 列表
            ordered = bool(re.match(r"^\d+\.\s+", stripped))
            items: List[str] = []
            while index < len(lines):
                current = lines[index].strip()
                match = re.match(r"^\d+\.\s+(.*)$", current) if ordered else re.match(r"^[-*+]\s+(.*)$", current)
                if not match:
                    break
                items.append(_md_inline(match.group(1)))
                index += 1
            tag = "ol" if ordered else "ul"
            blocks.append(f"<{tag}>" + "".join(f"<li>{item}</li>" for item in items) + f"</{tag}>")
            continue

        if not stripped:  # 空行
            index += 1
            continue

        paragraph: List[str] = []  # 普通段落
        while index < len(lines):
            current = lines[index].strip()
            if not current or current.startswith(("#", "```", "|", ">")) or re.match(r"^([-*+])\s+", current):
                break
            paragraph.append(current)
            index += 1
        blocks.append(f"<p>{_md_inline(' '.join(paragraph))}</p>")

    return "\n".join(blocks)


#: 邮件内嵌样式（普通字符串，避免 f-string 花括号冲突）
_EMAIL_CSS = """
body { margin:0; padding:0; background:#eef2f7; font-family:-apple-system,'Segoe UI','Microsoft YaHei',Roboto,Helvetica,Arial,sans-serif; color:#0f172a; }
.wrap { max-width:920px; margin:0 auto; padding:24px 16px 40px; }
.hero { background:#1e3a8a; border-radius:14px 14px 0 0; padding:26px 30px; color:#ffffff; }
.hero h1 { margin:0 0 8px; font-size:22px; letter-spacing:.5px; }
.hero p { margin:0; font-size:13px; color:#dbeafe; }
.badge { display:inline-block; margin-top:16px; padding:6px 14px; border-radius:999px; background:#dc2626; color:#ffffff; font-size:13px; font-weight:600; }
.card { background:#ffffff; border:1px solid #e2e8f0; border-top:none; border-radius:0 0 14px 14px; padding:24px 30px 30px; }
table.meta { width:100%; border-collapse:collapse; margin:0 0 24px; font-size:14px; }
table.meta td { padding:9px 12px; border:1px solid #e2e8f0; }
table.meta td.key { background:#f8fafc; color:#475569; width:150px; }
table.meta td.val { color:#0f172a; }
.report h1,.report h2,.report h3,.report h4 { color:#1e293b; margin:24px 0 10px; font-size:17px; border-left:4px solid #2563eb; padding-left:10px; }
.report p { line-height:1.75; font-size:14px; margin:10px 0; }
.report ul,.report ol { line-height:1.75; font-size:14px; padding-left:22px; }
.report li { margin:4px 0; }
.report code { background:#f1f5f9; color:#be123c; padding:2px 5px; border-radius:4px; font-family:Consolas,Monaco,'Courier New',monospace; font-size:13px; }
.report pre.code { background:#0f172a; color:#e2e8f0; padding:14px 16px; border-radius:10px; overflow-x:auto; font-size:13px; line-height:1.6; }
.report pre.code code { background:transparent; color:inherit; padding:0; }
.report table { width:100%; border-collapse:collapse; margin:12px 0; font-size:13px; }
.report th { background:#f1f5f9; text-align:left; padding:8px 10px; border:1px solid #e2e8f0; color:#334155; }
.report td { padding:8px 10px; border:1px solid #e2e8f0; }
.report blockquote { margin:12px 0; padding:8px 14px; border-left:4px solid #cbd5e1; background:#f8fafc; color:#475569; }
.report hr { border:none; border-top:1px solid #e2e8f0; margin:20px 0; }
.footer { margin-top:22px; padding:14px 18px; border-radius:10px; background:#fff7ed; border:1px solid #fed7aa; color:#9a3412; font-size:12.5px; line-height:1.7; }
"""


def render_report_html(pod: "client.V1Pod", reasons: Sequence[str], report_md: str, model: str) -> str:
    """把 AI 报告渲染成一份适合邮箱阅读的 HTML（标题头 + 概览表 + 正文）。"""
    meta = pod.metadata
    spec = pod.spec
    containers = (spec.containers if spec else None) or []
    image = ", ".join(f"{item.name}:{item.image}" for item in containers) or "-"
    node = (spec.node_name if spec else None) or "-"
    phase = (pod.status.phase if pod.status else None) or "-"
    restarts = 0
    for status in ((pod.status.container_statuses or []) if pod.status else []):
        restarts += status.restart_count or 0

    summary = [
        ("命名空间", meta.namespace or "-"),
        ("故障 Pod", meta.name or "-"),
        ("故障状态", ", ".join(reasons)),
        ("Pod 阶段", phase),
        ("容器镜像", image),
        ("所在节点", node),
        ("重启次数", str(restarts)),
        ("分析模型", model),
        ("报告生成时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ]
    meta_rows = "\n".join(
        f'<tr><td class="key">{html.escape(str(key))}</td><td class="val">{html.escape(str(value))}</td></tr>'
        for key, value in summary
    )
    body = markdown_to_html(report_md)
    pod_id = f"{meta.namespace}/{meta.name}"
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>K8s 故障诊断报告 - {html.escape(pod_id)}</title>
<style>{_EMAIL_CSS}</style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>K8s 故障诊断报告</h1>
    <p>k8s-aiops-doctor 自动巡检 · {html.escape(model)} 大模型分析 · 只读采集，不修改集群</p>
    <span class="badge">故障状态：{html.escape(", ".join(reasons))}</span>
  </div>
  <div class="card">
    <table class="meta">{meta_rows}</table>
    <div class="report">
{body}
    </div>
    <div class="footer">
      本邮件由 <strong>k8s-aiops-doctor</strong> 自动发送。脚本默认只读采集与诊断，
      邮件中的修复命令（包括自愈删除/重建）请人工评估后再执行。
    </div>
  </div>
</div>
</body>
</html>
"""


def save_html_report(args: argparse.Namespace, pod_id: str, html_doc: str) -> Path:
    """把 HTML 报告落盘留档（同时方便在浏览器里预览邮件效果）。"""
    safe_name = re.sub(r"[^0-9A-Za-z._-]", "_", pod_id)
    path = Path(args.report_dir) / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{safe_name}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html_doc, encoding="utf-8")
    return path


def build_email_message(config: EmailConfig, subject: str, html_body: str, text_body: str) -> EmailMessage:
    """构造 multipart/alternative 邮件（纯文本 + HTML 双版本，兼容各种客户端）。"""
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.sender
    message["To"] = ", ".join(config.recipients)
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    return message


def send_email(config: EmailConfig, message: EmailMessage) -> None:
    """真正执行 SMTP 发送（阻塞；只应在后台线程里调用）。"""
    server: Any = None
    if config.use_ssl:
        server = smtplib.SMTP_SSL(config.host, config.port, timeout=config.timeout)
    else:
        server = smtplib.SMTP(config.host, config.port, timeout=config.timeout)
    try:
        if config.use_starttls:
            server.starttls()
        if config.user and config.password:
            server.login(config.user, config.password)
        server.send_message(message, from_addr=config.sender, to_addrs=config.recipients)
    finally:
        try:
            server.quit()
        except Exception:  # pragma: no cover - 已发送成功时 quit 异常无影响
            pass


def _send_email_worker(config: EmailConfig, subject: str, html_body: str, text_body: str) -> None:
    """后台线程入口：发送邮件并记录结果，异常不会影响主流程。"""
    try:
        message = build_email_message(config, subject, html_body, text_body)
        send_email(config, message)
        log(f"[邮件告警] 发送成功 -> {', '.join(config.recipients)}（主题: {subject}）")
    except Exception as exc:
        log(f"[邮件告警] 发送失败: {type(exc).__name__}: {exc}")


def notify_by_email(
    args: argparse.Namespace,
    pod: "client.V1Pod",
    reasons: Sequence[str],
    report_text: str,
    model: str,
) -> None:
    """生成 HTML 报告并通过 SMTP 异步发送（不阻塞 watch 主循环）。"""
    if not getattr(args, "email_enabled", False):
        return
    meta = pod.metadata
    pod_id = f"{meta.namespace}/{meta.name}"
    subject = f"[K8s 故障告警] {pod_id} {', '.join(reasons)}"
    try:
        html_doc = render_report_html(pod, reasons, report_text, model)
    except Exception as exc:
        log(f"[邮件告警] 渲染 HTML 报告失败: {type(exc).__name__}: {exc}")
        return

    try:
        html_path = save_html_report(args, pod_id, html_doc)
        log(f"[邮件告警] HTML 报告已落盘: {html_path}")
    except OSError as exc:
        log(f"[邮件告警] HTML 报告落盘失败: {exc}")

    if args.email_dry_run:
        log("[邮件告警] --email-dry-run 已开启：只生成 HTML 报告，不真正发送邮件。")
        return

    config = load_email_config(args)
    if config is None:
        log(
            "[邮件告警] 未检测到完整 SMTP 配置（需要 SMTP_HOST / SMTP_USER / SMTP_PASSWORD，"
            "可选 SMTP_PORT、MAIL_TO、MAIL_FROM），本次仅落盘 HTML 报告。"
        )
        return

    thread = threading.Thread(
        target=_send_email_worker,
        args=(config, subject, html_doc, report_text),
        name="smtp-mailer",
        daemon=True,
    )
    thread.start()
    log(
        f"[邮件告警] 已异步提交发送任务 -> {', '.join(config.recipients)}"
        f"（SMTP {config.host}:{config.port}, {'SSL' if config.use_ssl else 'STARTTLS/明文'}），"
        "不阻塞诊断主流程。"
    )


# --------------------------------------------------------------------------- #
# 命令行参数与入口
# --------------------------------------------------------------------------- #

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="K8s 故障自愈机器人：watch 监听 Pod 故障，调用大模型输出诊断建议（只读，不修改集群）。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-n",
        "--namespace",
        default=os.environ.get("K8S_NAMESPACE", "default"),
        help="要监听的命名空间",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
        help="大模型名称",
    )
    parser.add_argument("--cooldown", type=int, default=600, help="同一 Pod 同一故障的告警冷却时间（秒）")
    parser.add_argument("--stream-timeout", type=int, default=300, help="单次 watch 连接的超时时间（秒）")
    parser.add_argument("--retry-delay", type=int, default=5, help="watch 断开后的重连等待时间（秒）")
    parser.add_argument("--tail-lines", type=int, default=200, help="读取容器日志的最后 N 行")
    parser.add_argument("--max-log-chars", type=int, default=6000, help="每个容器日志最多提交给模型的字符数")
    parser.add_argument("--events-limit", type=int, default=30, help="每个 Pod 最多采集多少条 Event")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="只监控 ImagePullBackOff / CrashLoopBackOff / Error 三种状态",
    )
    parser.add_argument("--once", action="store_true", help="只扫描一次当前已有的 Pod，不做持续监听")
    parser.add_argument("--no-llm", action="store_true", help="只采集并打印上下文，不调用大模型")
    parser.add_argument(
        "--heal",
        action="store_true",
        help="开启自愈模式（默认关闭）：对镜像拉取类故障自动“改镜像 -> 删除 Pod -> 重建 Pod”",
    )
    parser.add_argument(
        "--heal-image",
        default=DEFAULT_HEAL_IMAGE,
        help="自愈时统一替换成的镜像（也可用环境变量 HEAL_IMAGE）",
    )
    parser.add_argument(
        "--heal-namespace",
        default=DEFAULT_HEAL_NAMESPACE,
        help="允许执行自愈的命名空间白名单（逗号分隔；安全限制，默认仅 default）",
    )
    parser.add_argument("--heal-countdown", type=int, default=5, help="自愈前的安全倒计时秒数（Ctrl+C 可取消）")
    parser.add_argument("--heal-dir", default="heal-manifests", help="自愈清单（原始/修复后 YAML）落盘目录")
    parser.add_argument("--heal-max-attempts", type=int, default=2, help="同一 Pod 单次运行最多自愈几次（防死循环）")
    parser.add_argument("--heal-cooldown", type=int, default=900, help="同一 Pod 两次自愈之间的最小间隔（秒）")
    parser.add_argument("--email", action="store_true", help="生成报告后通过 SMTP 发送 HTML 邮件（也可用 SMTP_HOST 自动开启）")
    parser.add_argument("--no-email", action="store_true", help="即使配置了 SMTP_HOST 也不发送邮件")
    parser.add_argument("--email-dry-run", action="store_true", help="只把 HTML 报告落盘、不真正发信（自检用）")
    parser.add_argument("--mail-to", default=None, help=f"收件人，多个用逗号分隔（默认 {DEFAULT_MAIL_TO}）")
    parser.add_argument("--mail-from", default=None, help="发件人地址（默认取 SMTP_USER）")
    parser.add_argument("--smtp-host", default=None, help="SMTP 服务器地址（也可用环境变量 SMTP_HOST）")
    parser.add_argument("--smtp-port", type=int, default=None, help="SMTP 端口（默认取 SMTP_PORT 或 465=SSL）")
    parser.add_argument("--smtp-user", default=None, help="SMTP 账号（也可用环境变量 SMTP_USER）")
    parser.add_argument("--smtp-password", default=None, help="SMTP 密码/授权码（也可用环境变量 SMTP_PASSWORD）")
    parser.add_argument("--smtp-starttls", action="store_true", help="用 STARTTLS（587/25）而不是 SSL（465）")
    parser.add_argument("--smtp-timeout", type=int, default=30, help="SMTP 超时时间（秒）")
    parser.add_argument("--report-dir", default="reports", help="HTML 报告落盘目录（便于留档/预览）")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    force_utf8_stdout()
    args = parse_args(argv)
    try:
        run_doctor(args)
    except KeyboardInterrupt:
        log("已退出。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
