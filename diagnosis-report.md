[15:43:24] K8s 故障自愈机器人启动（只读模式，不会修改集群）
[15:43:24] 命名空间: default | 监控状态: ImagePullBackOff, CrashLoopBackOff, Error, ErrImagePull, InvalidImageName, ImageInspectError, CreateContainerConfigError, CreateContainerError, RunContainerError, ContainerCannotRun, OOMKilled, DeadlineExceeded, PodPhaseFailed
[15:43:24] 大模型: deepseek-chat | 告警冷却: 600s
[15:43:24] 全量同步完成：命名空间 default 共有 1 个 Pod。
[15:43:24] 检测到故障 Pod: default/broken-pod -> ImagePullBackOff，开始采集上下文 …
[15:43:24] 上下文采集完成（5432 字符），正在请求大模型 deepseek-chat 分析 …

==============================================================================
[AI 诊断报告] Pod: default/broken-pod | 故障状态: ImagePullBackOff | 模型: deepseek-chat
==============================================================================
# 故障诊断报告：default/broken-pod

## 1. 【故障结论】

Pod 处于 `ImagePullBackOff` 状态，根因是容器镜像引用 `nginx:does-not-exist` 在 Docker Hub 上不存在（tag 拼写/命名错误），kubelet 反复拉取失败并进入指数退避重试。这是一个纯粹的**镜像引用错误**问题，与节点、网络、资源、调度均无关。

---

## 2. 【证据分析】

### 支持根因的证据

| 证据来源 | 内容 | 推理 |
|---|---|---|
| 容器状态 | `state=waiting(reason=ImagePullBackOff)` | 容器从未启动，卡在镜像拉取阶段 |
| 状态 message | `failed to resolve reference "docker.io/library/nginx:does-not-exist": not found` | 明确返回 `NotFound`，说明 registry 中该 tag 不存在，而非网络超时或认证失败 |
| Event | `Failed to pull image ... not found`（多次） | 与状态 message 一致，反复确认镜像不存在 |
| Event | `Normal/Pulling` → `Warning/ErrImagePull` → `Normal/BackOff` → `Warning/ImagePullBackOff` | 典型的镜像拉取失败生命周期：尝试拉取 → 失败 → 退避 → 再失败 |
| spec | `image=nginx:does-not-exist` | 镜像名本身即"does-not-exist"，明显是人为构造/错误的 tag |
| spec | `imagePullPolicy=Always` | 每次都会重新拉取，无法使用本地缓存绕过（即便本地有同名镜像也不会用） |
| restartCount | `0` | 容器从未成功启动过，排除应用崩溃、OOM、探针失败等运行期问题 |

### 排除性证据（矛盾/无关项）

- **`PodScheduled=True`、`Initialized=True`、`PodReadyToStartContainers=True`**：说明调度、卷挂载、sandbox 创建均正常，问题不在调度或存储层。
- **`PodIP=10.244.0.8` 已分配**：网络插件工作正常，排除 CNI 问题。
- **`lastState=<unknown>`、`restartCount=0`**：无历史运行实例，进一步佐证容器从未启动。
- **容器日志不可用（HTTP 400）**：因为容器从未运行，无日志可采集，这与"镜像拉取失败"完全自洽，不构成矛盾。
- **Event 时间线中出现多次 `Scheduled`（15:31、15:32、15:33、15:39）**：说明该 Pod 被反复重建（可能是人工删除重建或 demo 反复触发），但每次都是同一个镜像错误，排除偶发性。

### 结论

所有证据一致指向：**镜像 tag 不存在**。无任何证据支持网络、认证、资源或调度类故障。

---

## 3. 【修复建议】

### 步骤 1：确认正确的镜像 tag

先验证目标镜像是否存在（在能访问 registry 的环境执行）：

```bash
# 方式一：用 crane / skopeo 检查 tag 是否存在
crane manifest docker.io/library/nginx:1.27.0
# 或
skopeo inspect docker://docker.io/library/nginx:1.27.0
```

**作用**：确认要使用的正确 tag，避免再次写错。

### 步骤 2：修正镜像引用

由于该 Pod 是**裸 Pod（无 ownerReferences）**，`kubectl edit` 无法修改镜像字段（Pod spec 的 image 不可变），必须**删除后重建**。

```bash
# 删除故障 Pod
kubectl delete pod broken-pod -n default
```

然后用修正后的镜像重建：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: broken-pod
  namespace: default
  labels:
    app: broken-pod
    purpose: k8s-aiops-doctor-demo
spec:
  restartPolicy: Always
  containers:
    - name: broken-pod
      image: nginx:1.27.0          # 修正为真实存在的 tag
      imagePullPolicy: IfNotPresent # 建议改为 IfNotPresent，减少无谓拉取
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
        limits:
          cpu: 200m
          memory: 128Mi
```

```bash
kubectl apply -f broken-pod.yaml
```

**作用**：用有效镜像重建 Pod，使其能正常拉取并启动。

### 步骤 3：验证恢复

```bash
kubectl get pod broken-pod -n default -w
kubectl describe pod broken-pod -n default | grep -A5 Events
```

**作用**：确认状态从 `Pending/ImagePullBackOff` 转为 `Running/Ready`，且无新的 Failed 事件。

### 步骤 4（可选）：若需固定镜像摘要

```yaml
image: nginx@sha256:<digest>
```

**作用**：使用 digest 而非 tag，彻底避免 tag 被覆盖或拼写错误。

---

## 4. 【预防措施】

1. **CI/CD 阶段做镜像存在性校验**：在部署前用 `crane manifest` / `skopeo inspect` 校验镜像 tag 是否存在，失败即阻断发布。
2. **使用不可变 tag 或 digest**：生产环境优先使用 digest 或语义化版本 tag，禁止使用 `latest` 或随意命名。
3. **准入控制（Admission Webhook / OPA Gatekeeper / Kyverno）**：编写策略校验镜像引用格式，拒绝明显非法或未在允许列表中的镜像。
4. **改用 Deployment 而非裸 Pod**：裸 Pod 无法滚动更新、无法自愈、镜像不可变导致只能删除重建。用 Deployment 管理可获得声明式更新能力。
5. **配置合理的 `imagePullPolicy`**：固定 tag 场景用 `IfNotPresent`，减少对 registry 的压力和退避等待。
6. **监控告警**：对 `ImagePullBackOff` / `ErrImagePull` 事件配置告警，第一时间发现镜像问题。

---

## 5. 【风险提示】

- **必须删除重建**：Pod spec 中 `image` 字段不可变，`kubectl edit` / `kubectl patch` 修改镜像会报错。裸 Pod 只能 `delete` + `apply`，**会造成短暂服务中断**（本例为 demo Pod，影响可忽略）。
- **无控制器兜底**：该 Pod 无 ownerReferences，删除后不会自动重建，需手动重新 apply，注意不要遗漏。
- **删除前确认无状态依赖**：若 Pod 挂载了持久卷或承载有状态数据，删除前需确认数据已持久化，避免丢失。
- **`imagePullPolicy=Always` 的副作用**：即使修正镜像后，若保留 `Always`，每次重启都会重新拉取，在网络不稳时可能再次触发拉取失败；建议按需改为 `IfNotPresent`。
- **回滚考量**：若修正镜像后新版本行为异常，由于是裸 Pod 无 ReplicaSet 历史，无法 `kubectl rollout undo`，需手动改回旧镜像重建。建议后续迁移到 Deployment 以获得回滚能力。
- **影响范围**：仅限 `default/broken-pod` 单个 Pod，节点 `desktop-control-plane` 及其他工作负载不受影响。
==============================================================================
提示：以上仅为分析建议。本脚本不会修改集群，请人工评估后再执行修复命令。

[15:43:35] --once 模式：单次扫描结束，退出。
