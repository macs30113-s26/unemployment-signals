# 下次回来从这里继续

## 当前进度（2026-05-18 暂停点）

### ✅ 已完成
- GitHub repo 结构正确，代码已 push
- AWS CLI 安装 + 凭证机制（PowerShell 交互式 SecureString 方法）
- S3 bucket `unemployment-signals-henry-2026` 已创建
- `articles.yaml` 已上传到 `s3://unemployment-signals-henry-2026/config/`
- Lambda function `pageview-ingest` 已创建并冒烟测试通过（match_count=55）
- 第一轮 ingest 部分跑完（dispatched 65k invocations，未知具体落地多少）

### ⚠️ 当前阻塞
- **AWS Academy Learner Lab 被 deactivate** at 2026-05-18T11:11:53-07:00
- 原因：65k Lambda async invocations 在 12 分钟内 burst 触发了 lab 的 abuse 检测
- 已发邮件给 TA 请求重置
- 等待期间已完成代码修复（见 README §7 "Incident"）

### ❌ 还没完成
- 验证 S3 staging 实际有多少文件（需等 lab 重置）
- 用新版 orchestrator 续跑剩下的 ingest（lower concurrency + dispatch-batch）
- PySpark 聚合
- Athena 建表
- 分析脚本 + 图表
- 截图证据
- README findings 段落填实际数字

---

## 等 TA 回复期间的工作

### ✅ 已做
- README 新增 §7 "Incident: Lab Deactivation Under Concurrent Fan-Out"
  - 这其实是项目的加分项，展示真实工程问题处理能力
- `orchestrator.py` 加了三个关键改动：
  - 默认 concurrency 从 40 降到 20
  - 新增 `--dispatch-batch 5000` 分批发送
  - 新增 `--head-fallback` 绕开 s3:ListBucket explicit deny
- 新增 `ingest/verify_articles.py` —— Wikipedia API 验证 articles.yaml 里的标题
- 更新 README 和 RESUME_HERE 反映这些改动

### 还可以做（等 TA 期间）
1. 跑 `python ingest/verify_articles.py` 检查 articles.yaml 里的 30 篇文章
2. README 里把 "YOUR NAME" 填成实际名字
3. 画一个干净的 architecture diagram（draw.io 或 excalidraw 把 SVG 改一下）

---

## Lab 恢复后的恢复步骤

### 1. 进项目目录 + 拉最新代码
```powershell
cd C:\Users\henry\Documents\final-project-unemployment-signals
git pull
```

### 2. AWS Academy 启动 Lab
- 等 TA 重置 lab → 收到通知后 → 打开 Learner Lab → 点 **Start Lab**
- 等绿圆圈
- 点 **AWS Details** → **AWS CLI** Show → 复制三行凭证

### 3. 配凭证 + region

```powershell
$accessKey = Read-Host "Paste aws_access_key_id (ASIA...)"
$secretKey = Read-Host "Paste aws_secret_access_key" -AsSecureString
$sessionToken = Read-Host "Paste aws_session_token" -AsSecureString
$bstr1 = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secretKey)
$secretPlain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr1)
$bstr2 = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($sessionToken)
$tokenPlain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr2)

$creds = "[default]`naws_access_key_id=$accessKey`naws_secret_access_key=$secretPlain`naws_session_token=$tokenPlain`n"
$config = "[default]`nregion = us-east-1`noutput = json`n"
mkdir $HOME\.aws -Force | Out-Null
[IO.File]::WriteAllText("$HOME\.aws\credentials", $creds, [Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText("$HOME\.aws\config", $config, [Text.UTF8Encoding]::new($false))

aws sts get-caller-identity
```

### 4. 先检查 S3 staging 实际状态

```powershell
aws s3 ls s3://unemployment-signals-henry-2026/staging/ --recursive --summarize | Select-Object -Last 5
```

如果 ListBucket 被拒，跳过这步，直接用 HEAD fallback 在续跑时检查。

### 5. 跑修复版的 orchestrator（关键：用新的 flags）

```powershell
python ingest/orchestrator.py `
    --start 2019-01-01 --end 2026-05-01 `
    --bucket unemployment-signals-henry-2026 `
    --concurrency 20 `
    --dispatch-batch 5000 `
    --head-fallback
```

这次：
- concurrency 20（半数压力，不触发 abuse detection）
- 一次只发 5000 个（不是 65000）
- 用 HEAD 检查已完成的小时，绕开 ListBucket 限制

预计 13 轮 × ~5 分钟 = **~3 小时**完成。可以离开电脑。

⚠️ **注意**：4 小时 lab session 限制下，需要至少 3 小时 + 一些 buffer。如果 lab 又被 deactivate，至少前面已经完成的轮次会保留在 S3，下次重启 lab 续跑 orchestrator 会自动从断点继续（这是 head-fallback 设计的目的）。

---

## Day 2-4 之后的事

详见 `docs/runbook.md`（PySpark on EC2 + Athena + 分析）。
