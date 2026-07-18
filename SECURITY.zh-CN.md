# 安全策略

**English：** [SECURITY.md](SECURITY.md)

## 支持版本

| Version | Supported |
|---------|-----------|
| `main` (0.1.x) | Yes |
| Older tags | Best effort |

## 报告漏洞

请**不要**在公开 GitHub Issue 中披露未修复的安全漏洞。

1. 优先使用 GitHub **Security Advisories**：[lanpishu6300/multiraft](https://github.com/lanpishu6300/multiraft/security/advisories/new)（若可用）
2. 或私密联系：**lanpishu6300@gmail.com**，主题 `[SECURITY] multiraft`

请包含：

- 受影响 crate / 组件
- 复现步骤或 PoC（私密）
- 影响评估（鉴权绕过、DoS、数据泄露等）

我们目标在 **72 小时**内确认，并给出修复计划或时间表。

## 范围说明

- Demo Admin HTTP 与 Raft gRPC 面向实验 / 本地集群 — 若脚本默认启用且暴露到不可信网络，视为范围内。
- 依赖 CVE：优先提交升版 PR 并附简短风险说明（尊重 openraft 精确锁定，除非刻意升版）。
