---
trigger: always_on
description: "进入 hakimi-surf 工作区开展任何分析、编码、重构、测试或部署任务前，必读本地《开发手册.md》。"
---

# 必读本地《开发手册.md》

1. 每次在 `hakimi-surf` 工作区开展任何分析、编码、重构、测试、提交或部署任务前，必须遵循项目根目录本地文件 `开发手册.md`（若下方自动展开为空或本机缺失，则读取 `tencent-cloud-KR:/root/Document/hakimi-surf/开发手册.md`）。
2. `开发手册.md` 已被 `.gitignore` 排除，仅在本机与 KR 本地维护；严禁强制提交进 Git，也不要把手册正文复制到受版本控制的文件中。
3. 跑测试严格遵循手册第三节：日常仅跑定向测试；需全量回归时一律使用 `bash scripts/test_fast.sh -q`（或 Mac 本地多核 `.venv`），严禁在 KR 2 核云主机上裸跑 `.venv/bin/pytest -q` 或 `uv run pytest -q`。

@[开发手册](../../开发手册.md)
