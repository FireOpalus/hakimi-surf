# 境内找歌远程插件编排 (`plugins/song_finder/deploy-cn`)

本目录存放境内节点 `/opt/hakimi-song-finder/` 的容器编排与环境变量模板。

## 源码位置说明

`song_finder` 的 Python 源码与镜像构建文件位于上一级目录 [plugins/song_finder/](../)，与其余远程插件共用单一事实源 `src/hakimi_plugin_sdk`，避免 SDK 双份漂移。

## 构建与部署

```bash
# 在仓库根目录构建镜像并导出/加载至境内节点
docker build -f plugins/song_finder/Dockerfile -t hakimi-plugin-song-finder:v1.0.0 .

# 在境内节点 /opt/hakimi-song-finder/
cp .env.song_finder.example .env.song_finder
chmod 600 .env.song_finder
docker compose up -d
```
