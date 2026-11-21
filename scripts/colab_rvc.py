"""KR-side orchestrator for headless Google Colab RVC training & cover generation.

Stdlib-only so it runs cleanly under KR `.venv/bin/python` or system `python3`,
invoking `.colab-venv/bin/colab` (`google-colab-cli`) with `--auth adc`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
COLAB_VENV_DIR = REPO_ROOT / ".colab-venv"
COLAB_BIN = COLAB_VENV_DIR / "bin" / "colab"
COLAB_PYTHON = COLAB_VENV_DIR / "bin" / "python"
COLAB_ADC_PATH = REPO_ROOT / "secrets" / "colab-adc.json"
COLAB_OAUTH_PENDING_PATH = REPO_ROOT / "secrets" / ".colab_oauth_pending.json"
VOICE_ENV_PATH = REPO_ROOT / "secrets" / "voice-gateway.env"
MAIN_ENV_PATH = REPO_ROOT / ".env"
REMOTE_WORKER_PATH = REPO_ROOT / "scripts" / "colab_remote_rvc.py"

DATASET_DIR = REPO_ROOT / "data" / "rvc_datasets" / "hakimi_breezeblue"
MODEL_DIR = REPO_ROOT / "data" / "rvc_models" / "hakimi"
SOURCES_DIR = REPO_ROOT / "data" / "rvc_sources"
COVERS_DIR = REPO_ROOT / "data" / "rvc_covers"

DEFAULT_ADMIN_BASE_URL = "http://127.0.0.1:12143"
DEFAULT_CHUNK_MB = 6
STATUS_OK_MARKER = "__HAKIMI_COLAB_STATUS__=OK"
STATUS_ERR_PREFIX = "__HAKIMI_COLAB_STATUS__=ERROR:"

MIME_BY_EXT = {
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".ogg": "audio/ogg",
    ".opus": "audio/opus",
    ".m4a": "audio/mp4",
    ".flac": "audio/flac",
}


@dataclass(frozen=True)
class UtteranceSpec:
    style: str
    instructions: str
    speed: float
    text: str


_STYLE_BALLAD = (
    "温柔甜美的少女声线，气息轻柔饱满，语调带着微微的旋律感和舒缓拉长的尾音，"
    "咬字清晰自然，完全纯净干声，不要混响或背景音乐。"
)
_STYLE_CHEERFUL = (
    "元气灵动的猫耳少女声线，清亮明快、情绪积极上扬，音高自然明亮，元音饱满不尖刺，"
    "吐字清脆有弹性，纯净干声。"
)
_STYLE_EXPRESSIVE = (
    "细腻灵巧的抒情少女声线，声音富于起伏和温柔的拖腔，轻声感叹与清亮高音自然过渡，"
    "咬字温柔圆润，纯净干声。"
)
_STYLE_LYRICAL = (
    "清澈干净的吟诵与长句表达，声线平稳柔和，辅音清晰不过重，元音连贯舒展，"
    "适合训练歌唱音色模型，纯净干声。"
)

_BALLAD_TEXTS = (
    "窗外的雨滴落下来的时候，空气里总是带着微凉的青草香，我就在屋檐下安静地听着雨声呀。",
    "晚风轻轻吹过树梢，把天边的云朵揉成柔软的棉絮，今晚的月亮一定会很亮很温柔吧。",
    "如果觉得累了的话，就把烦恼先放在一边，闭上眼睛听我慢慢给你讲一个温暖的小故事哦。",
    "夜深了，城市里的霓虹灯一盏一盏暗下去，只有这间小小的屋子还亮着暖黄色的灯光呢。",
    "每一片落叶都有自己的归途，就像走失的星星，最终还是会顺着银河找回它发光的位置。",
    "清晨的第一缕阳光照进窗台时，露珠在花瓣上闪闪发亮，连呼吸都变得格外轻盈甜美呀。",
    "记忆里那些闪着光的瞬间，就像玻璃罐子里收集的糖果，每次打开都能闻到甜甜的香气。",
    "你说话的声音很好听，像是夏夜里吹过湖面的晚风，让人听着听着就忍不住想要微笑起来。",
    "不管外面是刮风还是下雨，只要推开这扇门，屋里永远都有热茶和软乎乎的抱枕在等你哦。",
    "时间就像一条安静流淌的小河，把急躁的情绪慢慢冲淡，只留下最温柔最清澈的部分呢。",
    "偶尔停下来看看天边变幻的晚霞吧，橘红色的光影铺满整片天空，真的就像一幅温柔的画。",
    "在安静的午后翻开一页旧书，纸张散发着淡淡的木香，仿佛能听见时光轻轻翻页的声音。",
    "别害怕迷路呀，就算雾气把远方的路标暂时遮住了，我也会一直跟在你身边慢慢陪你走。",
    "听见钢琴弹起熟悉的旋律时，心里紧绷的弦突然就松下来了，整个人都融化在音乐里啦。",
    "雨停之后的天空常常会挂着一道淡淡的彩虹，虽然转瞬即逝，却足够让心情开心一整个下午。",
    "把今天的委屈和疲惫都交给我保管吧，等明天太阳升起的时候，一切都会重新变得明亮起来。",
    "雪花落在手心里的那一刻凉凉的，还没等你看清它的花纹，它就悄悄化作了一滴清亮的水珠。",
    "海浪一遍一遍拍打着细软的沙滩，带走踩出来的脚印，又留下亮晶晶的贝壳作为给你的礼物。",
    "其实幸福并没有那么复杂，可能只是一碗刚出锅的热汤，和一句刚好落进心坎里的晚安呀。",
    "无论走到多远的地方，只要抬头看看同一片星空，就会觉得彼此之间的距离其实一点也不远。",
)

_CHEERFUL_TEXTS = (
    "哇塞，今天的天气也太棒了吧！阳光暖洋洋地洒在身上，简直想立刻出门去兜风或者吃冰淇淋啦！",
    "快看快看，天边那朵云是不是特别像一只正在打盹的小猫咪？连尾巴翘起来的弧度都一模一样呢！",
    "好耶！终于把最麻烦的事情全部搞定啦，现在可以光明正大地躺平，享受一整晚的快乐时光咯！",
    "猜猜我今天发现了什么宝藏小店？他们家的草莓牛奶布丁又香又滑，一口下去幸福感直接爆棚呀！",
    "打起精神来呀！没有什么难题是一顿美味的夜宵解决不了的，如果有，那就再配一杯热可可吧！",
    "叮咚，你的专属元气补给站已经准时上线啦！今天也要元气满满地冲刺，绝对不能偷懒掉队哦！",
    "刚才路过面包店的时候香味飘出好远，刚出炉的可颂酥酥脆脆的，咬下去的时候还会掉渣呢！",
    "哼哼，这可难不倒我哦，只要稍微动动脑筋，这种小机关几秒钟就能轻轻松松破解掉啦！",
    "太厉害了吧！我就知道你一定能做到，这波操作简直完美得无懈可击，必须给你鼓鼓掌才行呀！",
    "周末的计划我已经全部安排妥当啦，先睡到自然醒，再去公园散步晒太阳，想想就觉得好开心！",
    "哎呀，差一点点就踩到水坑里了，还好我反应够快，像小猫一样轻巧地跳到旁边的石板上了呢！",
    "把耳机戴上，音量调到刚刚好，跟着欢快的节拍一起晃动肩膀，所有烦恼瞬间就被甩到九霄云外！",
    "新买的小彩灯串挂起来以后，整个房间立刻变得亮晶晶的，好像把一小片银河搬进了家里一样！",
    "哈哈，你刚才的表情也太有趣了吧，差点让我笑得说不出话来，再重复一遍刚才那个动作给我看嘛！",
    "春天的风软绵绵的，路边的樱花开了一整条街，花瓣随着风飘落下来的时候像下了一场粉色的雪。",
    "只要和你一起聊天，再无聊的等待都会变得特别有意思，时间不知不觉就溜走了好几个小时呢！",
)

_EXPRESSIVE_TEXTS = (
    "呜哇，怎么突然下这么大的雨呀，幸好我们提前跑进了便利店里，不然肯定要被淋成落汤鸡啦。",
    "啊——伸个大大的懒腰真的好舒服啊！连续忙了这么久，终于可以好好喘一口气休息一下了呢。",
    "咦，你有没有听见窗外好像有什么轻轻的响声？该不会是哪只路过的小动物在树枝上跳来跳去吧。",
    "嗯……让我想想看哦，这首歌的副歌部分应该怎么唱才更有感情呢？稍微放慢一点会不会更好呀。",
    "哎呀，别这样看着我啦，被你夸得都快要不好意思了，耳朵尖肯定已经偷偷变红了对不对呀。",
    "呜嗯，这颗柠檬糖比想象中酸好多哦，刚含进嘴里差点整张脸都皱在一起，不过回味又有点甜呢。",
    "啦啦啦，今天心情莫名地好得不得了，就连走路的步子都变得轻飘飘的，像是踩在云朵上一样！",
    "喂喂，可不许趁我不注意偷偷溜走哦，说好了要一直陪着我把这首长长的歌听完的，拉钩盖章！",
    "嘘——别出声，你仔细听听看，风穿过竹林的时候会发出沙沙的声响，好像大自然在轻声唱歌呢。",
    "哦呀，原来答案一直就藏在最显眼的地方呀，我们绕了这么一大圈才发现，真是太不可思议啦！",
    "呼，终于把沉甸甸的行李箱扛上楼梯了，感觉手臂都有点酸软了，快给我倒一杯冰冰凉凉的柠檬水！",
    "唉，有时候也会悄悄觉得有些孤单呢，但只要想到屏幕那头还有你在听我说话，心里马上又暖呼呼的。",
    "哇哦，烟花在夜空中炸开的那一瞬间真是太璀璨了，五颜六色的光点倒映在湖面上，美得让人屏住呼吸！",
    "唔，为什么闹钟总是在梦境最精彩的时候响起来呀，再多给我五分钟嘛，就五分钟绝对不赖床了！",
    "嘿嘿，这个小秘密只有我们两个人知道哦，绝对不能告诉其他任何人，要好好锁在心底的抽屉里。",
    "啊呀，刚才是不是不小心唱走音了？没关系没关系，清清嗓子再来一遍，这次一定给你一个完美的版本！",
)

_LYRICAL_TEXTS = (
    "一二三四五六七，七六五四三二一，清晨的微风拂过湖面，泛起一圈又一圈晶莹细碎的波纹。",
    "春花秋月何时了，江水悠悠向东流，高山上的白雪融化成清泉，穿行在幽深静谧的翠绿山谷之间。",
    "波浪起伏的音阶在空中流转，高音清亮穿透云层，中音圆润温暖人心，低音沉稳仿佛大地的呼吸。",
    "蓝天白云映照着碧绿的原野，成群的白鹭振翅飞向远方的芦苇荡，留下一串清脆悦耳的鸣叫声。",
    "雨过天晴的午后空气格外清新，青石板路上倒映着飞檐斗拱的影子，远处传来几声悠扬的古琴曲。",
    "每一声呼吸都要保持平稳放松，吐气均匀绵长，让声音自然地从胸腔共鸣延续到明亮的眉心位置。",
    "繁星点缀在深蓝色的夜幕之上，流星划破天际留下一道银白色的弧线，照亮了沉睡中的宁静山村。",
    "秋天的枫叶红得像燃烧的火炬，风一吹便沙沙作响，踩在厚厚的落叶地毯上发出清脆愉悦的声音。",
    "玻璃窗上的水雾渐渐凝结成细密的水珠，用指尖轻轻画出一颗小小的爱心，窗外世界立刻变得清晰。",
    "长长的走廊尽头是一扇半掩着的木门，温暖的阳光斜斜地铺在地板上，灰尘在光柱里安静地飞舞。",
    "无论是欢快的舞曲还是缓慢的民谣，都要用心去体会字里行间的节奏感，让每一句尾音都自然消散。",
    "远航的船只拉响了低沉的汽笛，白色的浪花在船尾翻涌散开，海鸥盘旋在桅杆上方久久不愿离去。",
    "冬日的暖阳透过薄薄的窗帘洒在书桌上，杯子里的红茶冒出袅袅白烟，带来一份难得的宁静与惬意。",
    "琴键在指尖下流畅地跳跃，从柔和的弱拍渐强到澎湃的高潮，随后又化作如泣如诉的轻声吟唱。",
    "山间的小路弯弯曲曲通向云雾深处，路旁不知名的野花悄悄绽放，散发着淡淡的、若有若无的芬芳。",
    "愿每一个认真生活的夜晚都有美梦相伴，愿每一次勇敢出发的清晨都能迎着微光抵达向往的远方。",
)


def build_corpus_utterances() -> list[UtteranceSpec]:
    specs: list[UtteranceSpec] = []
    groups = (
        ("ballad", _STYLE_BALLAD, 0.92, _BALLAD_TEXTS),
        ("cheerful", _STYLE_CHEERFUL, 1.02, _CHEERFUL_TEXTS),
        ("expressive", _STYLE_EXPRESSIVE, 0.95, _EXPRESSIVE_TEXTS),
        ("lyrical", _STYLE_LYRICAL, 0.90, _LYRICAL_TEXTS),
    )
    for style, instructions, speed, texts in groups:
        for text in texts:
            specs.append(
                UtteranceSpec(
                    style=style,
                    instructions=instructions,
                    speed=speed,
                    text=text,
                )
            )
    return specs


CORPUS_UTTERANCES = build_corpus_utterances()


def log(msg: str) -> None:
    print(f"[colab-rvc] {msg}", flush=True)


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE env files cleanly without leaking values."""
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {"'", '"'}:
            val = val[1:-1]
        if key:
            result[key] = val
    return result


def _write_secret_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    os.chmod(path, 0o600)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while buf := handle.read(1024 * 1024):
            digest.update(buf)
    return digest.hexdigest()


def _wav_duration_sec(path: Path) -> float | None:
    try:
        with path.open("rb") as handle:
            if handle.read(4) != b"RIFF":
                return None
        with wave.open(str(path), "rb") as wf:
            frames = wf.getnframes()
            rate = wf.getframerate()
            if rate <= 0:
                return None
            return frames / float(rate)
    except Exception:
        return None


def _breezeblue_tts_post(
    *,
    base_url: str,
    api_key: str,
    voice_id: str,
    model_id: str,
    text: str,
    instructions: str,
    speed: float,
    output_format: str,
    timeout: float = 90.0,
) -> bytes:
    cleaned_base = base_url.strip().rstrip("/") or "https://api.breeze.blue"
    if cleaned_base.endswith("/v1"):
        cleaned_base = cleaned_base[:-3].rstrip("/")
    voice_seg = urllib.parse.quote(voice_id.strip(), safe="")
    fmt_seg = urllib.parse.quote(output_format, safe="")
    url = f"{cleaned_base}/v1/text-to-speech/{voice_seg}?output_format={fmt_seg}"

    body: dict[str, Any] = {
        "text": text,
        "language_code": "zh",
    }
    if model_id.strip():
        body["model_id"] = model_id.strip()
    if instructions.strip():
        body["instructions"] = instructions.strip()
    if abs(speed - 1.0) > 1e-6:
        body["voice_settings"] = {"speed": round(speed, 2)}

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "audio/wav, application/octet-stream",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read(400).decode("utf-8", errors="replace")
        raise _BreezeHttpError(exc.code, detail) from None


class _BreezeHttpError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        super().__init__(f"BreezeBlue HTTP {status_code}: {detail[:200]}")


def build_dataset(
    *,
    dataset_dir: Path = DATASET_DIR,
    voice_env_path: Path = VOICE_ENV_PATH,
    force: bool = False,
    min_seconds: float = 300.0,
    tts_fn: Any = None,
    sleep_fn: Any = time.sleep,
) -> dict[str, Any]:
    """Synthesize dry Mandarin WAV clips via BreezeBlue and pack dataset.tar.gz."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    clips_dir = dataset_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    tar_path = dataset_dir / "dataset.tar.gz"
    manifest_path = dataset_dir / "dataset_manifest.json"

    if not force and tar_path.is_file() and manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if (
                int(manifest.get("clip_count", 0)) >= len(CORPUS_UTTERANCES)
                and float(manifest.get("total_duration_sec", 0.0)) >= min_seconds
                and _sha256_file(tar_path) == manifest.get("dataset_tar_sha256")
            ):
                log(
                    f"Dataset already built and verified ({manifest['clip_count']} clips, "
                    f"{manifest['total_duration_sec']:.1f}s); skipping synthesis."
                )
                return manifest
        except Exception:
            pass

    env = parse_env_file(voice_env_path)
    api_key = env.get("BREEZEBLUE_API_KEY", "").strip()
    voice_id = env.get("BREEZEBLUE_VOICE_ID", "").strip()
    model_id = env.get("BREEZEBLUE_MODEL_ID", "breeze-tts-2").strip() or "breeze-tts-2"
    base_url = env.get("BREEZEBLUE_BASE_URL", "https://api.breeze.blue").strip()
    if not api_key or not voice_id:
        raise RuntimeError(f"Missing BREEZEBLUE_API_KEY or BREEZEBLUE_VOICE_ID in {voice_env_path}")

    caller = tts_fn or _breezeblue_tts_post
    chosen_format = "wav_48000"
    sample_rate = 0
    total_duration = 0.0
    synthesized_count = 0
    skipped_count = 0
    utterances = CORPUS_UTTERANCES

    for idx, spec in enumerate(utterances, start=1):
        clip_path = clips_dir / f"utt_{idx:03d}.wav"
        if not force and clip_path.is_file():
            existing_dur = _wav_duration_sec(clip_path)
            if existing_dur is not None and existing_dur >= 1.5:
                if sample_rate == 0:
                    with wave.open(str(clip_path), "rb") as wf:
                        sample_rate = wf.getframerate()
                total_duration += existing_dur
                skipped_count += 1
                continue

        raw_wav = b""
        last_err: Exception | None = None
        for attempt in range(4):
            try:
                raw_wav = caller(
                    base_url=base_url,
                    api_key=api_key,
                    voice_id=voice_id,
                    model_id=model_id,
                    text=spec.text,
                    instructions=spec.instructions,
                    speed=spec.speed,
                    output_format=chosen_format,
                )
                break
            except _BreezeHttpError as exc:
                last_err = exc
                if idx == 1 and chosen_format == "wav_48000" and 400 <= exc.status_code < 429:
                    log("BreezeBlue rejected output_format=wav_48000; falling back to wav (24kHz).")
                    chosen_format = "wav"
                    continue
                if exc.status_code in {429, 500, 502, 503, 504} and attempt < 3:
                    delay = 1.5 * (2**attempt)
                    log(f"Utterance {idx} hit HTTP {exc.status_code}; retrying in {delay:.1f}s...")
                    sleep_fn(delay)
                    continue
                raise
            except Exception as exc:
                last_err = exc
                if attempt < 3:
                    delay = 1.5 * (2**attempt)
                    log(
                        f"Utterance {idx} error ({type(exc).__name__}); retrying in {delay:.1f}s..."
                    )
                    sleep_fn(delay)
                    continue
                raise

        if not raw_wav:
            raise RuntimeError(f"Empty audio returned for utterance {idx}: {last_err}")

        tmp_clip = clip_path.with_suffix(".wav.tmp")
        tmp_clip.write_bytes(raw_wav)
        dur = _wav_duration_sec(tmp_clip)
        if dur is None or dur < 1.0:
            tmp_clip.unlink(missing_ok=True)
            raise RuntimeError(f"Invalid WAV response for utterance {idx} (dur={dur})")
        tmp_clip.replace(clip_path)

        if sample_rate == 0:
            with wave.open(str(clip_path), "rb") as wf:
                sample_rate = wf.getframerate()
        total_duration += dur
        synthesized_count += 1
        if idx % 10 == 0 or idx == len(utterances):
            log(
                f"Dataset progress: {idx}/{len(utterances)} clips "
                f"(new={synthesized_count}, cached={skipped_count}, total={total_duration:.1f}s)"
            )
        sleep_fn(0.35)

    if total_duration < min_seconds:
        raise RuntimeError(
            f"Total dataset duration {total_duration:.1f}s is below required {min_seconds:.1f}s"
        )

    tmp_tar = tar_path.with_suffix(".tar.gz.tmp")
    with tarfile.open(tmp_tar, "w:gz") as tf:
        for clip in sorted(clips_dir.glob("utt_*.wav")):
            tf.add(clip, arcname=clip.name)
    tmp_tar.replace(tar_path)

    manifest = {
        "clip_count": len(utterances),
        "total_duration_sec": round(total_duration, 2),
        "sample_rate": sample_rate,
        "voice_id": voice_id,
        "model_id": model_id,
        "output_format": chosen_format,
        "dataset_tar_bytes": tar_path.stat().st_size,
        "dataset_tar_sha256": _sha256_file(tar_path),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(
        f"Dataset archive ready: {tar_path} "
        f"({manifest['clip_count']} clips, {manifest['total_duration_sec']}s, {sample_rate}Hz)"
    )
    return manifest


class ColabConnectionLostError(RuntimeError):
    """Raised when `colab` reports a dropped Jupyter WebSocket connection."""


def _ensure_colab_installed() -> None:
    if not COLAB_BIN.is_file() or not COLAB_PYTHON.is_file():
        raise RuntimeError(
            f"google-colab-cli not installed at {COLAB_VENV_DIR}. "
            "Run: python3 -m venv .colab-venv && .colab-venv/bin/pip install google-colab-cli"
        )


def _colab_base_env() -> dict[str, str]:
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = str(COLAB_ADC_PATH)
    env["PYTHONWARNINGS"] = "ignore"
    return env


def _terminate_proc(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2.0)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=2.0)
        except Exception:
            pass


def _stream_process_with_watchdog(
    cmd: list[str],
    *,
    env: dict[str, str],
    stdin_text: str | None = None,
    idle_timeout: float | None = 600.0,
    conn_lost_grace: float = 3.0,
    stream: bool = True,
) -> tuple[int, str]:
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdin=subprocess.PIPE if stdin_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )
    if stdin_text is not None and proc.stdin is not None:
        try:
            proc.stdin.write(stdin_text)
            proc.stdin.close()
        except BrokenPipeError:
            pass

    q: queue.Queue[str | None] = queue.Queue()

    def _reader() -> None:
        assert proc.stdout is not None
        try:
            for line in iter(proc.stdout.readline, ""):
                q.put(line)
        except Exception:
            pass
        finally:
            q.put(None)

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()

    lines: list[str] = []
    conn_lost = False
    conn_lost_deadline: float | None = None

    while True:
        wait_sec = idle_timeout
        if conn_lost_deadline is not None:
            remaining = conn_lost_deadline - time.monotonic()
            if remaining <= 0:
                break
            wait_sec = min(remaining, wait_sec) if wait_sec is not None else remaining
        try:
            item = q.get(timeout=wait_sec)
        except queue.Empty:
            if conn_lost:
                break
            _terminate_proc(proc)
            if proc.stdout is not None:
                try:
                    proc.stdout.close()
                except Exception:
                    pass
            raise TimeoutError(f"Command stalled for {idle_timeout}s: {' '.join(cmd)}") from None

        if item is None:
            break
        if stream:
            sys.stdout.write(item)
            sys.stdout.flush()
        lines.append(item)
        if not conn_lost and "Connection was lost." in item:
            conn_lost = True
            conn_lost_deadline = time.monotonic() + conn_lost_grace

    if conn_lost:
        _terminate_proc(proc)
        if proc.stdout is not None:
            try:
                proc.stdout.close()
            except Exception:
                pass
        raise ColabConnectionLostError("Colab WebSocket connection was lost.")

    try:
        rc = proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        _terminate_proc(proc)
        rc = proc.wait()
    if proc.stdout is not None:
        try:
            proc.stdout.close()
        except Exception:
            pass
    return rc, "".join(lines)


def _run_colab(
    args: list[str],
    *,
    require_adc: bool = True,
    check: bool = True,
    stream: bool = True,
    stdin_text: str | None = None,
    idle_timeout: float | None = 600.0,
) -> tuple[int, str]:
    _ensure_colab_installed()
    if require_adc and not COLAB_ADC_PATH.is_file():
        raise RuntimeError(
            f"Colab ADC credentials missing at {COLAB_ADC_PATH}. "
            "Run `auth --start` and `auth --code <CODE>` first."
        )
    cmd = [str(COLAB_BIN), "--auth", "adc", *args]
    env = _colab_base_env()
    rc, output = _stream_process_with_watchdog(
        cmd,
        env=env,
        stdin_text=stdin_text,
        idle_timeout=idle_timeout,
        stream=stream,
    )
    if check and rc != 0:
        raise RuntimeError(f"colab {' '.join(args)} failed (exit {rc})")
    return rc, output


def auth_start(
    *,
    pending_path: Path = COLAB_OAUTH_PENDING_PATH,
    colab_python: Path = COLAB_PYTHON,
) -> str:
    """Generate a Google OAuth URL and persist PKCE code_verifier for `auth --code`."""
    _ensure_colab_installed()
    script = """import json, os, sys
from importlib import resources
from google_auth_oauthlib.flow import InstalledAppFlow
from colab_cli.auth import PUBLIC_SCOPES, REMOTE_REDIRECT_URI

cfg_path = os.path.expanduser("~/.colab-cli-oauth-config.json")
if os.path.exists(cfg_path):
    with open(cfg_path, encoding="utf-8") as f:
        client_config = json.load(f)
else:
    cfg_res = resources.files("colab_cli").joinpath("oauth_config.json")
    client_config = json.loads(cfg_res.read_text())

flow = InstalledAppFlow.from_client_config(client_config, PUBLIC_SCOPES)
flow.redirect_uri = REMOTE_REDIRECT_URI
auth_url, state = flow.authorization_url(prompt="consent", token_usage="remote")
print(json.dumps({"auth_url": auth_url, "code_verifier": flow.code_verifier, "state": state}))
"""
    proc = subprocess.run(
        [str(colab_python), "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    _write_secret_json(
        pending_path,
        {
            "code_verifier": payload["code_verifier"],
            "state": payload["state"],
        },
    )
    auth_url = str(payload["auth_url"])
    print("\n=== Google Colab OAuth Authorization URL ===")
    print(auth_url)
    print("============================================\n", flush=True)
    return auth_url


def auth_exchange_code(
    code: str,
    *,
    pending_path: Path = COLAB_OAUTH_PENDING_PATH,
    adc_path: Path = COLAB_ADC_PATH,
    colab_python: Path = COLAB_PYTHON,
) -> None:
    """Exchange pasted OAuth code using saved PKCE code_verifier into secrets/colab-adc.json."""
    _ensure_colab_installed()
    cleaned_code = code.strip()
    if not cleaned_code:
        raise ValueError("Authorization code cannot be empty")
    if not pending_path.is_file():
        raise FileNotFoundError(
            f"Pending OAuth state not found at {pending_path}. Run `auth --start` first."
        )
    pending = json.loads(pending_path.read_text(encoding="utf-8"))

    script = """import json, os, sys
from importlib import resources
from google_auth_oauthlib.flow import InstalledAppFlow
from colab_cli.auth import PUBLIC_SCOPES, REMOTE_REDIRECT_URI

code = sys.stdin.read().strip()
code_verifier = sys.argv[1]

cfg_path = os.path.expanduser("~/.colab-cli-oauth-config.json")
if os.path.exists(cfg_path):
    with open(cfg_path, encoding="utf-8") as f:
        client_config = json.load(f)
else:
    cfg_res = resources.files("colab_cli").joinpath("oauth_config.json")
    client_config = json.loads(cfg_res.read_text())

flow = InstalledAppFlow.from_client_config(
    client_config, PUBLIC_SCOPES, autogenerate_code_verifier=False
)
flow.redirect_uri = REMOTE_REDIRECT_URI
flow.code_verifier = code_verifier
flow.oauth2session.scope = None
token = flow.fetch_token(code=code)
flow.oauth2session.scope = token.get("scope")
creds = flow.credentials
if not creds.refresh_token:
    raise RuntimeError("Google did not return a refresh_token; please retry auth --start.")

adc = {
    "type": "authorized_user",
    "client_id": creds.client_id,
    "client_secret": creds.client_secret,
    "refresh_token": creds.refresh_token,
    "token_uri": creds.token_uri or "https://oauth2.googleapis.com/token",
    "scopes": list(creds.scopes or PUBLIC_SCOPES),
}
print(json.dumps(adc))
"""
    proc = subprocess.run(
        [str(colab_python), "-c", script, str(pending["code_verifier"])],
        input=cleaned_code,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"OAuth token exchange failed: {proc.stderr.strip()}")
    adc_payload = json.loads(proc.stdout.strip().splitlines()[-1])
    _write_secret_json(adc_path, adc_payload)
    pending_path.unlink(missing_ok=True)
    log(f"Saved Colab ADC credentials to {adc_path} (mode 0600).")
    _run_colab(["whoami"])
    _run_colab(["usage"], check=False)


def exec_remote_worker(
    session: str,
    action: str,
    env_vars: dict[str, str] | None = None,
) -> str:
    """Execute scripts/colab_remote_rvc.py on the Colab session and verify sentinel."""
    args = ["exec", "-s", session, "--timeout", "0", "--env", f"HAKIMI_COLAB_ACTION={action}"]
    for k, v in (env_vars or {}).items():
        args.extend(["--env", f"{k}={v}"])
    args.extend(["-f", str(REMOTE_WORKER_PATH)])

    max_attempts = 3 if action in {"setup", "reassemble", "split"} else 1
    output = ""
    for attempt in range(1, max_attempts + 1):
        try:
            _, output = _run_colab(args, check=True, stream=True, idle_timeout=600.0)
            break
        except (ColabConnectionLostError, TimeoutError) as exc:
            if attempt >= max_attempts:
                raise
            log(
                f"Transient Colab failure during '{action}' "
                f"(attempt {attempt}/{max_attempts}: {exc}); retrying in 5s..."
            )
            time.sleep(5.0)

    for line in reversed(output.splitlines()):
        if line.startswith(STATUS_ERR_PREFIX):
            raise RuntimeError(
                f"Remote worker action '{action}' failed: {line[len(STATUS_ERR_PREFIX) :]}"
            )
    if STATUS_OK_MARKER not in output:
        raise RuntimeError(f"Remote worker action '{action}' did not emit {STATUS_OK_MARKER}")
    return output


def _ensure_remote_dirs(session: str, remote_dirs: list[str]) -> None:
    literal = json.dumps(remote_dirs)
    code = f"import os\nfor d in {literal}: os.makedirs(d, exist_ok=True)\n"
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            _run_colab(
                ["exec", "-s", session, "--timeout", "60"],
                stdin_text=code,
                stream=False,
                idle_timeout=60.0,
            )
            return
        except Exception as exc:
            last_err = exc
            if attempt < 3:
                log(f"Creating remote dirs failed (attempt {attempt}/3: {exc}); retrying in 4s...")
                time.sleep(4.0)
    raise RuntimeError(f"Failed creating remote dirs: {last_err}")


def upload_file_chunked(
    session: str,
    local_path: Path,
    remote_path: str,
    *,
    chunk_mb: int = DEFAULT_CHUNK_MB,
    tag: str | None = None,
) -> None:
    """Upload a file to Colab, chunking with SHA-256 verification when larger than chunk_mb."""
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file to upload not found: {local_path}")
    remote_parent = str(Path(remote_path).parent)
    chunk_bytes = max(1, int(chunk_mb)) * 1024 * 1024
    size = local_path.stat().st_size

    if size <= chunk_bytes:
        _ensure_remote_dirs(session, [remote_parent])
        log(f"Uploading {local_path.name} ({size} B) -> {remote_path}...")
        _run_colab(["upload", "-s", session, str(local_path), remote_path], idle_timeout=90.0)
        return

    safe_tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", tag or local_path.stem)
    remote_chunk_dir = f"/content/in_chunks/{safe_tag}"
    _ensure_remote_dirs(session, [remote_parent, remote_chunk_dir])
    expected_sha = _sha256_file(local_path)

    with tempfile.TemporaryDirectory(prefix="hakimi_up_") as tmp:
        tmp_dir = Path(tmp)
        parts: list[Path] = []
        with local_path.open("rb") as src:
            idx = 0
            while data := src.read(chunk_bytes):
                part = tmp_dir / f"part_{idx:03d}"
                part.write_bytes(data)
                parts.append(part)
                idx += 1

        log(f"Uploading {local_path.name} ({size} B) in {len(parts)} chunk(s)...")
        for i, part in enumerate(parts, start=1):
            log(f"  chunk {i}/{len(parts)}: {part.name} ({part.stat().st_size} B)")
            _run_colab(
                ["upload", "-s", session, str(part), f"{remote_chunk_dir}/{part.name}"],
                idle_timeout=90.0,
            )

    exec_remote_worker(
        session,
        "reassemble",
        {
            "HAKIMI_CHUNK_DIR": remote_chunk_dir,
            "HAKIMI_OUTPUT_PATH": remote_path,
            "HAKIMI_EXPECTED_SHA256": expected_sha,
        },
    )


def download_chunked_dir(
    session: str,
    remote_chunk_dir: str,
    local_output_path: Path,
) -> Path:
    """Download a remote chunked directory, reassemble, and verify SHA-256."""
    local_output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hakimi_dl_") as tmp:
        tmp_dir = Path(tmp)
        manifest_local = tmp_dir / "manifest.json"
        _run_colab(
            ["download", "-s", session, f"{remote_chunk_dir}/manifest.json", str(manifest_local)],
            idle_timeout=90.0,
        )
        manifest = json.loads(manifest_local.read_text(encoding="utf-8"))
        chunks: list[str] = list(manifest["chunks"])
        expected_sha = str(manifest["sha256"])

        tmp_out = tmp_dir / "reassembled.bin"
        digest = hashlib.sha256()
        with tmp_out.open("wb") as dst:
            for idx, part_name in enumerate(chunks, start=1):
                part_local = tmp_dir / part_name
                log(f"Downloading {manifest['filename']} chunk {idx}/{len(chunks)}...")
                _run_colab(
                    ["download", "-s", session, f"{remote_chunk_dir}/{part_name}", str(part_local)],
                    idle_timeout=90.0,
                )
                data = part_local.read_bytes()
                digest.update(data)
                dst.write(data)

        actual_sha = digest.hexdigest()
        if actual_sha.lower() != expected_sha.lower():
            raise ValueError(
                f"Download SHA-256 mismatch for {local_output_path.name}: "
                f"expected {expected_sha}, got {actual_sha}"
            )
        shutil.move(str(tmp_out), str(local_output_path))
    log(f"Downloaded & verified {local_output_path} ({local_output_path.stat().st_size} B)")
    return local_output_path


def _wait_kernel_ready(session: str, *, retries: int = 3) -> None:
    """Wait briefly for a newly-booted Colab runtime's Jupyter kernel WebSocket."""
    time.sleep(4.0)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            _, out = _run_colab(
                ["exec", "-s", session, "--timeout", "60"],
                stdin_text='print("__READY__", flush=True)\n',
                stream=False,
                idle_timeout=60.0,
            )
            if "__READY__" in out:
                log(f"Colab kernel warmup confirmed for '{session}'.")
                return
            last_err = RuntimeError(f"Kernel warmup output missing __READY__: {out[:200]}")
        except Exception as exc:
            last_err = exc
        if attempt < retries:
            log(f"Kernel warmup attempt {attempt}/{retries} failed ({last_err}); retrying in 5s...")
            time.sleep(5.0)
    raise RuntimeError(f"Colab kernel failed warmup after {retries} attempts: {last_err}")


def provision_gpu(
    session: str,
    *,
    gpu: str = "L4",
    strict_gpu: bool = False,
    high_mem: bool = False,
) -> str:
    """Provision a Colab GPU runtime, supporting comma-separated fallbacks (e.g. H100,A100)."""
    if "," in gpu:
        candidates = [part.strip() for part in gpu.split(",") if part.strip()]
        if not candidates:
            raise ValueError(f"Invalid --gpu specification: {gpu!r}")
    else:
        candidates = [gpu] if (strict_gpu or gpu.upper() == "T4") else [gpu, "T4"]
    last_err: Exception | None = None
    for accel in candidates:
        cmd = ["new", "-s", session, "--gpu", accel]
        if high_mem and accel.upper() != "L4":
            cmd.append("--high-mem")
        created = False
        try:
            log(f"Provisioning Colab session '{session}' with GPU={accel}...")
            _run_colab(cmd, idle_timeout=120.0)
            created = True
            _wait_kernel_ready(session, retries=3)
            return accel
        except Exception as exc:
            last_err = exc
            log(f"Failed provisioning GPU={accel}: {exc}")
            if created:
                stop_session(session)
    raise RuntimeError(f"Could not provision Colab GPU ({candidates}): {last_err}")


def stop_session(session: str) -> None:
    log(f"Stopping Colab session '{session}'...")
    try:
        rc, output = _run_colab(["stop", "-s", session], check=False, idle_timeout=90.0)
        if rc != 0:
            log(f"Warning: colab stop returned {rc}: {output.strip()}")
    except Exception as exc:
        log(f"Warning: colab stop failed ({exc})")


def _load_colab_drive_module():
    import importlib.util

    if "colab_drive" in sys.modules:
        return sys.modules["colab_drive"]
    mod_path = Path(__file__).with_name("colab_drive.py")
    spec = importlib.util.spec_from_file_location("colab_drive", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load colab_drive module from {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["colab_drive"] = mod
    spec.loader.exec_module(mod)
    return mod


class SourceNotFoundError(RuntimeError):
    """Raised when auto-search cannot locate a clean official source track."""

    def __init__(self, code: str = "source_not_found", detail: str = "") -> None:
        self.code = code
        super().__init__(detail or code)


_NOISY_SOURCE_KEYWORDS = (
    "cover",
    "翻唱",
    "remix",
    "nightcore",
    "8d",
    "karaoke",
    "ktv",
    "伴奏",
    "消音",
    "纯音乐",
    "instrumental",
    "acapella",
    "live",
    "现场",
    "演唱会",
    "concert",
    "reaction",
    "反应",
    "shorts",
    "#shorts",
    "slowed",
    "reverb",
    "speed up",
    "sped up",
    "升调",
    "降调",
    "教学",
    "tutorial",
    "吉他弹唱",
    "钢琴版",
    "piano ver",
    "翻自",
    "合唱版",
)


def is_clean_source_candidate(info: dict[str, Any]) -> bool:
    """Filter yt-dlp metadata entries to exclude covers, live clips, shorts, and remixes."""
    if not isinstance(info, dict):
        return False
    if info.get("is_live") or info.get("was_live"):
        return False
    age_limit = info.get("age_limit")
    if isinstance(age_limit, (int, float)) and age_limit >= 18:
        return False
    dur = info.get("duration")
    if not isinstance(dur, (int, float)) or dur < 60 or dur > 600:
        return False
    title = str(info.get("title") or "").strip().lower()
    if not title:
        return False
    webpage_url = str(info.get("webpage_url") or info.get("url") or "").lower()
    if "/shorts/" in webpage_url:
        return False
    for bad in _NOISY_SOURCE_KEYWORDS:
        if bad in title:
            return False
    return True


def _yt_dlp_bin() -> str:
    yt_dlp = REPO_ROOT / ".venv" / "bin" / "yt-dlp"
    return str(yt_dlp) if yt_dlp.is_file() else shutil.which("yt-dlp") or "yt-dlp"


def search_and_download_clean_source(
    query: str,
    out_dir: Path,
    *,
    artist: str = "",
    yt_dlp_bin: str = "",
) -> Path:
    """Search YouTube via yt-dlp for a clean original track, or raise SourceNotFoundError."""
    clean_query = " ".join(f"{artist} {query}".split()).strip()
    if not clean_query:
        raise SourceNotFoundError("source_not_found", "Empty source search query")
    out_dir.mkdir(parents=True, exist_ok=True)
    yt_bin = yt_dlp_bin or _yt_dlp_bin()
    search_expr = f"ytsearch8:{clean_query}"
    probe_cmd = [
        yt_bin,
        "--dump-json",
        "--no-playlist",
        "--skip-download",
        "--ignore-errors",
        "--no-warnings",
        search_expr,
    ]
    proc = subprocess.run(probe_cmd, capture_output=True, text=True, errors="replace", check=False)
    candidates: list[dict[str, Any]] = []
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if is_clean_source_candidate(item):
            candidates.append(item)

    if not candidates:
        raise SourceNotFoundError(
            "source_not_found", f"No clean original audio found for query: {clean_query}"
        )

    for cand in candidates[:3]:
        target_url = str(cand.get("webpage_url") or cand.get("url") or "").strip()
        if not target_url.startswith(("https://", "http://")):
            continue
        vid_id = re.sub(r"[^A-Za-z0-9_-]", "", str(cand.get("id") or "src"))[:24] or "src"
        out_path = out_dir / f"source_{vid_id}.m4a"
        out_path.unlink(missing_ok=True)
        dl_cmd = [
            yt_bin,
            "-f",
            "bestaudio[ext=m4a]/bestaudio",
            "--no-playlist",
            "--no-warnings",
            "-o",
            str(out_path),
            target_url,
        ]
        dl_proc = subprocess.run(
            dl_cmd, capture_output=True, text=True, errors="replace", check=False
        )
        if dl_proc.returncode == 0 and out_path.is_file() and out_path.stat().st_size > 8192:
            return out_path
        out_path.unlink(missing_ok=True)

    raise SourceNotFoundError(
        "source_not_found", f"All clean candidate downloads failed for: {clean_query}"
    )


def parse_colab_usage() -> dict[str, Any]:
    """Query Colab ADC account CU balance and active sessions safely (never returns tokens)."""
    try:
        _ensure_colab_installed()
    except Exception as exc:
        return {
            "reachable": False,
            "compute_units": None,
            "usage_rate": None,
            "active_sessions": 0,
            "sessions": [],
            "error": str(exc)[:160],
        }
    if not COLAB_ADC_PATH.is_file():
        return {
            "reachable": False,
            "compute_units": None,
            "usage_rate": None,
            "active_sessions": 0,
            "sessions": [],
            "error": "colab_adc_missing",
        }
    try:
        rc_u, out_u = _run_colab(["usage"], check=False, stream=False, idle_timeout=30.0)
        rc_s, out_s = _run_colab(["sessions"], check=False, stream=False, idle_timeout=30.0)
    except Exception as exc:
        return {
            "reachable": False,
            "compute_units": None,
            "usage_rate": None,
            "active_sessions": 0,
            "sessions": [],
            "error": f"colab_cli_error:{type(exc).__name__}",
        }

    cu_balance: float | None = None
    usage_rate: float | None = None
    active_assignments = 0
    if m_cu := re.search(r"Current balance:\s*([0-9]+(?:\.[0-9]+)?)", out_u):
        cu_balance = float(m_cu.group(1))
    if m_rate := re.search(r"Usage rate:\s*([0-9]+(?:\.[0-9]+)?)", out_u):
        usage_rate = float(m_rate.group(1))
    if m_act := re.search(r"Active assignments:\s*([0-9]+)", out_u):
        active_assignments = int(m_act.group(1))

    sessions: list[str] = []
    if rc_s == 0:
        for line in out_s.splitlines():
            clean = line.strip()
            if not clean or clean.startswith(("[colab]", "Name", "-", "=")):
                continue
            token = clean.split()[0]
            if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", token) and token.lower() not in {
                "no",
                "active",
                "session",
            }:
                sessions.append(token)

    return {
        "reachable": rc_u == 0,
        "compute_units": cu_balance,
        "usage_rate": usage_rate,
        "active_sessions": max(active_assignments, len(sessions)),
        "sessions": sessions,
        "error": None if rc_u == 0 else "colab_usage_failed",
    }


def stop_all_sessions(extra_sessions: list[str] | None = None) -> dict[str, Any]:
    """Terminate all active Colab sessions reported by `colab sessions` plus known targets."""
    usage = parse_colab_usage()
    targets: list[str] = []
    for name in [*(usage.get("sessions") or []), *(extra_sessions or []), "hakimi-rvc"]:
        clean = str(name or "").strip()
        if clean and clean not in targets:
            targets.append(clean)
    stopped: list[str] = []
    for sess in targets:
        try:
            rc, _ = _run_colab(["stop", "-s", sess], check=False, stream=False, idle_timeout=60.0)
            if rc == 0:
                stopped.append(sess)
        except Exception:
            pass
    return {"terminated_sessions": len(stopped), "sessions": stopped}


def run_cover_pipeline(
    *,
    session: str,
    gpu_spec: str,
    source_path: Path,
    pth_path: Path,
    index_path: Path,
    cover_dir: Path,
    model_name: str = "hakimi",
    pitch: int = 0,
    index_rate: float = 0.55,
    protect: float = 0.33,
    volume_envelope: float = 0.85,
    start_sec: str = "auto",
    duration_sec: float = 50.0,
    chorus_anchor: str = "",
    fallback_start_sec: float = 60.0,
) -> dict[str, Any]:
    """Provision Colab GPU, run setup + RVC cover, download outputs, and always stop session."""
    assigned_gpu = provision_gpu(session, gpu=gpu_spec)
    try:
        exec_remote_worker(session, "setup", {"HAKIMI_SETUP_MODE": "cover"})
        remote_pth = f"/content/out/{model_name}.pth"
        remote_idx = f"/content/out/{model_name}.index"
        upload_file_chunked(session, pth_path, remote_pth, tag=f"{model_name}_pth")
        upload_file_chunked(session, index_path, remote_idx, tag=f"{model_name}_idx")
        full_mp3, chorus_mp3, meta = _run_remote_cover(
            session,
            source_path=source_path,
            remote_pth=remote_pth,
            remote_idx=remote_idx,
            cover_dir=cover_dir,
            pitch=pitch,
            index_rate=index_rate,
            protect=protect,
            volume_envelope=volume_envelope,
            start_sec=start_sec,
            duration_sec=duration_sec,
            chorus_anchor=chorus_anchor,
            fallback_start_sec=fallback_start_sec,
        )
        return {
            "assigned_gpu": assigned_gpu,
            "full_mp3": full_mp3,
            "chorus_mp3": chorus_mp3,
            "meta": meta,
        }
    finally:
        stop_session(session)


def run_train_pipeline(
    *,
    session: str,
    gpu_spec: str,
    dataset_tar: Path,
    model_name: str = "hakimi",
    epochs: int = 250,
    save_every: int = 50,
    batch_size: int = 8,
    sample_rate: int = 40000,
    model_dir: Path = MODEL_DIR,
) -> dict[str, Any]:
    """Provision Colab GPU, run setup + RVC training, download weights, and always stop session."""
    assigned_gpu = provision_gpu(session, gpu=gpu_spec)
    try:
        exec_remote_worker(session, "setup")
        pth_out, idx_out = _run_remote_train(
            session,
            dataset_tar=dataset_tar,
            model_name=model_name,
            epochs=epochs,
            save_every=save_every,
            batch_size=batch_size,
            sample_rate=sample_rate,
            model_dir=model_dir,
        )
        return {
            "assigned_gpu": assigned_gpu,
            "pth_path": pth_out,
            "index_path": idx_out,
        }
    finally:
        stop_session(session)


def migrate_drive_assets(
    *,
    cred_path: Path | None = None,
    root_folder_id: str = "",
    model_dir: Path = MODEL_DIR,
    covers_dir: Path = COVERS_DIR,
    sources_dir: Path = SOURCES_DIR,
    include_sources: bool = False,
    overwrite: bool = False,
    dry_run: bool = False,
    drive_client: Any = None,
) -> dict[str, Any]:
    """Idempotently initialize Hakimi_Songs/ on Drive and migrate existing local assets."""
    drive_mod = _load_colab_drive_module()
    if drive_client is None:
        resolved = drive_mod.resolve_drive_credentials_path(cred_path, repo_root=REPO_ROOT)
        drive_client = drive_mod.ColabDriveClient(resolved, root_folder_id=root_folder_id)

    tree = drive_client.ensure_folder_tree(root_folder_id)
    summary: dict[str, Any] = {
        "folders": tree.as_dict(),
        "uploaded": [],
        "skipped": [],
        "dry_run": dry_run,
    }

    existing_by_role = {
        role: {item["name"]: item for item in drive_client.list_folder(role)}
        for role in ("models", "songs", "inbox")
    }

    def _sync_file(role: str, local_path: Path, drive_name: str) -> None:
        if not overwrite and drive_name in existing_by_role[role]:
            summary["skipped"].append({"role": role, "name": drive_name, "reason": "exists"})
            return
        if dry_run:
            summary["uploaded"].append(
                {
                    "role": role,
                    "name": drive_name,
                    "size": local_path.stat().st_size,
                    "dry_run": True,
                }
            )
            return
        res = drive_client.upload_file(
            role, local_path, drive_name=drive_name, replace_existing=True
        )
        summary["uploaded"].append({"role": role, **res})

    if model_dir.is_dir():
        for ext in (".pth", ".index"):
            for path in sorted(model_dir.rglob(f"*{ext}")):
                if path.is_file():
                    _sync_file("models", path, path.name)

    if covers_dir.is_dir():
        for cover_sub in sorted(p for p in covers_dir.iterdir() if p.is_dir()):
            slug = _slugify(cover_sub.name)
            chorus = cover_sub / "cover_chorus.mp3"
            full = cover_sub / "cover_full.mp3"
            if chorus.is_file():
                _sync_file("songs", chorus, f"{slug}_chorus.mp3")
            if full.is_file():
                _sync_file("songs", full, f"{slug}_full.mp3")

    if include_sources and sources_dir.is_dir():
        for src in sorted(p for p in sources_dir.iterdir() if p.is_file()):
            _sync_file("inbox", src, src.name)

    return summary


def upload_song_to_admin(
    *,
    audio_path: Path,
    title: str,
    artist: str = "",
    tags: str = "",
    start_sec: float = 0.0,
    duration_sec: float = 58.0,
    admin_base_url: str = DEFAULT_ADMIN_BASE_URL,
    env_path: Path = MAIN_ENV_PATH,
    token_override: str = "",
) -> dict[str, Any]:
    """Upload finished cover audio to KR SongLibrary `/admin/api/songs/upload`."""
    if not audio_path.is_file():
        raise FileNotFoundError(f"Audio file not found: {audio_path}")
    token = token_override.strip() or parse_env_file(env_path).get("ADMIN_TOKEN", "").strip()
    if not token:
        raise RuntimeError(f"ADMIN_TOKEN not found in {env_path}")

    ext = audio_path.suffix.lower()
    mime = MIME_BY_EXT.get(ext, "application/octet-stream")
    raw_bytes = audio_path.read_bytes()
    url = f"{admin_base_url.rstrip('/')}/admin/api/songs/upload"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": mime,
        "Content-Encoding": "identity",
        "X-Song-Title": urllib.parse.quote(title, safe=""),
        "X-Song-Artist": urllib.parse.quote(artist, safe=""),
        "X-Song-Tags": urllib.parse.quote(tags, safe=""),
        "X-Song-Filename": urllib.parse.quote(audio_path.name, safe=""),
        "X-Song-Start": str(start_sec),
        "X-Song-Duration": str(duration_sec),
    }
    req = urllib.request.Request(url, data=raw_bytes, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        detail = exc.read(400).decode("utf-8", errors="replace")
        raise RuntimeError(f"Song upload failed (HTTP {exc.code}): {detail}") from None


def fetch_song(url: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    yt_dlp = REPO_ROOT / ".venv" / "bin" / "yt-dlp"
    yt_bin = str(yt_dlp) if yt_dlp.is_file() else shutil.which("yt-dlp") or "yt-dlp"
    cmd = [
        yt_bin,
        "-f",
        "bestaudio[ext=m4a]/bestaudio",
        "--no-playlist",
        "-o",
        str(out_path),
        url,
    ]
    log(f"Downloading source song via yt-dlp -> {out_path}...")
    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0 or not out_path.is_file():
        raise RuntimeError(f"yt-dlp failed fetching {url} (exit {proc.returncode})")
    return out_path


def _run_remote_train(
    session: str,
    *,
    dataset_tar: Path,
    model_name: str,
    epochs: int,
    save_every: int,
    batch_size: int,
    sample_rate: int,
    model_dir: Path,
) -> tuple[Path, Path]:
    upload_file_chunked(session, dataset_tar, "/content/work/dataset.tar.gz", tag="dataset")
    exec_remote_worker(
        session,
        "train",
        {
            "HAKIMI_MODEL_NAME": model_name,
            "HAKIMI_DATASET_TAR": "/content/work/dataset.tar.gz",
            "HAKIMI_EPOCHS": str(epochs),
            "HAKIMI_SAVE_EVERY": str(save_every),
            "HAKIMI_BATCH_SIZE": str(batch_size),
            "HAKIMI_SAMPLE_RATE": str(sample_rate),
        },
    )
    model_dir.mkdir(parents=True, exist_ok=True)
    pth_out = model_dir / f"{model_name}.pth"
    idx_out = model_dir / f"{model_name}.index"
    download_chunked_dir(session, f"/content/out_chunks/{model_name}_pth", pth_out)
    download_chunked_dir(session, f"/content/out_chunks/{model_name}_index", idx_out)
    return pth_out, idx_out


def _run_remote_cover(
    session: str,
    *,
    source_path: Path,
    remote_pth: str,
    remote_idx: str,
    cover_dir: Path,
    pitch: int,
    index_rate: float,
    protect: float,
    volume_envelope: float,
    start_sec: str,
    duration_sec: float,
    chorus_anchor: str,
    fallback_start_sec: float,
) -> tuple[Path, Path, dict[str, Any]]:
    ext = source_path.suffix or ".m4a"
    remote_source = f"/content/work/source_song{ext}"
    upload_file_chunked(session, source_path, remote_source, tag="source_song")
    exec_remote_worker(
        session,
        "cover",
        {
            "HAKIMI_SOURCE_AUDIO": remote_source,
            "HAKIMI_PTH_PATH": remote_pth,
            "HAKIMI_INDEX_PATH": remote_idx,
            "HAKIMI_PITCH": str(pitch),
            "HAKIMI_INDEX_RATE": str(index_rate),
            "HAKIMI_PROTECT": str(protect),
            "HAKIMI_VOLUME_ENVELOPE": str(volume_envelope),
            "HAKIMI_START_SEC": str(start_sec),
            "HAKIMI_DURATION_SEC": str(duration_sec),
            "HAKIMI_CHORUS_ANCHOR": chorus_anchor,
            "HAKIMI_FALLBACK_START_SEC": str(fallback_start_sec),
        },
    )
    cover_dir.mkdir(parents=True, exist_ok=True)
    full_mp3 = cover_dir / "cover_full.mp3"
    chorus_mp3 = cover_dir / "cover_chorus.mp3"
    meta_json = cover_dir / "cover_meta.json"
    download_chunked_dir(session, "/content/out_chunks/cover_full", full_mp3)
    download_chunked_dir(session, "/content/out_chunks/cover_chorus", chorus_mp3)
    _run_colab(
        ["download", "-s", session, "/content/out/cover_meta.json", str(meta_json)],
        idle_timeout=90.0,
    )
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    return full_mp3, chorus_mp3, meta


def _slugify(title: str) -> str:
    slug = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", title.strip()).strip("_")
    return slug or f"song_{int(time.time())}"


def cmd_train(args: argparse.Namespace) -> int:
    tar_path = args.dataset_tar or (DATASET_DIR / "dataset.tar.gz")
    if not tar_path.is_file():
        raise FileNotFoundError(
            f"Dataset archive not found at {tar_path}; run build-dataset first."
        )
    provision_gpu(args.session, gpu=args.gpu, strict_gpu=args.strict_gpu, high_mem=args.high_mem)
    try:
        exec_remote_worker(args.session, "setup")
        _run_remote_train(
            args.session,
            dataset_tar=tar_path,
            model_name=args.model_name,
            epochs=args.epochs,
            save_every=args.save_every,
            batch_size=args.batch_size,
            sample_rate=args.sample_rate,
            model_dir=args.model_dir,
        )
    finally:
        if not args.keep_session:
            stop_session(args.session)
    return 0


def cmd_cover(args: argparse.Namespace) -> int:
    pth_local = args.model_dir / f"{args.model_name}.pth"
    idx_local = args.model_dir / f"{args.model_name}.index"
    if not pth_local.is_file() or not idx_local.is_file():
        raise FileNotFoundError(
            f"Local model files not found in {args.model_dir}; run train first."
        )
    cover_dir = COVERS_DIR / _slugify(args.title)
    provision_gpu(args.session, gpu=args.gpu, strict_gpu=args.strict_gpu, high_mem=args.high_mem)
    try:
        exec_remote_worker(args.session, "setup", {"HAKIMI_SETUP_MODE": "cover"})
        remote_pth = f"/content/out/{args.model_name}.pth"
        remote_idx = f"/content/out/{args.model_name}.index"
        upload_file_chunked(args.session, pth_local, remote_pth, tag=f"{args.model_name}_pth")
        upload_file_chunked(args.session, idx_local, remote_idx, tag=f"{args.model_name}_idx")
        _, chorus_mp3, meta = _run_remote_cover(
            args.session,
            source_path=args.source,
            remote_pth=remote_pth,
            remote_idx=remote_idx,
            cover_dir=cover_dir,
            pitch=args.pitch,
            index_rate=args.index_rate,
            protect=args.protect,
            volume_envelope=args.volume_envelope,
            start_sec=args.start,
            duration_sec=args.duration,
            chorus_anchor=args.chorus_anchor,
            fallback_start_sec=args.fallback_start,
        )
    finally:
        if not args.keep_session:
            stop_session(args.session)

    if args.upload:
        res = upload_song_to_admin(
            audio_path=chorus_mp3,
            title=args.title,
            artist=args.artist,
            tags=args.tags,
            start_sec=0.0,
            duration_sec=min(58.0, max(5.0, float(meta.get("chorus_duration_sec", 50.0)) + 1.0)),
            admin_base_url=args.admin_url,
        )
        log(f"Uploaded to SongLibrary: {json.dumps(res, ensure_ascii=False)}")
    return 0


def cmd_all(args: argparse.Namespace) -> int:
    build_dataset(
        dataset_dir=DATASET_DIR,
        force=args.force_dataset,
        min_seconds=args.min_seconds,
    )
    tar_path = DATASET_DIR / "dataset.tar.gz"
    cover_dir = COVERS_DIR / _slugify(args.title)
    provision_gpu(args.session, gpu=args.gpu, strict_gpu=args.strict_gpu, high_mem=args.high_mem)
    try:
        exec_remote_worker(args.session, "setup")
        _run_remote_train(
            args.session,
            dataset_tar=tar_path,
            model_name=args.model_name,
            epochs=args.epochs,
            save_every=args.save_every,
            batch_size=args.batch_size,
            sample_rate=args.sample_rate,
            model_dir=args.model_dir,
        )
        _, chorus_mp3, meta = _run_remote_cover(
            args.session,
            source_path=args.source,
            remote_pth=f"/content/out/{args.model_name}.pth",
            remote_idx=f"/content/out/{args.model_name}.index",
            cover_dir=cover_dir,
            pitch=args.pitch,
            index_rate=args.index_rate,
            protect=args.protect,
            volume_envelope=args.volume_envelope,
            start_sec=args.start,
            duration_sec=args.duration,
            chorus_anchor=args.chorus_anchor,
            fallback_start_sec=args.fallback_start,
        )
    finally:
        if not args.keep_session:
            stop_session(args.session)

    if args.upload:
        res = upload_song_to_admin(
            audio_path=chorus_mp3,
            title=args.title,
            artist=args.artist,
            tags=args.tags,
            start_sec=0.0,
            duration_sec=min(58.0, max(5.0, float(meta.get("chorus_duration_sec", 50.0)) + 1.0)),
            admin_base_url=args.admin_url,
        )
        log(f"Uploaded to SongLibrary: {json.dumps(res, ensure_ascii=False)}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orchestrate Google Colab RVC training and singing cover upload for Hakimi."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_auth = sub.add_parser("auth", help="One-time OAuth setup for Colab ADC credentials.")
    auth_grp = p_auth.add_mutually_exclusive_group(required=True)
    auth_grp.add_argument("--start", action="store_true", help="Print OAuth consent URL.")
    auth_grp.add_argument("--code", help="Exchange pasted OAuth authorization code.")

    sub.add_parser("whoami", help="Verify Colab ADC auth and compute unit balance.")

    p_ds = sub.add_parser(
        "build-dataset", help="Synthesize dry Mandarin WAV dataset via BreezeBlue."
    )
    p_ds.add_argument("--force", action="store_true", help="Regenerate all clips.")
    p_ds.add_argument("--min-seconds", type=float, default=300.0)

    p_fetch = sub.add_parser("fetch-song", help="Download source song audio via yt-dlp.")
    p_fetch.add_argument("--url", required=True)
    p_fetch.add_argument("--out", type=Path, required=True)

    def _add_session_opts(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--gpu",
            default="L4",
            help="Preferred Colab GPU or comma-separated fallback list (e.g. H100,A100).",
        )
        p.add_argument("--strict-gpu", action="store_true")
        p.add_argument("--high-mem", action="store_true")
        p.add_argument("--session", default="hakimi-rvc")
        p.add_argument("--keep-session", action="store_true")
        p.add_argument("--model-name", default="hakimi")
        p.add_argument("--model-dir", type=Path, default=MODEL_DIR)

    def _add_train_opts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--epochs", type=int, default=250)
        p.add_argument("--save-every", type=int, default=50)
        p.add_argument("--batch-size", type=int, default=8)
        p.add_argument("--sample-rate", type=int, default=40000)

    def _add_cover_opts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--source", type=Path, required=True)
        p.add_argument("--title", required=True)
        p.add_argument("--artist", default="")
        p.add_argument("--tags", default="")
        p.add_argument("--pitch", type=int, default=0)
        p.add_argument("--index-rate", type=float, default=0.55)
        p.add_argument("--protect", type=float, default=0.33)
        p.add_argument("--volume-envelope", type=float, default=0.85)
        p.add_argument("--start", default="auto")
        p.add_argument("--duration", type=float, default=50.0)
        p.add_argument("--chorus-anchor", default="")
        p.add_argument("--fallback-start", type=float, default=60.0)
        p.add_argument("--upload", action=argparse.BooleanOptionalAction, default=True)
        p.add_argument("--admin-url", default=DEFAULT_ADMIN_BASE_URL)

    p_train = sub.add_parser("train", help="Train RVC v2 model on Colab GPU.")
    _add_session_opts(p_train)
    _add_train_opts(p_train)
    p_train.add_argument("--dataset-tar", type=Path, default=None)

    p_cover = sub.add_parser("cover", help="Generate RVC cover on Colab GPU and upload to admin.")
    _add_session_opts(p_cover)
    _add_cover_opts(p_cover)

    p_all = sub.add_parser("all", help="Build dataset -> train -> cover -> upload in one GPU VM.")
    _add_session_opts(p_all)
    _add_train_opts(p_all)
    _add_cover_opts(p_all)
    p_all.add_argument("--force-dataset", action="store_true")
    p_all.add_argument("--min-seconds", type=float, default=300.0)

    p_up = sub.add_parser("upload-song", help="Upload a local audio clip to KR SongLibrary.")
    p_up.add_argument("--audio", type=Path, required=True)
    p_up.add_argument("--title", required=True)
    p_up.add_argument("--artist", default="")
    p_up.add_argument("--tags", default="")
    p_up.add_argument("--start", type=float, default=0.0)
    p_up.add_argument("--duration", type=float, default=58.0)
    p_up.add_argument("--admin-url", default=DEFAULT_ADMIN_BASE_URL)

    p_mig = sub.add_parser(
        "migrate-drive",
        help="Initialize Hakimi_Songs/ on Drive and migrate existing local models/covers.",
    )
    p_mig.add_argument("--cred-path", type=Path, default=None)
    p_mig.add_argument("--root-folder-id", default="")
    p_mig.add_argument("--model-dir", type=Path, default=MODEL_DIR)
    p_mig.add_argument("--covers-dir", type=Path, default=COVERS_DIR)
    p_mig.add_argument("--sources-dir", type=Path, default=SOURCES_DIR)
    p_mig.add_argument("--include-sources", action="store_true")
    p_mig.add_argument("--overwrite", action="store_true")
    p_mig.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "auth":
        if args.start:
            auth_start()
        else:
            auth_exchange_code(args.code)
        return 0
    if args.command == "whoami":
        _run_colab(["whoami"])
        _run_colab(["usage"], check=False)
        return 0
    if args.command == "build-dataset":
        build_dataset(force=args.force, min_seconds=args.min_seconds)
        return 0
    if args.command == "fetch-song":
        fetch_song(args.url, args.out)
        return 0
    if args.command == "train":
        return cmd_train(args)
    if args.command == "cover":
        return cmd_cover(args)
    if args.command == "all":
        return cmd_all(args)
    if args.command == "upload-song":
        res = upload_song_to_admin(
            audio_path=args.audio,
            title=args.title,
            artist=args.artist,
            tags=args.tags,
            start_sec=args.start,
            duration_sec=args.duration,
            admin_base_url=args.admin_url,
        )
        log(f"Uploaded to SongLibrary: {json.dumps(res, ensure_ascii=False)}")
        return 0
    if args.command == "migrate-drive":
        res = migrate_drive_assets(
            cred_path=args.cred_path,
            root_folder_id=args.root_folder_id,
            model_dir=args.model_dir,
            covers_dir=args.covers_dir,
            sources_dir=args.sources_dir,
            include_sources=args.include_sources,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
        print(json.dumps(res, ensure_ascii=False, indent=2), flush=True)
        return 0
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
