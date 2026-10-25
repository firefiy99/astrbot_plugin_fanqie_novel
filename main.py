"""番茄小说下载助手（astrbot_plugin_fanqie_novel）

群里发一条番茄分享链接，自动抓取整本书并导出 TXT 发回聊天。

设计要点：
- 抓取交给自带的后端（Rust 版 Web UI），插件只做编排，
  上游接口变化时只需换后端版本，插件无需改动；
- 后端首次运行自动准备，用户不用手动装任何东西；
- 任务全程后台执行，先回执再推文件，避免 QQ 消息超时；
- 串行队列：番茄反爬严格，并发只会让整本失败；
- 关键节点播报（25/50/75），既不刷屏又能确认还活着；
- 结果按书缓存，短期内重复请求直接重发不再抓取；
- 失败原因分类给出（后端离线/准备中/已有任务/超时）。

创作者：小星萤
"""

import asyncio
import os
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import quote
from typing import Any, Dict, Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import File, Plain
from astrbot.api.star import Context, Star
from astrbot.core.star.filter.command import GreedyStr
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .backend import BackendError, FanqieBackend
from .backend_manager import BackendManager
from .share_parser import extract_url, looks_like_fanqie, parse_book_id

PLUGIN_NAME = "astrbot_plugin_fanqie_novel"
DATA_DIR = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
DL_DIR = DATA_DIR / "downloads"

HELP = (
    "番茄小说下载助手"
    "\n/番茄 搜索 <关键词>  搜索书籍"
    "\n/番茄 下载 <序号>    下载搜索结果里的第 N 本"
    "\n/番茄 <分享链接>     下载整本为 TXT"
    "\n/番茄 状态           查看当前任务"
    "\n/番茄 取消         取消排队任务"
    "\n/番茄 健康         检查后端状态"
    "\n/番茄 回链         诊断文件回链是否可用"
    "\n/番茄 清理         清理下载缓存"
    "\n也可以直接把番茄分享链接发到群里。"
)


class Job:
    def __init__(self, key: str, umo: str, target: str):
        self.key = key
        self.umo = umo
        self.target = target
        self.title = ""
        self.job_id: Optional[int] = None
        self.state = "queued"
        self.started = time.time()
        self.error = ""

    @property
    def mins(self) -> int:
        return int((time.time() - self.started) // 60)


class FanqiePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        DL_DIR.mkdir(parents=True, exist_ok=True)

        self._mode = str(config.get("backend_mode", "auto") or "auto")
        self._manager: Optional[BackendManager] = None
        self._ready = False
        self._err = ""
        self._backend: Optional[FanqieBackend] = None

        self._queue: asyncio.Queue = asyncio.Queue()
        self._jobs: "OrderedDict[str, Job]" = OrderedDict()
        self._current: Optional[Job] = None
        self._stop = False
        self._worker: Optional[asyncio.Task] = None
        self._cache: Dict[str, tuple] = {}
        self._http_runner = None
        self._http_base = ""
        self._http_port = int(config.get("file_http_port", 18999) or 18999)
        # 文件回链：优先复用 AstrBot 面板端口的令牌服务，不自开端口
        self._file_base = ""
        self._file_base_override = str(config.get("file_url_base", "") or "").strip()
        self._search_hits: "OrderedDict[str, tuple]" = OrderedDict()
        self._search_last: Dict[str, float] = {}
        self._search_lock = asyncio.Lock()

        logger.info("[%s] 已加载，模式=%s" % (PLUGIN_NAME, self._mode))

    async def initialize(self):
        self._stop = False
        if self._mode == "external":
            url = str(self.config.get("backend_url", "http://127.0.0.1:18423"))
            pwd = str(self.config.get("backend_password", "") or "")
            self._backend = FanqieBackend(url, pwd)
            self._ready = True
        else:
            asyncio.create_task(self._boot())
        self._worker = asyncio.create_task(self._loop())

    async def _boot(self):
        try:
            self._manager = BackendManager(DATA_DIR, logger)
            base = await self._manager.start()
            self._backend = FanqieBackend(base)
            self._ready = True
            self._err = ""
            logger.info("[%s] 后端就绪 %s" % (PLUGIN_NAME, base))
        except Exception as e:
            self._err = str(e)
            logger.error("[%s] 后端启动失败: %s" % (PLUGIN_NAME, e))

    async def terminate(self):
        self._stop = True
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except BaseException:
                pass
        if self._manager:
            await self._manager.stop()
        if self._http_runner is not None:
            try:
                await self._http_runner.cleanup()
            except BaseException:
                pass
            self._http_runner = None

    # ---------- 工具 ----------

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        if not self.config.get("admin_only", False):
            return True
        raw = str(self.config.get("admin_list", "") or "")
        allow = set(x.strip() for x in raw.replace(",", "\n").split("\n") if x.strip())
        return str(event.get_sender_id() or "") in allow

    async def _say(self, umo: str, text: str):
        try:
            await self.context.send_message(umo, MessageChain(chain=[Plain(text)]))
        except Exception as e:
            logger.error("[%s] 发送失败: %s" % (PLUGIN_NAME, e))

    # ---------- 文件回链（走 AstrBot 自带令牌服务，不再自开端口） ----------

    def _dashboard_port(self) -> int:
        """AstrBot 面板端口（默认 6185）。发文件复用这个端口，不再另开端口。"""
        try:
            from astrbot.core import astrbot_config
            port = int((astrbot_config.get("dashboard") or {}).get("port") or 6185)
            return port if 0 < port < 65536 else 6185
        except Exception:
            return 6185

    @staticmethod
    def _route_gateway() -> str:
        """读默认路由的网关地址（容器里就是宿主机网关）。"""
        try:
            with open("/proc/net/route", "r", encoding="utf-8") as f:
                for line in f.readlines()[1:]:
                    parts = line.split()
                    if len(parts) < 3 or parts[1] != "00000000":
                        continue
                    raw = int(parts[2], 16)
                    ip = ".".join(str((raw >> s) & 0xFF) for s in (0, 8, 16, 24))
                    if ip and not ip.startswith("0."):
                        return ip
        except Exception:
            pass
        return ""

    def _detect_host(self) -> str:
        """探测本机在容器/主机网络中的地址。"""
        import socket
        for probe in ("8.8.8.8", "172.22.0.1"):
            try:
                sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sk.connect((probe, 80))
                ip = sk.getsockname()[0]
                sk.close()
                if ip and not ip.startswith("127."):
                    return ip
            except Exception:
                pass
        try:
            ip = socket.gethostbyname(socket.gethostname())
            if ip and not ip.startswith("127."):
                return ip
        except Exception:
            pass
        return "127.0.0.1"

    def _file_base_candidates(self) -> list:
        """候选回链基址，按「外部最可能可达」排序。

        面板端口通常是唯一已映射到宿主机/放行的端口，所以优先网关
        （端口已发布时从容器内也能连通），再退容器 IP、容器名、本机。
        """
        out = []

        def _add(u) -> None:
            u = str(u or "").strip().rstrip("/")
            if u and u not in out:
                out.append(u)

        _add(self._file_base_override)
        try:
            from astrbot.core import astrbot_config
            _add(astrbot_config.get("callback_api_base", ""))
        except Exception:
            pass
        port = self._dashboard_port()
        gw = self._route_gateway()
        if gw:
            _add("http://%s:%d" % (gw, port))
        ip = self._detect_host()
        if ip and not ip.startswith("127."):
            _add("http://%s:%d" % (ip, port))
        _add("http://astrbot:%d" % port)
        _add("http://127.0.0.1:%d" % port)
        return out

    async def _probe_base(self, base: str, path: str) -> bool:
        """用一次性令牌探测该基址是否真能取到文件。

        AstrBot 的令牌是一次性的，探测和正式发文件必须用不同令牌。
        必须用异步 client：令牌服务就在本进程里，同步请求会把自己堵死。
        """
        import aiohttp
        try:
            from astrbot.core import file_token_service
            token = await file_token_service.register_file(path, timeout=60)
        except Exception as e:
            logger.warning("[%s] 文件令牌服务不可用: %s" % (PLUGIN_NAME, e))
            return False
        url = "%s/api/file/%s" % (base.rstrip("/"), token)
        try:
            to = aiohttp.ClientTimeout(total=4)
            async with aiohttp.ClientSession(timeout=to) as s:
                async with s.get(url) as r:
                    return r.status == 200
        except Exception:
            return False

    async def _token_url(self, base: str, path: str) -> str:
        try:
            from astrbot.core import file_token_service
            token = await file_token_service.register_file(path, timeout=1800)
            return "%s/api/file/%s" % (base.rstrip("/"), token)
        except Exception as e:
            logger.warning("[%s] 注册文件令牌失败: %s" % (PLUGIN_NAME, e))
            return ""

    async def _file_url(self, path: str, name: str) -> str:
        """把本地文件换成一个 NapCat 取得到的 URL。"""
        if self._file_base:
            url = await self._token_url(self._file_base, path)
            if url:
                return url
        for base in self._file_base_candidates():
            if await self._probe_base(base, path):
                self._file_base = base
                logger.info("[%s] 文件回链地址 %s" % (PLUGIN_NAME, base))
                url = await self._token_url(base, path)
                if url:
                    return url
        # 兜底：自带 HTTP 服务（需 NapCat 能直连本机这个端口）
        base = await self._http_start()
        logger.warning(
            "[%s] 令牌回链不可用，退回自带文件服务 %s（若收不到文件，请配置 file_url_base）"
            % (PLUGIN_NAME, base)
        )
        return base + "/f/" + quote(name)

    async def _diag(self) -> str:
        """诊断文件回链：真实进程内逐个候选做一次令牌探测。"""
        probe = None
        for pth in DL_DIR.glob("*.txt"):
            probe = str(pth)
            break
        if not probe:
            probe = str(DL_DIR / "_probe.txt")
            try:
                with open(probe, "w", encoding="utf-8") as f:
                    f.write("probe")
            except Exception as e:
                return "诊断失败：无法创建探测文件 %s" % e
        lines = ["文件回链诊断（面板端口 %d）" % self._dashboard_port()]
        if self._file_base_override:
            lines.append("配置指定：%s" % self._file_base_override)
        ok_any = False
        for base in self._file_base_candidates():
            good = await self._probe_base(base, probe)
            ok_any = ok_any or good
            lines.append("%s %s" % ("可用  " if good else "不可用", base))
        if self._file_base:
            lines.append("当前使用：%s" % self._file_base)
        lines.append("结论：令牌回链%s" % (
            "可用，不需要任何端口映射" if ok_any
            else "全部不可用，将退回自带文件服务（需端口映射）"))
        return "\n".join(lines)

    async def _http_start(self) -> str:
        """兜底的独立文件服务（仅在令牌服务不可用时使用）。"""
        if self._http_base:
            return self._http_base
        from aiohttp import web
        app = web.Application()
        app.router.add_get("/f/{name}", self._http_get)
        runner = web.AppRunner(app)
        await runner.setup()
        cands = [self._http_port] + list(range(28900, 28960))
        ok = False
        last = None
        for cand in cands:
            try:
                site = web.TCPSite(runner, "0.0.0.0", cand)
                await site.start()
                self._http_port = cand
                ok = True
                break
            except Exception as e:
                last = e
        if not ok:
            raise RuntimeError("no free http port: %s" % last)
        self._http_runner = runner
        host = self._detect_host()
        self._http_base = "http://%s:%d" % (host, self._http_port)
        logger.info("[%s] 兜底文件服务已启动 %s" % (PLUGIN_NAME, self._http_base))
        return self._http_base

    async def _http_get(self, request):
        from aiohttp import web
        name = request.match_info["name"]
        f = DL_DIR / os.path.basename(name)
        if not f.exists():
            return web.Response(status=404, text="not found")
        return web.FileResponse(str(f))

    async def _send_file(self, umo: str, path: str, name: str) -> bool:
        try:
            mb = os.path.getsize(path) / 1048576
            limit = int(self.config.get("max_file_mb", 80) or 80)
            if mb > limit:
                await self._say(umo, "文件 %.1fMB 超过 %dMB 上限，未发送。" % (mb, limit))
                return False
            url = await self._file_url(path, name)
            await self.context.send_message(umo, MessageChain(chain=[File(name=name, url=url)]))
            return True
        except Exception as e:
            logger.error("[%s] 发文件失败: %s" % (PLUGIN_NAME, e))
            return False

    def _cached(self, key: str):
        hours = int(self.config.get("cache_hours", 24) or 0)
        if hours <= 0:
            return None
        hit = self._cache.get(key)
        if not hit:
            return None
        path, ts = hit
        if time.time() - ts > hours * 3600 or not os.path.exists(path):
            self._cache.pop(key, None)
            return None
        return path, ts

    def _status(self) -> str:
        lines = []
        if self._current:
            c = self._current
            lines.append("进行中：《%s》已用 %d 分钟" % (c.title or c.key, c.mins))
        n = sum(1 for j in self._jobs.values() if j.state == "queued")
        if n:
            lines.append("排队：%d 本" % n)
        return "\n".join(lines) if lines else "暂无任务"

    # ---------- 队列 ----------

    async def _loop(self):
        while not self._stop:
            try:
                job = await asyncio.wait_for(self._queue.get(), timeout=5)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if job.state == "cancelled":
                self._queue.task_done()
                continue
            self._current = job
            try:
                await self._run(job)
            except Exception as e:
                logger.exception("[%s] 任务异常" % PLUGIN_NAME)
                job.state = "failed"
                await self._say(job.umo, "下载《%s》失败：%s" % (job.title or job.key, e))
            finally:
                self._current = None
                self._queue.task_done()

    async def _run(self, job: Job):
        if not (self._ready and self._backend):
            job.state = "failed"
            await self._say(job.umo, "后端未就绪：%s" % (self._err or "仍在准备中，请稍后重试"))
            return

        if not await self._backend.ping():
            job.state = "failed"
            await self._say(job.umo, "后端无响应，请检查下载器是否正常")
            return

        job.state = "running"
        try:
            data = await self._backend.create_job(job.target)
        except BackendError as e:
            job.state = "failed"
            await self._say(job.umo, "无法创建任务：%s" % e)
            return

        jid = data.get("id") if isinstance(data, dict) else None
        if jid is None:
            job.state = "failed"
            await self._say(job.umo, "后端未返回任务 ID，版本可能不匹配")
            return
        job.job_id = int(jid)

        timeout = int(self.config.get("timeout_minutes", 40) or 40) * 60
        poll = max(5, int(self.config.get("poll_interval", 20) or 20))
        end = time.time() + timeout
        said = set()

        while time.time() < end:
            await asyncio.sleep(poll)
            if job.state == "cancelled":
                await self._say(job.umo, "已取消《%s》" % (job.title or job.key))
                return
            try:
                info = await self._backend.job(job.job_id)
            except BackendError:
                continue
            if not info:
                continue
            if not job.title:
                job.title = str(info.get("title") or info.get("book_name") or "")
            st = str(info.get("state") or "").lower()
            pct = self._pct(info)

            # 只在任务确实耗时较久时才播报，避免快速完成时刷屏
            if (self.config.get("progress_broadcast", True)
                    and pct >= 25 and job.mins >= 1):
                # 一次轮询可能跨过多档，只发当前最新的一档，且每档只发一次
                mark = max(m for m in (25, 50, 75) if pct >= m)
                if mark not in said:
                    said.add(mark)
                    await self._say(job.umo, self._bar(job, pct))

            if st in ("done", "finished", "success", "completed"):
                await self._deliver(job, info)
                return
            if st in ("failed", "error"):
                err = str(info.get("error") or info.get("message") or "未知错误")
                job.state = "failed"
                await self._say(job.umo, "下载《%s》失败：%s" % (job.title or job.key, err))
                return

        job.state = "failed"
        await self._say(job.umo, "下载《%s》超时（>%d 分钟）" % (job.title or job.key, timeout // 60))

    def _pct(self, info: dict) -> int:
        p = info.get("progress")
        if isinstance(p, dict):
            for k in ("percent", "percentage", "pct"):
                v = p.get(k)
                if isinstance(v, (int, float)):
                    return int(v)
            def _pick(d, keys):
                for k in keys:
                    v = d.get(k)
                    if isinstance(v, (int, float)):
                        return v
                return None
            done = _pick(p, ("saved_chapters", "group_done", "done"))
            total = _pick(p, ("chapter_total", "group_total", "total"))
            if isinstance(done, (int, float)) and isinstance(total, (int, float)) and total:
                return int(done * 100 / total)
        return -1

    def _bar(self, job: Job, pct: int) -> str:
        n = 12
        f = int(n * pct / 100)
        return "《%s》%s%s %d%% 已用 %d 分钟" % (job.title or job.key, "█" * f, "░" * (n - f), pct, job.mins)

    async def _deliver(self, job: Job, info: dict):
        title = job.title or str(info.get("title") or info.get("book_name") or "")
        job.state = "done"
        if not title:
            job.state = "failed"
            await self._say(job.umo, "抓取完成，但拿不到书名，无法定位文件。")
            return
        # 上游 /api/library 不可靠，但 /download/<文件名> 稳定，直接用书名拼。
        name = title + ".txt"
        local = DL_DIR / name
        if not await self._pull(name, local):
            job.state = "failed"
            await self._say(job.umo, "《%s》完成，但文件拉取失败。" % title)
            return
        await self._say(job.umo, "《%s》完成，正在发送…" % title)
        sent = await self._send_file(job.umo, str(local), name)
        if sent:
            job.state = "done"
            self._cache[job.key] = (str(local), time.time())
        else:
            job.state = "failed"

    async def _pull(self, rel: str, dest: Path) -> bool:
        import aiohttp
        try:
            to = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(timeout=to, headers=self._backend.headers()) as s:
                async with s.get(self._backend.file_url(rel)) as r:
                    if r.status != 200:
                        logger.warning("[%s] 拉文件 %s" % (PLUGIN_NAME, r.status))
                        return False
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest, "wb") as f:
                        async for ch in r.content.iter_chunked(65536):
                            f.write(ch)
            return dest.exists() and dest.stat().st_size > 0
        except Exception as e:
            logger.error("[%s] 拉取文件失败: %s" % (PLUGIN_NAME, e))
            return False

    # ---------- 指令 ----------

    @filter.command("番茄", alias={"fanqie"})
    async def cmd(self, event: AstrMessageEvent, content: GreedyStr):
        if not self._is_admin(event):
            yield event.plain_result("你没有使用权限。")
            return
        text = (content or "").strip()
        if not text or text in ("帮助", "help", "?"):
            yield event.plain_result(HELP)
            return
        if text in ("状态", "status"):
            yield event.plain_result(self._status())
            return
        if text in ("取消", "cancel"):
            yield event.plain_result(self._cancel())
            return
        if text in ("清理", "清缓存", "清除缓存", "purge"):
            yield event.plain_result(self._purge("all"))
            return
        if text in ("清理文件", "清文件"):
            yield event.plain_result(self._purge("files"))
            return
        if text in ("清理内存", "清内存"):
            yield event.plain_result(self._purge("mem"))
            return
        if text in ("健康", "health", "ping"):
            yield event.plain_result(await self._health())
            return
        if text.startswith("搜索"):
            yield event.plain_result(await self._search(event, text[2:].strip()))
            return
        if text.startswith("search"):
            yield event.plain_result(await self._search(event, text[6:].strip()))
            return
        if text.startswith("下载"):
            yield event.plain_result(await self._download_by_index(event, text[2:].strip()))
            return
        if text.startswith("dl"):
            yield event.plain_result(await self._download_by_index(event, text[2:].strip()))
            return
        if text in ("回链", "诊断", "diag"):
            yield event.plain_result(await self._diag())
            return
        yield event.plain_result(await self._submit(event, text))

    @filter.regex(r"changdunovel\.com|fanqienovel\.com")
    async def auto(self, event: AstrMessageEvent):
        """群里贴链接就识别；是否直接开跑由 auto_download 决定。"""
        if not self._is_admin(event):
            return
        msg = event.message_str or ""
        if not looks_like_fanqie(msg):
            return
        url = extract_url(msg)
        if not url:
            return
        # 同一条消息若同时命中了显式指令，交给指令处理，避免重复触发
        stripped = msg.strip()
        # 指令消息：框架会去掉斜杠，故同时判断中文/英文前缀
        if (stripped.startswith("番茄") or stripped.startswith("fanqie")
                or stripped.startswith("/") or stripped.startswith("／")
                or stripped.startswith("搜索") or stripped.startswith("下载")):
            return
        if not self.config.get("auto_download", False):
            bid, need = parse_book_id(url)
            shown = bid if bid else url
            yield event.plain_result(
                "识别到番茄链接：%s" % shown
                + "\n确认下载请发送: /番茄 " + shown
            )
            return
        yield event.plain_result(await self._submit(event, url))

    async def _health(self) -> str:
        if not self._ready:
            return "后端尚未就绪：" + (self._err or "首次运行正在准备，请稍后")
        ok = await self._backend.ping()
        return ("后端在线（%s）" % self._backend.base_url) if ok else ("后端无响应：%s" % self._backend.base_url)

    def _cancel(self) -> str:
        c = self._current
        if c and c.state == "running":
            c.state = "cancelled"
            return "已请求取消《%s》" % (c.title or c.key)
        q = [j for j in self._jobs.values() if j.state == "queued"]
        for j in q:
            j.state = "cancelled"
        return ("已取消 %d 个排队任务" % len(q)) if q else "当前没有可取消的任务"

    def _purge(self, scope: str) -> str:
        """清理缓存。scope: files=本地文件, mem=内存, all=全部, backend=后端缓存"""
        mem_n = len(self._cache)
        file_n = 0
        size = 0
        if scope in ("files", "all"):
            try:
                for p in DL_DIR.glob("*"):
                    if p.is_file():
                        size += p.stat().st_size
                        p.unlink()
                        file_n += 1
            except Exception as e:
                logger.warning("[%s] 清理文件失败: %s" % (PLUGIN_NAME, e))
        if scope in ("mem", "all"):
            self._cache.clear()
            # 顺便清掉已结束的任务记录
            for k in [k for k, v in self._jobs.items() if v.state in ("done", "failed", "cancelled")]:
                self._jobs.pop(k, None)
        be_n = 0
        if scope in ("backend", "all"):
            lib = DATA_DIR / "library"
            if lib.exists():
                import shutil
                for d in lib.iterdir():
                    try:
                        if d.is_dir():
                            shutil.rmtree(d, ignore_errors=True)
                            be_n += 1
                        elif d.is_file() and d.suffix.lower() in (".txt", ".epub", ".pdf"):
                            d.unlink()
                            be_n += 1
                    except Exception as e:
                        logger.warning("[%s] 清理后端缓存失败: %s" % (PLUGIN_NAME, e))
        parts = []
        if scope in ("files", "all"):
            parts.append("文件 %d 个（%.1fMB）" % (file_n, size / 1048576))
        if scope in ("mem", "all"):
            parts.append("内存记录 %d 条" % mem_n)
        if scope in ("backend", "all"):
            parts.append("后端缓存 %d 项" % be_n)
        if not parts:
            return "没有可清理的内容"
        return "已清理：" + "，".join(parts)

    # ---------- 搜索 ----------

    def _fmt_words(self, n) -> str:
        try:
            v = int(str(n))
        except (TypeError, ValueError):
            return ""
        if v <= 0:
            return ""
        if v >= 10000:
            return "%.1f万字" % (v / 10000.0)
        return "%d字" % v

    def _fmt_book(self, idx, item):
        raw = item.get("raw") or {}
        title = str(raw.get("book_name") or item.get("title") or "(无书名)").strip()
        author = str(item.get("author") or raw.get("author") or "").strip()
        cate = str(raw.get("category") or "").strip()
        words = self._fmt_words(raw.get("word_number"))
        status = str(raw.get("creation_status") or "").strip()
        state = "完结" if status == "0" else "连载"
        meta = [x for x in (author, cate, words, state) if x]
        head = "%d. %s" % (idx, title)
        if meta:
            return head + "\n   " + " · ".join(meta)
        return head

    async def _search(self, event, keyword):
        keyword = (keyword or "").strip()
        if not keyword:
            return "用法：/番茄 搜索 <关键词>"
        if not (self._ready and self._backend):
            return "后端尚未就绪：" + (self._err or "首次运行正在准备，请稍后重试")

        umo = event.unified_msg_origin
        cool = float(self.config.get("search_cooldown", 5) or 0)
        last = self._search_last.get(umo, 0.0)
        if cool > 0 and time.time() - last < cool:
            return "请稍等 %.0f 秒再搜索。" % (cool - (time.time() - last))
        self._search_last[umo] = time.time()

        await self._say(umo, "正在搜索「%s」…（约需几秒）" % keyword)

        async with self._search_lock:
            try:
                items = await self._backend.search(keyword)
            except BackendError as e:
                logger.warning("[%s] 搜索失败: %s" % (PLUGIN_NAME, e))
                return "搜索失败：%s" % e
            except Exception as e:
                logger.exception("[%s] 搜索异常" % PLUGIN_NAME)
                return "搜索失败：%s" % e

        if not items:
            self._search_hits.pop(umo, None)
            return "没搜到「%s」相关的书。\n换个关键词试试，或用分享链接直接下载。" % keyword

        limit = int(self.config.get("search_max_results", 10) or 10)
        limit = max(1, min(limit, len(items)))
        shown = items[:limit]

        lines = ["搜索「%s」共 %d 本（显示前 %d 本）" % (keyword, len(items), limit), ""]
        for i, it in enumerate(shown, 1):
            lines.append(self._fmt_book(i, it))
        lines.append("")
        lines.append("下载请发送：/番茄 下载 <序号>，例如 /番茄 下载 1")

        ttl = int(self.config.get("search_cache_minutes", 10) or 0) * 60
        self._search_hits[umo] = (time.time() + max(60, ttl), list(shown))
        while len(self._search_hits) > 50:
            self._search_hits.popitem(last=False)

        return "\n".join(lines)

    async def _download_by_index(self, event, idx_text):
        umo = event.unified_msg_origin
        try:
            idx = int(str(idx_text).strip())
        except (TypeError, ValueError):
            return "用法：/番茄 下载 <序号>，序号来自上一次搜索结果"
        hit = self._search_hits.get(umo)
        if not hit:
            return "没有可用的搜索结果，请先 /番茄 搜索 <关键词>"
        expire, items = hit
        if time.time() > expire:
            self._search_hits.pop(umo, None)
            return "搜索结果已过期，请重新 /番茄 搜索 <关键词>"
        if idx < 1 or idx > len(items):
            return "序号超出范围，可选 1~%d" % len(items)

        item = items[idx - 1]
        raw = item.get("raw") or {}
        bid = str(item.get("book_id") or raw.get("book_id") or "").strip()
        if not bid:
            return "这条结果没有 book_id，无法下载。"
        title = str(raw.get("book_name") or item.get("title") or "").strip()
        if title:
            await self._say(umo, "已选择第 %d 本：《%s》，开始下载…" % (idx, title))
        return await self._submit(event, bid)

    async def _submit(self, event: AstrMessageEvent, text: str) -> str:
        bid, need = parse_book_id(text)
        if not bid and not need:
            return ("没识别到番茄链接。\n支持：\n"
                    "  https://changdunovel.com/t/xxxxx/\n"
                    "  https://fanqienovel.com/page/<id>\n"
                    "  纯 book_id")
        if not self._ready:
            return "后端尚未就绪：" + (self._err or "首次运行正在准备，请稍后重试")

        key = bid or text.strip()
        hit = self._cached(key)
        if hit:
            path, ts = hit
            await self._send_file(event.unified_msg_origin, path, os.path.basename(path))
            return "命中缓存（%d 分钟前下载过），已重新发送。" % int((time.time() - ts) // 60)

        for j in self._jobs.values():
            if j.key == key and j.state in ("queued", "running"):
                return "这本书已在队列中。\n" + self._status()

        active = [j for j in self._jobs.values() if j.state in ("queued", "running")]
        if len(active) >= int(self.config.get("max_queue", 3) or 3):
            return "队列已满（%d 本），请等当前任务完成。" % len(active)

        job = Job(key, event.unified_msg_origin, text.strip())
        self._jobs[key] = job
        # 清理超过 2 小时的终态任务，避免字典无限增长
        now = time.time()
        for k in [k for k, v in self._jobs.items() if v.state in ("done", "failed", "cancelled") and now - v.started > 7200]:
            self._jobs.pop(k, None)
        await self._queue.put(job)
        return ("已收到，开始处理（前面还有 %d 个）。\n"
                "整本抓取完成后会自动把 TXT 发到这里。" % max(0, self._queue.qsize() - 1))
