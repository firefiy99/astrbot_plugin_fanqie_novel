"""下载器后端的自动管理：准备二进制、选端口、拉起、看护、退出清理。

目标是「装完即用」：用户不需要手动安装任何东西。
若 bin/ 下已有二进制则直接使用，不重复下载。
"""

import asyncio
import os
import platform
import subprocess
import stat
from pathlib import Path
from typing import Optional

import aiohttp

VERSION = "v2.4.16"
REPO = "zhongbai2333/Tomato-Novel-Downloader"

ASSET_MAP = {
    ("Linux", "x86_64"): "TomatoNovelDownloader-Linux_amd64-{v}",
    ("Linux", "aarch64"): "TomatoNovelDownloader-Linux_arm64-{v}",
    ("Linux", "armv7l"): "TomatoNovelDownloader-Android_arm32-{v}",
    ("Darwin", "x86_64"): "TomatoNovelDownloader-macOS_amd64-{v}",
    ("Darwin", "arm64"): "TomatoNovelDownloader-macOS_arm64-{v}",
    ("Windows", "AMD64"): "TomatoNovelDownloader-Win64-{v}.exe",
}

MIRRORS = [
    "https://ghfast.top/https://github.com/{repo}/releases/download/{ver}/{asset}",
    "https://gh-proxy.com/https://github.com/{repo}/releases/download/{ver}/{asset}",
    "https://github.com/{repo}/releases/download/{ver}/{asset}",
]


class BackendStartError(Exception):
    pass


def detect_asset() -> Optional[str]:
    pat = ASSET_MAP.get((platform.system(), platform.machine()))
    return pat.format(v=VERSION) if pat else None


async def download_binary(dest: Path, logger) -> bool:
    asset = detect_asset()
    if not asset:
        logger.error("[fanqie] 不支持的平台: %s %s" % (platform.system(), platform.machine()))
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    timeout = aiohttp.ClientTimeout(total=2400, sock_read=120)

    for idx, mirror in enumerate(MIRRORS, 1):
        url = mirror.format(repo=REPO, ver=VERSION, asset=asset)
        logger.info("[fanqie] 获取后端 (%d/%d)" % (idx, len(MIRRORS)))
        try:
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        logger.warning("[fanqie] 镜像返回 %s，换下一个" % resp.status)
                        continue
                    total = int(resp.headers.get("Content-Length") or 0)
                    got = 0
                    with open(tmp, "wb") as f:
                        async for chunk in resp.content.iter_chunked(262144):
                            f.write(chunk)
                            got += len(chunk)
                    if total and got < total:
                        logger.warning("[fanqie] 下载不完整 %d/%d" % (got, total))
                        tmp.unlink(missing_ok=True)
                        continue
                    if got < 1000000:
                        logger.warning("[fanqie] 文件异常小 %d 字节" % got)
                        tmp.unlink(missing_ok=True)
                        continue
            os.replace(tmp, dest)
            st = os.stat(dest)
            os.chmod(dest, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            logger.info("[fanqie] 后端就绪: %d 字节" % got)
            return True
        except Exception as e:
            logger.warning("[fanqie] 镜像失败: %s" % type(e).__name__)
            tmp.unlink(missing_ok=True)
            continue
    return False


async def find_free_port(start: int = 18423, tries: int = 40) -> int:
    for port in range(start, start + tries):
        try:
            _, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=0.4)
            w.close()
            continue
        except Exception:
            return port
    raise BackendStartError("从 %d 起找不到空闲端口" % start)


class BackendManager:
    def __init__(self, data_dir: Path, logger):
        self.data_dir = data_dir
        self.logger = logger
        self.bin_dir = data_dir / "bin"
        self.home = data_dir / "backend"
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.port: Optional[int] = None
        self._watch: Optional[asyncio.Task] = None
        self._stopping = False

    def binary_path(self) -> Path:
        name = "tomato.exe" if platform.system() == "Windows" else "tomato"
        return self.bin_dir / name

    async def prepare(self) -> Path:
        p = self.binary_path()
        if p.exists() and p.stat().st_size > 1000000:
            self.logger.info("[fanqie] 使用已有后端")
            if platform.system() != "Windows":
                st = os.stat(p)
                if not st.st_mode & stat.S_IEXEC:
                    os.chmod(p, st.st_mode | stat.S_IEXEC)
            return p
        if not await download_binary(p, self.logger):
            raise BackendStartError(
                "后端准备失败。可手动下载放到: " + str(p)
                + " | 下载页: https://github.com/" + REPO + "/releases/tag/" + VERSION
            )
        return p

    def _force_txt(self, cfg: Path):
        try:
            if cfg.exists():
                out = []
                for ln in cfg.read_text(encoding="utf-8").splitlines():
                    if ln.strip().startswith("novel_format:"):
                        out.append("novel_format: txt")
                    else:
                        out.append(ln)
                cfg.write_text("\n".join(out) + "\n", encoding="utf-8")
            else:
                cfg.parent.mkdir(parents=True, exist_ok=True)
                cfg.write_text("novel_format: txt\n", encoding="utf-8")
            # 固定保存目录，避免文件散落到工作目录
            text = cfg.read_text(encoding="utf-8")
            lines = [x for x in text.splitlines() if not x.strip().startswith("save_path:")]
            lib = self.bin_dir.parent / "library"
            lib.mkdir(parents=True, exist_ok=True)
            lines.append("save_path: " + str(lib))
            cfg.write_text(chr(10).join(lines) + chr(10), encoding="utf-8")
        except Exception as e:
            self.logger.warning("[fanqie] 写配置失败: %s" % e)

    async def start(self) -> str:
        binary = await self.prepare()
        self.home.mkdir(parents=True, exist_ok=True)
        self.port = await find_free_port(18423)
        self._force_txt(self.home / "config.yml")

        env = os.environ.copy()
        env["TOMATO_WEB_ADDR"] = "127.0.0.1:%d" % self.port
        env.pop("TOMATO_WEB_PASSWORD", None)

        logp = self.home / "backend.out"
        self.logger.info("[fanqie] 启动后端 127.0.0.1:%d" % self.port)
        with open(logp, "ab") as lf:
            self.proc = await asyncio.create_subprocess_exec(
                str(binary), "--server", "--data-dir", str(self.home),
                stdout=lf, stderr=lf, env=env, stdin=lf,
            )

        base = "http://127.0.0.1:%d" % self.port
        if not await self._wait_ready(base, 60):
            raise BackendStartError("后端 60 秒内未就绪，日志: " + str(logp))
        self._stopping = False
        self._watch = asyncio.create_task(self._watchdog())
        return base

    async def _wait_ready(self, base: str, timeout: int) -> bool:
        loop = asyncio.get_event_loop()
        end = loop.time() + timeout
        to = aiohttp.ClientTimeout(total=3)
        while loop.time() < end:
            if self.proc and self.proc.returncode is not None:
                return False
            try:
                async with aiohttp.ClientSession(timeout=to) as s:
                    async with s.get(base + "/api/status") as r:
                        if r.status == 200:
                            await r.text()
                            return True
            except Exception:
                pass
            await asyncio.sleep(1)
        return False

    async def _watchdog(self):
        n = 0
        while not self._stopping and n < 3:
            await asyncio.sleep(5)
            if self._stopping:
                return
            if self.proc and self.proc.returncode is not None:
                n += 1
                self.logger.warning("[fanqie] 后端退出(%s)，重启 %d/3" % (self.proc.returncode, n))
                try:
                    await self.start()
                except Exception as e:
                    self.logger.error("[fanqie] 重启失败: %s" % e)
                    return

    async def stop(self):
        self._stopping = True
        if self._watch:
            self._watch.cancel()
            try:
                await self._watch
            except BaseException:
                pass
        if self.proc and self.proc.returncode is None:
            self.logger.info("[fanqie] 停止后端")
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), timeout=8)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
            except Exception as e:
                self.logger.warning("[fanqie] 停止异常: %s" % e)
