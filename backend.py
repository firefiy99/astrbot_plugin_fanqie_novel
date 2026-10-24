"""番茄下载器后端的 HTTP 客户端。

后端是 Rust 版 Web UI 的 REST API；插件只依赖这几个端点。
所有异常统一成 BackendError，便于上层给出可读的失败原因。
"""

import asyncio
import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import aiohttp


class BackendError(Exception):
    pass


class FanqieBackend:
    def __init__(self, base_url: str, password: str = "", timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.password = password or ""
        self.timeout = timeout

    def headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.password:
            h["x-tomato-password"] = self.password
        return h

    async def _req(self, method: str, path: str, **kw) -> Any:
        url = self.base_url + path
        to = aiohttp.ClientTimeout(total=self.timeout)
        try:
            async with aiohttp.ClientSession(timeout=to, headers=self.headers()) as s:
                async with s.request(method, url, **kw) as r:
                    body = await r.text()
                    if r.status in (401, 403):
                        raise BackendError("后端拒绝访问，密码不正确或后端已加锁")
                    if r.status == 429:
                        raise BackendError("后端已有任务在跑，请稍后再试")
                    if r.status == 404:
                        raise BackendError("接口不存在: " + path + "（后端版本可能不匹配）")
                    if r.status != 200:
                        raise BackendError(f"后端返回 {r.status}: {body[:180]}")
                    if not body.strip():
                        raise BackendError("后端返回空响应: " + path)
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError:
                        raise BackendError("后端返回非 JSON: " + body[:180])
        except aiohttp.ClientConnectorError as e:
            raise BackendError("无法连接到后端 " + self.base_url) from e
        except asyncio.TimeoutError as e:
            raise BackendError(f"后端响应超时（{self.timeout}s）") from e

    async def ping(self) -> bool:
        try:
            await self._req("GET", "/api/status")
            return True
        except BackendError:
            return False

    async def create_job(self, target: str) -> Dict[str, Any]:
        """target 可以是 book_id 或分享链接，后端会自己解析。"""
        return await self._req("POST", "/api/jobs", json={"book_id": target})

    async def list_jobs(self) -> List[Dict[str, Any]]:
        data = await self._req("GET", "/api/jobs")
        if isinstance(data, dict):
            for k in ("items", "jobs", "data"):
                if isinstance(data.get(k), list):
                    return data[k]
            return []
        return data if isinstance(data, list) else []

    async def job(self, job_id: int) -> Optional[Dict[str, Any]]:
        for j in await self.list_jobs():
            try:
                if int(j.get("id", -1)) == int(job_id):
                    return j
            except (TypeError, ValueError):
                continue
        return None

    async def library(self) -> List[Dict[str, Any]]:
        data = await self._req("GET", "/api/library")
        if isinstance(data, dict):
            for k in ("items", "books", "data"):
                if isinstance(data.get(k), list):
                    return data[k]
            return []
        return data if isinstance(data, list) else []

    def file_url(self, rel: str) -> str:
        return self.base_url + "/download/" + quote(str(rel).lstrip("/"), safe="/")
