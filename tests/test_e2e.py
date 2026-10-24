"""端到端：直接驱动 backend 客户端，验证任务流转与文件获取。"""
import asyncio, sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backend import BackendError, FanqieBackend

BASE = "http://127.0.0.1:18499"

async def main():
    be = FanqieBackend(BASE)
    print("1) ping:", await be.ping())
    d = await be.create_job("https://changdunovel.com/t/4geS0m5pPy0/")
    print("2) create_job:", d)
    jid = d["id"]
    try:
        await be.create_job("7490373272979115070")
        print("3) 并发限制: 未触发（异常）")
    except BackendError as e:
        print("3) 并发限制: 已触发")
    last = -1
    for _ in range(60):
        await asyncio.sleep(0.4)
        j = await be.job(jid)
        if not j: continue
        p = j.get("progress") or {}
        pct = int(p.get("saved_chapters",0)*100/max(p.get("chapter_total",1),1))
        if pct != last:
            print("   进度", pct, "%", j.get("state")); last = pct
        if j.get("state") == "done": break
    j = await be.job(jid)
    print("4) 最终:", j.get("state"), "|", j.get("title"))
    lib = await be.library()
    print("5) library:", lib)
    rel = lib[0]["path"]
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.get(be.file_url(rel)) as r:
            body = await r.text()
    print("6) 文件:", len(body), "字节 | 首行:", body.splitlines()[0])
    assert j.get("state") == "done" and "章" in body
    print("\n全部通过")

asyncio.run(main())
