"""模拟后端 REST API，用于端到端验证插件编排逻辑。"""
import asyncio, os, time
from aiohttp import web

PORT = 18499
D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_mockdata")
os.makedirs(D, exist_ok=True)
jobs, nid, TOTAL = {}, [1], 8

async def status(r): return web.json_response({"ok": True, "version": "mock"})
async def jobs_get(r):
    items = list(jobs.values())
    jid = r.query.get("id")
    if jid: items = [j for j in items if str(j["id"]) == str(jid)]
    return web.json_response({"items": items})

async def jobs_post(r):
    b = await r.json()
    if any(j["state"] in ("queued","running") for j in jobs.values()):
        return web.json_response({"error":"TOO_MANY_REQUESTS"}, status=429)
    i = nid[0]; nid[0] += 1
    raw = str(b.get("book_id","")).rstrip("/")
    t = "测试书-" + raw.replace("/","_")[-6:]
    jobs[i] = {"id": i, "title": t, "book_id": raw, "state": "queued",
               "progress": {"saved_chapters": 0, "chapter_total": TOTAL}}
    asyncio.create_task(run(i))
    return web.json_response({"id": i, "state": "queued"})

async def run(i):
    j = jobs[i]; j["state"] = "running"
    for k in range(1, TOTAL+1):
        await asyncio.sleep(0.3)
        j["progress"] = {"saved_chapters": k, "chapter_total": TOTAL}
    name = str(j["title"]) + ".txt"
    lines = ["《" + str(j["title"]) + "》", ""]
    for c in range(1, 31):
        lines += ["第" + str(c) + "章 标题", "", "这是第 " + str(c) + " 章正文。", ""]
    with open(os.path.join(D, name), "w", encoding="utf-8") as f:
        f.write(chr(10).join(lines))
    j["state"] = "done"; j["file"] = name

async def library(r):
    return web.json_response({"items": [{"title": j["title"], "path": j["file"], "book_id": j["book_id"]}
                                        for j in jobs.values() if j.get("state")=="done" and j.get("file")]})

async def dl(r):
    p = os.path.join(D, os.path.basename(r.match_info["path"]))
    return web.FileResponse(p) if os.path.exists(p) else web.Response(status=404)

app = web.Application()
app.router.add_get("/api/status", status)
app.router.add_get("/api/jobs", jobs_get)
app.router.add_post("/api/jobs", jobs_post)
app.router.add_get("/api/library", library)
app.router.add_get("/download/{path:.*}", dl)

if __name__ == "__main__":
    print("mock on", PORT, flush=True)
    web.run_app(app, host="127.0.0.1", port=PORT, print=None)
