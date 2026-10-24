import asyncio, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backend_manager import BackendManager, detect_asset, find_free_port

class L:
    def info(self,m): print('[I]',m)
    def warning(self,m): print('[W]',m)
    def error(self,m): print('[E]',m)

async def main():
    print('1) 平台资产:', detect_asset())
    p1 = await find_free_port(18423)
    print('2) 空闲端口:', p1)
    srv = await asyncio.start_server(lambda r,w: None, '127.0.0.1', p1)
    p2 = await find_free_port(18423)
    print('3) 占用后:', p2, '(应不同)')
    srv.close(); await srv.wait_closed()
    with tempfile.TemporaryDirectory() as td:
        m = BackendManager(Path(td), L())
        cfg = Path(td)/'backend'/'config.yml'
        cfg.parent.mkdir(parents=True)
        cfg.write_text('novel_format: epub\nmax_workers: 1\n', encoding='utf-8')
        m._force_txt(cfg)
        got = cfg.read_text(encoding='utf-8')
        print('4) 配置:', got.strip().replace(chr(10),' | '))
        assert 'novel_format: txt' in got and 'max_workers' in got
    print('\nmanager 通过')

asyncio.run(main())
