import json
import random
import aiohttp
import asyncio
from pathlib import Path


class CharacterManager:
    """使用 animewifex 图床数据源的角色管理器"""

    # animewifex 图床配置
    IMAGE_BASE_URL = "https://cdn.jsdmirror.com/gh/monbed/wife@main"
    IMAGE_LIST_URL = "https://animewife.dpdns.org/list.txt"

    def __init__(self) -> None:
        self._characters: list[dict] | None = None
        self._id_index: dict[int, dict] | None = None

    async def _fetch_image_list(self) -> list[str]:
        """从远程获取图片列表"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.IMAGE_LIST_URL) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        return [line.strip() for line in text.splitlines() if line.strip()]
        except Exception:
            pass
        return []

    def _parse_character(self, filepath: str) -> dict | None:
        """解析图片路径为角色数据
        格式: img1/作品名!角色名.jpg
        """
        try:
            # 移除路径前缀和扩展名
            filename = filepath.split("/")[-1]
            name_part = filename.rsplit(".", 1)[0]  # 移除扩展名
            
            if "!" in name_part:
                source, char_name = name_part.split("!", 1)
            else:
                source = "未知作品"
                char_name = name_part
            
            # 生成唯一ID（基于文件名哈希）
            char_id = hash(filepath) % 10000000
            
            return {
                "id": char_id,
                "name": char_name,
                "source": source,  # 作品名
                "image_url": f"{self.IMAGE_BASE_URL}/{filepath}",
                "filepath": filepath,
                "gender": "女",  # 默认为女性角色
                "heat": 0
            }
        except Exception:
            return None

    async def load_characters_async(self) -> list[dict]:
        """异步加载角色数据"""
        if self._characters is None:
            file_list = await self._fetch_image_list()
            self._characters = []
            for filepath in file_list:
                char = self._parse_character(filepath)
                if char:
                    self._characters.append(char)
            
            # 构建ID索引
            self._id_index = {
                c.get("id"): c
                for c in self._characters
                if isinstance(c, dict) and c.get("id") is not None
            }
        return self._characters

    def load_characters(self) -> list[dict]:
        """同步加载（用于初始化）"""
        if self._characters is None:
            # 尝试异步加载，如果失败则返回空列表
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 如果事件循环正在运行，创建新任务
                    future = asyncio.ensure_future(self.load_characters_async())
                    # 等待结果（可能会阻塞，但初始化时应该没问题）
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        self._characters = pool.submit(asyncio.run, self.load_characters_async()).result()
                else:
                    self._characters = loop.run_until_complete(self.load_characters_async())
            except Exception:
                self._characters = []
        return self._characters

    def get_random_character(self, limit=None):
        """随机获取一个角色"""
        chars = self.load_characters()
        if not chars:
            return None
        if limit and isinstance(limit, int) and limit > 0:
            chars = chars[:limit]
        return random.choice(chars)

    def get_character_by_id(self, id):
        """根据ID获取角色"""
        try:
            cid = int(id)
            if self._id_index is None:
                self.load_characters()
            return self._id_index.get(cid)
        except:
            return None

    def search_characters_by_name(self, keyword: str) -> list[dict]:
        """根据角色名搜索"""
        if not keyword:
            return []
        key_lower = str(keyword).lower()
        chars = self.load_characters()
        if not chars:
            return []
        return [c for c in chars if key_lower in str(c.get("name", "")).lower()]

