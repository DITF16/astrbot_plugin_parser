import re
import asyncio
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Any
from urllib.parse import urlparse

import msgspec
from aiohttp import TCPConnector, ClientSession

from astrbot.api import logger
from ...config import PluginConfig
from ...cookie import CookieJar
from ..base import (
    BaseParser,
    ParseException,
    Platform,
    handle,
)
from ...download import auto_task

if TYPE_CHECKING:
    from ...data import ParseResult, Content


@dataclass(slots=True)
class ProbedVideo:
    """探测到的最佳视频信息"""
    url: str
    size: int
    headers: dict[str, str]


class DouyinParser(BaseParser):
    platform: ClassVar[Platform] = Platform(name="douyin", display_name="抖音")
    
    PLAY_RATIOS: ClassVar[tuple[str, ...]] = ("1080p", "720p", "540p")
    
    # 备用公开API（仅在官方路径失败或较慢时使用）
    BACKUP_APIS: ClassVar[list[str]] = [
        "https://api.douyin.wang/api/video",
        "https://api.douyin.wtf/api/video",
        "https://tikwm.com/api/",
    ]

    def __init__(self, config: PluginConfig, downloader):
        super().__init__(config, downloader)
        self.mycfg = config.parser.douyin
        self.cookiejar = CookieJar(config, self.mycfg, domain="douyin.com")
        
        # 可在 _conf_schema.json 和 default_template.json 中新增以下配置项
        self.use_race_mode: bool = getattr(self.mycfg, "use_race_mode", True)
        self.max_concurrent_parse: int = getattr(self.mycfg, "max_concurrent_parse", 3)
        self.download_chunk_size: int = getattr(self.mycfg, "download_chunk_size", 2 * 1024 * 1024)  # 2MB

        self._parse_sem = asyncio.Semaphore(self.max_concurrent_parse)
        self._probe_sem = asyncio.Semaphore(6)
        self._ttwid_cache: str | None = None
        self._ttwid_lock = asyncio.Lock()

        # 优化连接池，提升整体速度
        if not hasattr(self.session, "_connector") or self.session._connector is None:
            self.session._connector = TCPConnector(
                limit=20, ttl_dns_cache=300, keepalive_timeout=25
            )

        self._set_cookies()

    def _set_cookies(self, cookies_str: str = ""):
        cookies_str = cookies_str or self.cookiejar.cookies_str
        if cookies_str:
            self.ios_headers["Cookie"] = cookies_str
            self.android_headers["Cookie"] = cookies_str

    def _sync_headers(self, url: str) -> dict[str, str]:
        headers = self.ios_headers.copy()
        headers.pop("Cookie", None)
        if cookie := self.cookiejar.get_cookie_header_for_url(url):
            headers["Cookie"] = cookie
        return headers

    async def ensure_ttwid(self) -> None:
        async with self._ttwid_lock:
            if self._ttwid_cache:
                self.cookiejar.update_from_response([f"ttwid={self._ttwid_cache}"])
                self._set_cookies()
                return

            try:
                async with self.session.get(
                    "https://www.douyin.com/", headers=self.ios_headers, timeout=6
                ) as resp:
                    for c in resp.headers.getall("Set-Cookie", []):
                        if "ttwid=" in c:
                            ttwid = c.split("ttwid=")[1].split(";")[0]
                            self._ttwid_cache = ttwid
                            self.cookiejar.update_from_response([f"ttwid={ttwid}"])
                            self._set_cookies()
                            logger.debug("[抖音] ttwid 已缓存")
                            return
            except Exception:
                pass

    @handle("v.douyin", r"v\.douyin\.com/[a-zA-Z0-9_\-]+")
    async def _parse_short_link(self, searched: re.Match[str]):
        url = f"https://{searched.group(0)}"
        return await self.parse_with_redirect(url)

    @handle("", r"(?<![A-Za-z0-9_/=:%?&.-])(?P<vid>\d{18,20})(?!\d)")
    @handle("aweme_id", r"aweme_id[=:/\s]+(?P<vid>\d{10,})")
    @handle("aweme", r"aweme/(?P<vid>\d{10,})")
    @handle("douyin", r"douyin\.com/(?P<ty>video|note)/(?P<vid>\d+)")
    @handle("iesdouyin", r"iesdouyin\.com/share/(?P<ty>slides|video|note)/(?P<vid>\d+)")
    @handle("m.douyin", r"m\.douyin\.com/share/(?P<ty>slides|video|note)/(?P<vid>\d+)")
    @handle(
        "jingxuan.douyin",
        r"jingxuan\.douyin.com/m/(?P<ty>slides|video|note)/(?P<vid>\d+)",
    )
    async def _parse_douyin(self, searched: re.Match[str]):
        ty = searched.groupdict().get("ty") or "video"
        vid = searched.group("vid")
        async with self._parse_sem:
            await self.ensure_ttwid()
            if ty == "slides":
                return await self.parse_slides(vid)
            if self.use_race_mode:
                return await self._race_parse(vid)
            return await self._official_parse(vid)

    async def _race_parse(self, vid: str) -> "ParseResult":
        """并行竞速模式：官方路径 vs 备用API，谁先成功用谁（速度优先）"""
        tasks = {
            asyncio.create_task(self._official_parse(vid), name="official"),
            asyncio.create_task(self._backup_api_parse(vid), name="backup"),
        }
        
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        
        for task in pending:
            if not task.done():
                task.cancel()
        
        for task in done:
            if not task.exception():
                return task.result()
            logger.debug(f"[抖音] {task.get_name()} 路径失败: {task.exception()}")
        
        raise ParseException("官方路径与备用API均解析失败")

    async def _official_parse(self, vid: str) -> "ParseResult":
        """官方最优路径（速度最快）"""
        share_url = f"https://www.iesdouyin.com/share/video/{vid}/"
        logger.debug(f"[抖音][官方] 开始解析: {share_url}")

        headers = self._sync_headers(share_url)
        async with self.session.get(share_url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                raise ParseException(f"HTTP {resp.status}")
            html = await resp.text()
            self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
            self._set_cookies()

        if not (router := self._extract_router_data(html)):
            raise ParseException("提取 RouterData 失败")

        vd = router.get("video_data") or router
        desc = vd.get("desc", "抖音视频")
        cover = vd.get("cover_url")
        nickname = vd.get("author", {}).get("nickname", "抖音用户")
        avatar = vd.get("avatar_url")
        video_url = vd.get("video_url")

        if play_token := vd.get("play_token"):
            try:
                probed = await self.probe_video_url(play_token, share_url)
                video_url = probed.url
            except Exception as e:
                logger.debug(f"[抖音] 清晰度探测失败: {e}")

        if not video_url:
            raise ParseException("无法获取视频地址")

        content = self.create_video_content(
            video_url, cover, headers=self.ios_headers, timeout=30
        )
        author = self.create_author(nickname, avatar, headers=self.ios_headers)
        
        logger.info(f"[抖音][官方] 解析成功 | {desc[:25]}...")
        return self.result(title=desc, author=author, contents=[content])

    async def _backup_api_parse(self, vid: str) -> "ParseResult":
        """备用API路径（降级使用）"""
        for api_url in self.BACKUP_APIS:
            try:
                params = {"url": f"https://v.douyin.com/{vid}/"} if "tikwm" in api_url else {"video_id": vid}
                async with self.session.get(api_url, params=params, timeout=8) as resp:
                    data = await resp.json(content_type=None)

                item = data.get("data", data)
                video_url = item.get("play_url") or item.get("url") or item.get("video_url")
                if not video_url:
                    continue

                desc = item.get("title") or item.get("desc", "抖音视频")
                cover = item.get("cover") or item.get("thumbnail")
                nickname = item.get("author", {}).get("nickname", "抖音用户")
                avatar = item.get("avatar")

                content = self.create_video_content(video_url, cover, headers=self.ios_headers, timeout=30)
                author = self.create_author(nickname, avatar, headers=self.ios_headers)

                logger.info(f"[抖音][备用API] 解析成功 | {api_url}")
                return self.result(title=desc, author=author, contents=[content])
            except Exception as e:
                logger.debug(f"[抖音][备用] {api_url} 失败: {e}")
                continue
        raise ParseException("所有备用API均失败")

    def _extract_router_data(self, html: str) -> dict | None:
        patterns = [
            r'window\._ROUTER_DATA\s*=\s*({.+?});\s*</script>',
            r'"routerData"\s*:\s*({.+?})(?:,|"|\s)',
            r'JSON\.parse\((.+?)\)\s*;',
        ]
        for pat in patterns:
            if matched := re.search(pat, html, re.DOTALL):
                try:
                    json_str = matched.group(1).strip()
                    if json_str.startswith('"'):
                        json_str = json.loads(json_str)
                    data = msgspec.json.decode(json_str if isinstance(json_str, str) else json_str, type=dict)
                    return data.get("data", data) or data.get("video", {})
                except Exception:
                    continue
        return None

    async def probe_video_url(self, video_id: str, referer: str) -> ProbedVideo:
        async with self._probe_sem:
            tasks = [self._probe_single_ratio(video_id, ratio, referer) for ratio in self.PLAY_RATIOS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            valid = [r for r in results if isinstance(r, ProbedVideo)]
            if not valid:
                raise ParseException("清晰度探测失败")
            best = max(valid, key=lambda x: x.size)
            logger.debug(f"[抖音] 最佳清晰度: {best.size // 1024}KB")
            return best

    async def _probe_single_ratio(self, video_id: str, ratio: str, referer: str) -> ProbedVideo | None:
        url = f"https://aweme.snssdk.com/aweme/v1/play/?video_id={video_id}&ratio={ratio}"
        headers = self.ios_headers.copy()
        headers.update({"Referer": referer, "Range": "bytes=0-2097152"})

        try:
            async with self.session.get(url, headers=headers, allow_redirects=True, timeout=8) as resp:
                if resp.status >= 400:
                    return None
                size = int(resp.headers.get("Content-Length", 0))
                if size == 0 and (cr := resp.headers.get("Content-Range")):
                    if m := re.search(r"/(\d+)", cr):
                        size = int(m.group(1))
                if size > 0:
                    return ProbedVideo(str(resp.real_url), size, headers)
        except Exception:
            pass
        return None

    async def parse_with_redirect(self, url: str) -> "ParseResult":
        async with self.session.get(
            url, headers=self.ios_headers, allow_redirects=False, timeout=10
        ) as resp:
            self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
            self._set_cookies()
            redirect_url = resp.headers.get("Location", url)
        
        if redirect_url == url:
            raise ParseException("短链重定向失败")
        
        keyword, searched = self.search_url(redirect_url)
        return await self.parse(keyword, searched)

    async def parse_slides(self, video_id: str):
        """图集解析（保持与原插件完全一致）"""
        url = "https://www.iesdouyin.com/web/api/v2/aweme/slidesinfo/"
        params = {
            "aweme_ids": f"[{video_id}]",
            "request_source": "200",
        }
        logger.debug(f"[抖音] 请求参数: {params}")
        async with self.session.get(
            url, params=params, headers=self.android_headers
        ) as resp:
            logger.debug(f"[抖音] 幻灯片API响应状态码: {resp.status}")
            resp.raise_for_status()
            set_cookie_headers = resp.headers.getall("Set-Cookie", [])
            self.cookiejar.update_from_response(set_cookie_headers)
            self._set_cookies()

            from .slides import SlidesInfo
            response_text = await resp.read()
            logger.debug(f"[抖音] 幻灯片API响应体大小: {len(response_text)} 字节")
            slides_data = msgspec.json.decode(
                response_text, type=SlidesInfo
            ).aweme_details[0]

        logger.debug(
            f"[抖音] 幻灯片解析成功 - 作者: {slides_data.name}, 描述: {slides_data.desc[:50]}..."
        )
        contents = []

        if image_urls := slides_data.image_urls:
            logger.debug(f"[抖音] 检测到幻灯片图片，数量: {len(image_urls)}")
            contents.extend(
                self.create_image_contents(image_urls, headers=self.android_headers)
            )

        if dynamic_urls := slides_data.dynamic_urls:
            logger.debug(f"[抖音] 检测到幻灯片动态效果，数量: {len(dynamic_urls)}")
            contents.extend(
                self.create_dynamic_contents(dynamic_urls, headers=self.android_headers)
            )

        author = self.create_author(
            slides_data.name, slides_data.avatar_url, headers=self.android_headers
        )

        return self.result(
            title=slides_data.desc,
            author=author,
            contents=contents,
            timestamp=slides_data.create_time,
        )

    @auto_task
    async def download_task(self, content: "Content"):
        """优化下载：使用较大 chunk_size 减少 IO 调用次数"""
        if not content.url or not content.path:
            return
        logger.debug(f"[抖音] 开始下载: {content.url[:60]}...")
        try:
            await self.downloader.streamd(
                content.url,
                content.path,
                headers=content.headers or self.ios_headers,
                timeout=35,
                chunk_size=self.download_chunk_size,
            )
            logger.debug(f"[抖音] 下载完成 → {content.path.name}")
        except Exception as e:
            logger.error(f"[抖音] 下载失败: {e}")
            raise