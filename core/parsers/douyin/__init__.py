import re
import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urlparse

import msgspec
from aiohttp import ClientError, TCPConnector

from astrbot.api import logger
from ...config import PluginConfig
from ...cookie import CookieJar
from ..base import (
    BaseParser,
    Downloader,
    ParseException,
    Platform,
    handle,
)

if TYPE_CHECKING:
    from ...data import ParseResult, Content

@dataclass(slots=True)
class ProbedVideo:
    url: str
    size: int
    headers: dict[str, str]


class DouyinParser(BaseParser):
    platform: ClassVar[Platform] = Platform(name="douyin", display_name="抖音")
    PLAY_RATIOS: ClassVar[tuple[str, ...]] = ("1080p", "720p", "540p", "360p")
    TTWID_REGISTER_URL: ClassVar[str] = "https://ttwid.bytedance.com/ttwid/union/register/"
    
    # 备用公开API（仅在官方路径失败或较慢时使用）
    BACKUP_APIS: ClassVar[list[str]] = [
        "https://api.douyin.wang/api/video",
        "https://api.douyin.wtf/api/video",
        "https://tikwm.com/api/",
    ]

    def __init__(self, config: PluginConfig, downloader: Downloader):
        super().__init__(config, downloader)
        self.mycfg = config.parser.douyin
        self.cookiejar = CookieJar(config, self.mycfg, domain="douyin.com")
        
        # 优化配置项
        self.use_race_mode: bool = getattr(self.mycfg, "use_race_mode", True)
        self.download_chunk_size: int = getattr(self.mycfg, "download_chunk_size", 2 * 1024 * 1024)

        # 优化连接池，提升整体速度
        if not hasattr(self.session, "_connector") or self.session._connector is None:
            self.session._connector = TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=25)

        self._set_cookies()

    def _set_cookies(self, cookies_str: str = ""):
        cookies_str = cookies_str or self.cookiejar.cookies_str
        if cookies_str:
            self.ios_headers["Cookie"] = cookies_str
            self.android_headers["Cookie"] = cookies_str

    def _sync_headers_for_url(self, url: str) -> dict[str, str]:
        headers = self.ios_headers.copy()
        headers.pop("Cookie", None)
        if cookies_str := self.cookiejar.get_cookie_header_for_url(url):
            headers["Cookie"] = cookies_str
        elif self._is_iesdouyin_url(url):
            if cookies_str := self.cookiejar.get_cookie_header(domain="iesdouyin.com"):
                headers["Cookie"] = cookies_str
        return headers

    @staticmethod
    def _is_iesdouyin_url(url: str) -> bool:
        hostname = urlparse(url).hostname or ""
        return hostname == "iesdouyin.com" or hostname.endswith(".iesdouyin.com")

    def _has_ttwid(self) -> bool:
        cookies = self.cookiejar.get(domain="iesdouyin.com") or {}
        return bool(cookies.get("ttwid"))

    @staticmethod
    def _build_iesdouyin_url(ty: str, vid: str) -> str:
        return f"https://www.iesdouyin.com/share/{ty}/{vid}/"

    async def ensure_ttwid(self) -> None:
        """原版最稳的 ttwid 注册逻辑"""
        if self._has_ttwid():
            return
        logger.debug("[抖音] 当前缺少匿名 ttwid，尝试注册")
        headers = self.ios_headers.copy()
        headers.update({"Content-Type": "application/json", "Referer": "https://www.iesdouyin.com/"})
        payload = {"region": "cn", "aid": 1768, "needFid": False, "service": "www.iesdouyin.com", "union": True, "fid": ""}
        try:
            async with self.session.post(self.TTWID_REGISTER_URL, json=payload, headers=headers) as resp:
                if resp.status >= 400:
                    raise ParseException(f"ttwid register status: {resp.status}")
                self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
                self._set_cookies()
                body = await resp.json(content_type=None)
        except Exception as e:
            raise ParseException("ttwid register failed") from e

        if not isinstance(body, dict):
            raise ParseException("ttwid register returned invalid body")

        if callback_url := body.get("redirect_url"):
            callback_headers = self._sync_headers_for_url(callback_url)
            callback_headers["Referer"] = "https://www.iesdouyin.com/"
            try:
                async with self.session.get(callback_url, headers=callback_headers, allow_redirects=False) as resp:
                    self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
                    self._set_cookies()
            except Exception as e:
                raise ParseException("ttwid callback failed") from e

    @handle("v.douyin", r"v\.douyin\.com/[a-zA-Z0-9_\-]+")
    @handle("jx.douyin", r"jx\.douyin\.com/[a-zA-Z0-9_\-]+")
    async def _parse_short_link(self, searched: re.Match[str]):
        url = f"https://{searched.group(0)}"
        return await self.parse_with_redirect(url)

    @handle("", r"(?<![A-Za-z0-9_/=:%?&.-])(?P<vid>\d{18,20})(?!\d)")
    @handle("aweme_id", r"aweme_id[=:/\s]+(?P<vid>\d{10,})")
    @handle("aweme", r"aweme/(?P<vid>\d{10,})")
    @handle("douyin", r"douyin\.com/(?P<ty>video|note)/(?P<vid>\d+)")
    @handle("iesdouyin", r"iesdouyin\.com/share/(?P<ty>slides|video|note)/(?P<vid>\d+)")
    @handle("m.douyin", r"m\.douyin\.com/share/(?P<ty>slides|video|note)/(?P<vid>\d+)")
    @handle("jingxuan.douyin", r"jingxuan\.douyin.com/m/(?P<ty>slides|video|note)/(?P<vid>\d+)")
    async def _parse_douyin(self, searched: re.Match[str]):
        ty = searched.groupdict().get("ty") or "video"
        vid = searched.group("vid")
        logger.debug(f"[抖音] 解析类型: {ty}, ID: {vid}")
        
        if ty == "slides":
            return await self.parse_slides(vid)

        await self.ensure_ttwid()
        
        # 竞速模式分发
        if self.use_race_mode:
            return await self._race_parse(ty, vid)
        return await self._official_parse(ty, vid)

    async def _race_parse(self, ty: str, vid: str) -> "ParseResult":
        """保留竞速模式：官方与备用API谁快用谁"""
        tasks = {
            asyncio.create_task(self._official_parse(ty, vid), name="official"),
            asyncio.create_task(self._backup_api_parse(vid), name="backup"),
        }
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        
        for task in pending:
            task.cancel()
            
        for task in done:
            if not task.exception():
                return task.result()
            logger.debug(f"[抖音] {task.get_name()} 路径失败: {task.exception()}")
            
        raise ParseException("官方路径与备用API均解析失败")

    async def _official_parse(self, ty: str, vid: str) -> "ParseResult":
        """使用原版最稳的 RouterData 解析逻辑"""
        url = self._build_iesdouyin_url(ty, vid)
        share_headers = self._sync_headers_for_url(url)
        async with self.session.get(url, headers=share_headers, allow_redirects=False) as resp:
            if resp.status != 200:
                raise ParseException(f"status: {resp.status}")
            text = await resp.text()
            self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
            self._set_cookies()

        pattern = re.compile(r"window\._ROUTER_DATA\s*=\s*(.*?)</script>", flags=re.DOTALL)
        matched = pattern.search(text)
        if not matched or not matched.group(1):
            raise ParseException("can't find _ROUTER_DATA in html")

        from .video import RouterData
        video_data = msgspec.json.decode(matched.group(1).strip(), type=RouterData).video_data
        
        contents = []
        if image_urls := video_data.image_urls:
            contents.extend(self.create_image_contents(image_urls, headers=self.ios_headers))
        elif video_data.video:
            cover_url = video_data.cover_url
            duration = video_data.video.duration if video_data.video else 0
            video_headers = self._build_media_headers(url)
            video_url = None
            if play_token := video_data.play_token:
                try:
                    probed = await self.probe_video_url(play_token, url)
                    video_url, video_headers = probed.url, probed.headers
                except Exception as e:
                    logger.warning(f"[抖音] play 端点探测失败，回退 play_addr: {e}")
            video_url = video_url or video_data.video_url
            if video_url:
                contents.append(self.create_video_content(video_url, cover_url, duration, headers=video_headers))

        author = self.create_author(video_data.author.nickname, video_data.avatar_url, headers=self.ios_headers)
        return self.result(title=video_data.desc, author=author, contents=contents, timestamp=video_data.create_time)

    async def _backup_api_parse(self, vid: str) -> "ParseResult":
        """备用公开API，防官方风控"""
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
                return self.result(title=desc, author=author, contents=[content])
            except Exception:
                continue
        raise ParseException("备用API均失败")

    async def parse_with_redirect(self, url: str) -> "ParseResult":
        async with self.session.get(url, headers=self.ios_headers, allow_redirects=False) as resp:
            self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
            self._set_cookies()
            redirect_url = resp.headers.get("Location", url) if resp.status in (301, 302, 303, 307, 308) else url
        if redirect_url == url:
            raise ParseException(f"无法重定向 URL: {url}")
        keyword, searched = self.search_url(redirect_url)
        return await self.parse(keyword, searched)

    def _build_media_headers(self, referer: str) -> dict[str, str]:
        headers = self.ios_headers.copy()
        headers.pop("Cookie", None)
        headers["Referer"] = referer
        return headers

    async def probe_video_url(self, video_id: str, referer: str) -> ProbedVideo:
        probed_by_size: dict[int, ProbedVideo] = {}
        for ratio in self.PLAY_RATIOS:
            play_url = f"https://aweme.snssdk.com/aweme/v1/play/?video_id={video_id}&ratio={ratio}"
            headers = self._build_media_headers(referer)
            headers["Range"] = "bytes=0-1"
            try:
                async with self.session.get(play_url, headers=headers, allow_redirects=True) as resp:
                    if resp.status >= 400:
                        continue
                    size = self._extract_response_size(resp.headers)
                    if size > 0:
                        probed_by_size[size] = ProbedVideo(str(resp.url), size, self._build_media_headers(referer))
            except Exception:
                continue
        if not probed_by_size:
            raise ParseException("can't probe play endpoint")
        return max(probed_by_size.values(), key=lambda item: item.size)

    @staticmethod
    def _extract_response_size(headers) -> int:
        if cr := headers.get("Content-Range"):
            if m := re.search(r"/(\d+)\s*$", cr):
                return int(m.group(1))
        if cl := headers.get("Content-Length"):
            try: return int(cl)
            except ValueError: pass
        return 0

    async def parse_slides(self, video_id: str):
        """原版幻灯片解析"""
        url = "https://www.iesdouyin.com/web/api/v2/aweme/slidesinfo/"
        params = {"aweme_ids": f"[{video_id}]", "request_source": "200"}
        async with self.session.get(url, params=params, headers=self.android_headers) as resp:
            resp.raise_for_status()
            self.cookiejar.update_from_response(resp.headers.getall("Set-Cookie", []))
            self._set_cookies()
            from .slides import SlidesInfo
            slides_data = msgspec.json.decode(await resp.read(), type=SlidesInfo).aweme_details[0]

        contents = []
        if image_urls := slides_data.image_urls:
            contents.extend(self.create_image_contents(image_urls, headers=self.android_headers))
        if dynamic_urls := slides_data.dynamic_urls:
            contents.extend(self.create_dynamic_contents(dynamic_urls, headers=self.android_headers))
        author = self.create_author(slides_data.name, slides_data.avatar_url, headers=self.android_headers)
        return self.result(title=slides_data.desc, author=author, contents=contents, timestamp=slides_data.create_time)
