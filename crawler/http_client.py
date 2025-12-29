import http.client
import json
import time
import ssl
import gzip
import zlib
from io import BytesIO
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlparse, urlencode
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"


@dataclass
class HttpResponse:
    status_code: int
    headers: Dict[str, str]
    body: str
    json_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300
    
    @property
    def is_rate_limited(self) -> bool:
        return self.status_code == 429


class RateLimiter:
    def __init__(self, min_delay: float = 2.0):
        self.min_delay = min_delay
        self.last_request_time: float = 0
        self.request_count: int = 0
        
    def wait_if_needed(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            sleep_time = self.min_delay - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
    def record_request(self):
        self.last_request_time = time.time()
        self.request_count += 1


class RedditHttpClient:
    def __init__(
        self,
        user_agent: str,
        request_delay: float = 2.0,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        timeout: int = 30
    ):
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.rate_limiter = RateLimiter(min_delay=request_delay)
        
        # SSL context for HTTPS
        self.ssl_context = ssl.create_default_context()
        
        # Track request statistics
        self.stats = {
            "requests": 0,
            "successful": 0,
            "failed": 0,
            "retries": 0,
            "rate_limited": 0
        }
    
    def _get_connection(self, host: str) -> http.client.HTTPSConnection:
        return http.client.HTTPSConnection(
            host,
            timeout=self.timeout,
            context=self.ssl_context
        )
    
    def _build_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers
    
    def _decompress_response(self, data: bytes, encoding: str) -> str:
        try:
            if encoding == "gzip":
                return gzip.GzipFile(fileobj=BytesIO(data)).read().decode("utf-8")
            elif encoding == "deflate":
                return zlib.decompress(data).decode("utf-8")
            else:
                return data.decode("utf-8")
        except Exception as e:
            logger.warning(f"Decompression failed: {e}, trying raw decode")
            return data.decode("utf-8", errors="replace")
    
    def _parse_response(self, response: http.client.HTTPResponse) -> HttpResponse:  
        try:
            headers = {k.lower(): v for k, v in response.getheaders()}
            
            raw_data = response.read()
            content_encoding = headers.get("content-encoding", "")
            body = self._decompress_response(raw_data, content_encoding)
            
            json_data = None
            if body:
                try:
                    json_data = json.loads(body)
                except json.JSONDecodeError:
                    pass
            
            return HttpResponse(
                status_code=response.status,
                headers=headers,
                body=body,
                json_data=json_data
            )
        except Exception as e:
            return HttpResponse(
                status_code=response.status if response else 0,
                headers={},
                body="",
                error=str(e)
            )
    
    def request(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 0
    ) -> HttpResponse:
        self.rate_limiter.wait_if_needed()
        
        parsed = urlparse(url)
        host = parsed.netloc
        path = parsed.path
        
        if params:
            query = urlencode(params)
            if parsed.query:
                path = f"{path}?{parsed.query}&{query}"
            else:
                path = f"{path}?{query}"
        elif parsed.query:
            path = f"{path}?{parsed.query}"
        
        request_headers = self._build_headers(headers)
        
        logger.debug(f"Requesting: {method.value} {host}{path}")
        
        try:
            conn = self._get_connection(host)
            conn.request(method.value, path, headers=request_headers)
            
            self.rate_limiter.record_request()
            self.stats["requests"] += 1
            
            response = conn.getresponse()
            result = self._parse_response(response)
            conn.close()
            
            if result.is_rate_limited:
                self.stats["rate_limited"] += 1
                if retry_count < self.max_retries:
                    retry_after = float(result.headers.get("retry-after", self.retry_delay * 2))
                    logger.warning(f"Rate limited. Waiting {retry_after}s before retry...")
                    time.sleep(retry_after)
                    return self.request(url, method, params, headers, retry_count + 1)
            
            if result.status_code >= 500 and retry_count < self.max_retries:
                self.stats["retries"] += 1
                logger.warning(f"Server error {result.status_code}. Retry {retry_count + 1}/{self.max_retries}")
                time.sleep(self.retry_delay)
                return self.request(url, method, params, headers, retry_count + 1)
            
            if result.is_success:
                self.stats["successful"] += 1
            else:
                self.stats["failed"] += 1
                logger.error(f"Request failed: {result.status_code} - {result.body[:200]}")
            
            return result
            
        except Exception as e:
            logger.error(f"Request exception: {e}")
            self.stats["failed"] += 1
            
            if retry_count < self.max_retries:
                self.stats["retries"] += 1
                logger.warning(f"Connection error. Retry {retry_count + 1}/{self.max_retries}")
                time.sleep(self.retry_delay)
                return self.request(url, method, params, headers, retry_count + 1)
            
            return HttpResponse(
                status_code=0,
                headers={},
                body="",
                error=str(e)
            )
    
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> HttpResponse:
        return self.request(url, HttpMethod.GET, params, headers)
    
    def get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        response = self.get(url, params)
        if response.is_success and response.json_data:
            return response.json_data, None
        return None, response.error or f"HTTP {response.status_code}: {response.body[:200]}"
    
    def get_stats(self) -> Dict[str, int]:      
        return self.stats.copy()
    
    def reset_stats(self):
        self.stats = {
            "requests": 0,
            "successful": 0,
            "failed": 0,
            "retries": 0,
            "rate_limited": 0
        }

