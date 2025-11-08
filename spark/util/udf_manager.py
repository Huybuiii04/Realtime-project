"""
Module quản lý các User Defined Functions (UDFs) cho Spark Streaming.
"""
from urllib.parse import urlparse
from user_agents import parse


class UDFManager:
    """
    Lớp quản lý các UDFs để trích xuất thông tin từ URL, user agent, v.v.
    """
    
    @staticmethod
    def get_country(url):
        """
        Trích xuất mã quốc gia từ URL.
        
        :param url: URL cần phân tích
        :return: Mã quốc gia (ví dụ: 'vn', 'us') hoặc 'unknown'
        """
        if not url:
            return 'unknown'
        
        try:
            parsed_url = urlparse(url)
            domain_parts = parsed_url.netloc.split('.')
            
            # Lấy phần cuối cùng của domain (TLD)
            if len(domain_parts) >= 2:
                tld = domain_parts[-1].lower()
                # Kiểm tra xem TLD có phải là mã quốc gia không
                if len(tld) == 2:
                    return tld
                # Nếu có subdomain với mã quốc gia (ví dụ: example.com.vn)
                if len(domain_parts) >= 3:
                    country_code = domain_parts[-2].lower()
                    if len(country_code) == 2:
                        return country_code
            
            return 'unknown'
        except Exception:
            return 'unknown'
    
    @staticmethod
    def get_browser(user_agent_string):
        """
        Trích xuất tên trình duyệt từ chuỗi user agent.
        
        :param user_agent_string: Chuỗi user agent
        :return: Tên trình duyệt hoặc 'unknown'
        """
        if not user_agent_string:
            return 'unknown'
        
        try:
            user_agent = parse(user_agent_string)
            browser = user_agent.browser.family
            return browser if browser else 'unknown'
        except Exception:
            return 'unknown'
    
    @staticmethod
    def get_os(user_agent_string):
        """
        Trích xuất tên hệ điều hành từ chuỗi user agent.
        
        :param user_agent_string: Chuỗi user agent
        :return: Tên hệ điều hành hoặc 'unknown'
        """
        if not user_agent_string:
            return 'unknown'
        
        try:
            user_agent = parse(user_agent_string)
            os = user_agent.os.family
            return os if os else 'unknown'
        except Exception:
            return 'unknown'
