"""
Module quản lý cấu hình cho Spark application.
"""
import configparser
import os


class Config:
    """
    Lớp đọc và quản lý cấu hình từ file spark.conf
    """
    
    def __init__(self, config_file='spark.conf'):
        """
        Khởi tạo đối tượng Config.
        
        :param config_file: Đường dẫn đến file cấu hình
        """
        self.config = configparser.ConfigParser()
        
        # Tìm file config
        if os.path.exists(config_file):
            config_path = config_file
        else:
            # Thử tìm trong thư mục hiện tại
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(os.path.dirname(current_dir), config_file)
        
        if os.path.exists(config_path):
            self.config.read(config_path)
        else:
            raise FileNotFoundError(f"Config file not found: {config_file}")
    
    def _get_section_conf(self, section):
        """
        Lấy tất cả cấu hình từ một section.
        
        :param section: Tên section trong file config
        :return: Dictionary chứa các cấu hình
        """
        if not self.config.has_section(section):
            raise ValueError(f"Section '{section}' not found in config file")
        
        # Lấy tất cả các options trong section
        options = {}
        for key, value in self.config.items(section):
            # Xử lý biến môi trường trong format ${VAR_NAME:default_value}
            if value.startswith('${') and value.endswith('}'):
                # Loại bỏ ${ và }
                var_part = value[2:-1]
                
                # Tách tên biến và giá trị mặc định
                if ':' in var_part:
                    var_name, default_value = var_part.split(':', 1)
                    value = os.getenv(var_name, default_value)
                else:
                    var_name = var_part
                    value = os.getenv(var_name, '')
            
            options[key] = value
        
        return options
    
    def get_spark_conf(self):
        """Lấy cấu hình Spark."""
        return self._get_section_conf('SPARK')
    
    def get_kafka_conf(self):
        """Lấy cấu hình Kafka."""
        return self._get_section_conf('KAFKA')
    
    def get_postgres_conf(self):
        """Lấy cấu hình PostgreSQL."""
        return self._get_section_conf('POSTGRES')
