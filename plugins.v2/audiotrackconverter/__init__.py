"""音频轨道转换器插件 - 最小可运行版本"""
from typing import List, Dict, Any, Tuple
from app.plugins import _PluginBase
import logging

logger = logging.getLogger(__name__)


class AudioTrackConverter(_PluginBase):
    """音频轨道转换插件"""
    
    # 插件元数据
    plugin_name = "音频轨道转换器"
    plugin_version = "1.0.0"
    plugin_author = "User"
    plugin_desc = "监控视频目录，将EAC3/AC3单音轨转换为AAC立体声外挂音轨"
    plugin_icon = "Audiobookshelf_A.png"
    plugin_order = 100
    plugin_config_prefix = "audiotrackconverter_"
    
    def __init__(self):
        super().__init__()
        self._enabled = False
        logger.info("✅ AudioTrackConverter 类实例化成功")
    
    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        logger.info(f"✅ 插件初始化完成，启用状态: {self._enabled}")
    
    def get_state(self) -> bool:
        """获取插件状态"""
        return self._enabled
    
    def get_api(self) -> List[Dict[str, Any]]:
        """注册API接口"""
        return []
    
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页面"""
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
        }
    
    def get_page(self) -> List[dict]:
        """插件状态页面"""
        return [
            {
                'component': 'VAlert',
                'props': {
                    'type': 'success' if self._enabled else 'info',
                    'text': f'音频轨道转换器 - {"已启用" if self._enabled else "未启用"}'
                }
            }
        ]
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册命令"""
        return []
    
    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务"""
        if not self.get_state():
            return []
        return []
    
    def stop_service(self):
        """停止服务"""
        logger.info("⏹️ 插件已停止")
        self._enabled = False
