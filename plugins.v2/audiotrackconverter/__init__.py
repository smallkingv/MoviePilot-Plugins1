from app.core.plugin import _PluginBase
from app.core.event import eventmanager, Event
from app.schemas.types import EventType
from pathlib import Path
import subprocess
import json
import re
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class AudioTrackConverterPlugin(_PluginBase):
    """音频轨道转换插件 - 将EAC3/AC3单音轨转换为AAC外挂音轨"""
    
    # 插件元数据
    plugin_name = "音频轨道转换器"
    plugin_version = "1.0.0"
    plugin_author = "User"
    plugin_desc = "监控视频目录，将EAC3/AC3单音轨转换为AAC立体声外挂音轨"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/audio.png"
    plugin_order = 100
    
    def __init__(self):
        super().__init__()
        self._config = {}
        self._watch_dirs = []
        self._enabled = False
        
    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        if config:
            self._config = config
            self._enabled = config.get("enabled", False)
            # 解析监控目录列表（支持多行输入）
            dirs_str = config.get("watch_dirs", "")
            self._watch_dirs = [
                Path(d.strip()) 
                for d in dirs_str.split("\n") 
                if d.strip()
            ]
            logger.info(f"插件初始化完成，监控目录: {self._watch_dirs}")
    
    def get_state(self) -> bool:
        """获取插件状态"""
        return self._enabled
    
    def get_command(self) -> List[Dict[str, Any]]:
        """注册命令"""
        return [{
            "cmd": "/audio_convert_scan",
            "event": EventType.PluginAction,
            "desc": "手动扫描音频轨道",
            "category": "音频处理",
            "data": {"action": "audio_convert_scan"}
        }]
    
    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务"""
        if not self._enabled or not self._watch_dirs:
            return []
        
        interval = self._config.get("scan_interval", 3600)  # 默认1小时
        
        return [{
            "id": "audio_track_converter_scan",
            "name": "音频轨道转换扫描",
            "trigger": "interval",
            "func": self._scan_directories,
            "kwargs": {"seconds": interval}
        }]
    
    @eventmanager.register(EventType.TransferComplete)
    def handle_transfer_complete(self, event: Event):
        """处理整理完成事件"""
        if not self._enabled:
            return
        
        event_data = event.event_data
        if not event_data:
            return
        
        # 获取整理后的文件路径
        file_path = event_data.get("path") or event_data.get("target_path")
        if not file_path:
            return
        
        self._process_video_file(Path(file_path))
    
    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event):
        """处理插件动作"""
        event_data = event.event_data
        if not event_data or event_data.get("action") != "audio_convert_scan":
            return
        
        logger.info("收到手动扫描请求")
        self._scan_directories()
    
    def _scan_directories(self):
        """扫描监控目录"""
        if not self._enabled or not self._watch_dirs:
            return
        
        logger.info(f"开始扫描目录: {self._watch_dirs}")
        
        video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv'}
        
        for watch_dir in self._watch_dirs:
            if not watch_dir.exists():
                logger.warning(f"监控目录不存在: {watch_dir}")
                continue
            
            # 递归查找视频文件
            for video_file in watch_dir.rglob('*'):
                if video_file.is_file() and video_file.suffix.lower() in video_extensions:
                    self._process_video_file(video_file)
    
    def _process_video_file(self, video_path: Path):
        """处理单个视频文件"""
        try:
            logger.info(f"处理视频文件: {video_path}")
            
            # 1. 检查是否已有外挂AAC音轨
            if self._has_external_aac_track(video_path):
                logger.info(f"已存在外挂AAC音轨，跳过: {video_path}")
                return
            
            # 2. 获取音轨信息
            audio_tracks = self._get_audio_tracks(video_path)
            
            if not audio_tracks:
                logger.warning(f"未检测到音轨: {video_path}")
                return
            
            # 3. 检查是否为多音轨
            if len(audio_tracks) > 1:
                logger.info(f"多音轨视频，跳过: {video_path}")
                return
            
            # 4. 检查音轨编码
            single_track = audio_tracks[0]
            codec = single_track.get('codec', '').upper()
            
            if codec not in ['EAC3', 'AC3']:
                logger.info(f"音轨编码不是EAC3/AC3 ({codec})，跳过: {video_path}")
                return
            
            # 5. 提取语言代码
            language = single_track.get('language', 'und')
            if not language or language == '':
                language = 'und'
            
            # 6. 生成外挂音轨文件路径
            external_audio_path = self._generate_external_audio_path(video_path, language)
            
            if external_audio_path.exists():
                logger.info(f"外挂音轨已存在，跳过: {external_audio_path}")
                return
            
            # 7. 执行转码
            success = self._convert_to_aac(video_path, external_audio_path, single_track.get('index', 0))
            
            if success:
                logger.info(f"成功创建外挂AAC音轨: {external_audio_path}")
            else:
                logger.error(f"转码失败: {video_path}")
        
        except Exception as e:
            logger.error(f"处理视频文件时出错 {video_path}: {str(e)}", exc_info=True)
    
    def _get_audio_tracks(self, video_path: Path) -> List[Dict]:
        """使用ffprobe获取视频音轨信息"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-select_streams', 'a',  # 只选择音频流
                str(video_path)
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"ffprobe执行失败: {result.stderr}")
                return []
            
            data = json.loads(result.stdout)
            streams = data.get('streams', [])
            
            audio_tracks = []
            for idx, stream in enumerate(streams):
                track_info = {
                    'index': stream.get('index', idx),
                    'codec': stream.get('codec_name', '').upper(),
                    'channels': stream.get('channels', 0),
                    'language': stream.get('tags', {}).get('language', 'und'),
                    'title': stream.get('tags', {}).get('title', '')
                }
                audio_tracks.append(track_info)
            
            return audio_tracks
        
        except Exception as e:
            logger.error(f"获取音轨信息失败: {str(e)}")
            return []
    
    def _has_external_aac_track(self, video_path: Path) -> bool:
        """检查是否已存在外挂AAC音轨文件"""
        parent_dir = video_path.parent
        base_name = video_path.stem
        
        # 查找可能的外挂音轨文件
        # 命名格式: basename.[language].aac 或 basename.default.aac
        for aac_file in parent_dir.glob(f"{base_name}.*.aac"):
            return True
        
        return False
    
    def _generate_external_audio_path(self, video_path: Path, language: str) -> Path:
        """生成外挂音轨文件路径（Emby命名规范）"""
        parent_dir = video_path.parent
        base_name = video_path.stem
        
        # Emby外挂音轨命名规范:
        # 格式: <video_name>.<language_code>.aac
        # 例如: movie.eng.aac, episode.chi.aac
        # 如果是默认语言，可以使用: <video_name>.default.aac
        
        if language == 'und' or not language:
            audio_filename = f"{base_name}.default.aac"
        else:
            audio_filename = f"{base_name}.{language}.aac"
        
        return parent_dir / audio_filename
    
    def _convert_to_aac(self, video_path: Path, output_path: Path, audio_index: int) -> bool:
        """使用ffmpeg提取并转码音频为AAC立体声"""
        try:
            # ffmpeg命令：
            # -i: 输入文件
            # -map 0:a:{audio_index}: 选择特定音频流
            # -ac 2: 转换为立体声
            # -ab 192k: 比特率192kbps
            # -c:a aac: 使用AAC编码器
            # -y: 覆盖输出文件
            
            cmd = [
                'ffmpeg',
                '-i', str(video_path),
                '-map', f'0:a:{audio_index}',
                '-ac', '2',          # 立体声
                '-ab', '192k',       # 比特率
                '-c:a', 'aac',       # AAC编码
                '-y',                # 覆盖输出
                str(output_path)
            ]
            
            logger.info(f"执行转码命令: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5分钟超时
            )
            
            if result.returncode != 0:
                logger.error(f"ffmpeg转码失败: {result.stderr}")
                # 清理可能的部分文件
                if output_path.exists():
                    output_path.unlink()
                return False
            
            # 验证输出文件
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"转码成功，文件大小: {output_path.stat().st_size} bytes")
                return True
            else:
                logger.error("转码完成但输出文件无效")
                return False
        
        except subprocess.TimeoutExpired:
            logger.error(f"转码超时: {video_path}")
            if output_path.exists():
                output_path.unlink()
            return False
        
        except Exception as e:
            logger.error(f"转码异常: {str(e)}")
            if output_path.exists():
                output_path.unlink()
            return False
    
    def get_form(self) -> List[Dict[str, Any]]:
        """返回插件配置表单"""
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'watch_dirs',
                                            'label': '监控目录',
                                            'placeholder': '每行一个目录路径\n例如:\n/media/movies\n/media/tv',
                                            'rows': 5
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'scan_interval',
                                            'label': '扫描间隔（秒）',
                                            'placeholder': '3600',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
