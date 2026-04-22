import json
import subprocess
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
from queue import Queue

from apscheduler.triggers.cron import CronTrigger
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class AudioTranscoderPlugin(_PluginBase):
    """
    音频转码插件
    自动检测新整理的视频文件，对EAC3/AC3单音轨进行转码为AAC立体声外挂音轨
    """

    plugin_name = "音频转码插件"
    plugin_desc = "自动检测新整理的视频文件，对EAC3/AC3单音轨进行转码为AAC立体声外挂音轨"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "2.0.0"
    plugin_author = "AI Assistant"
    author_url = "https://github.com"
    plugin_config_prefix = "audiotranscoder_"
    plugin_order = 50
    auth_level = 1

    _enabled = False
    _run_once = False  # 立即运行一次
    _monitor_paths = ""
    _audio_codecs = "eac3,ac3"
    _max_workers = 1  # 最大并发线程数
    _logs = []
    
    # 并发控制
    _task_queue = Queue()  # 任务队列
    _is_processing = False  # 是否正在处理任务（保留用于兼容）
    _worker_threads = []  # 工作线程列表

    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._monitor_paths = config.get("monitor_paths", "")
        self._audio_codecs = config.get("audio_codecs", "eac3,ac3")
        self._max_workers = int(config.get("max_workers", 1))
        
        # 加载日志
        self._logs = self.get_data("logs") or []
        logger.info(f"音频转码插件初始化完成，启用状态: {self._enabled}, 最大并发数: {self._max_workers}")
        
        # 启动后台工作线程（根据配置的数量）
        self._start_workers()
        
        # 检查是否需要立即运行一次
        run_once = config.get("run_once", False)
        logger.info(f"run_once 配置值: {run_once} (类型: {type(run_once).__name__})")
        
        # 如果勾选了"立即运行一次"，在后台线程中执行扫描后重置标志
        if run_once:
            logger.info("检测到立即运行标志，将在后台执行扫描...")
            # 使用后台线程异步执行，避免阻塞配置保存
            threading.Thread(
                target=self._run_once_scan,
                daemon=True
            ).start()
            # 延迟重置配置，等待当前初始化完成后执行
            def reset_run_once():
                time.sleep(2)  # 等待 2 秒确保配置已保存
                try:
                    current_config = self.get_config() or {}
                    current_config["run_once"] = False
                    self.update_config(current_config)
                    logger.info("已重置 run_once 配置为 False")
                except Exception as e:
                    logger.error(f"重置 run_once 配置失败: {str(e)}")
            
            threading.Thread(
                target=reset_run_once,
                daemon=True
            ).start()
    
    def _start_workers(self):
        """启动工作线程"""
        # 停止旧的工作线程
        for _ in range(len(self._worker_threads)):
            self._task_queue.put(None)  # 发送退出信号
        
        # 等待旧线程结束
        for thread in self._worker_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        self._worker_threads = []
        
        # 启动新的工作线程
        for i in range(self._max_workers):
            thread = threading.Thread(
                target=self._process_queue,
                daemon=True,
                name=f"AudioTranscoderWorker-{i+1}"
            )
            thread.start()
            self._worker_threads.append(thread)
            logger.info(f"工作线程 {i+1}/{self._max_workers} 已启动")

    def get_state(self) -> bool:
        """返回插件运行状态"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册命令 - 立即执行一次扫描"""
        return [
            {
                "id": "audiotranscoder_run",
                "name": "立即执行音频转码扫描",
                "func": "_scan_monitor_paths"
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """无插件API"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """不注册定时服务，使用手动命令触发"""
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页面"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "run_once",
                                            "label": "保存后立即运行一次",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "monitor_paths",
                                            "label": "监控目录（每行一个）",
                                            "rows": 6,
                                            "placeholder": "/media/video\n/data/movies\n/mnt/nas/tv",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "audio_codecs",
                                            "label": "需要转码的音频格式",
                                            "placeholder": "eac3,ac3",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_workers",
                                            "label": "最大并发转码数",
                                            "placeholder": "1",
                                            "type": "number",
                                            "min": 1,
                                            "max": 8,
                                            "hint": "建议 1-4，根据 CPU 性能调整",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "run_once": False,
            "monitor_paths": "",
            "audio_codecs": "eac3,ac3",
            "max_workers": 1,
        }

    def get_page(self) -> List[dict]:
        """详情页面 - 显示日志和队列状态"""
        # 获取最后15条日志
        recent_logs = self._logs[-15:] if self._logs else []
        
        log_items = []
        for log_entry in reversed(recent_logs):
            log_items.append({
                "component": "VCard",
                "props": {
                    "variant": "flat",
                    "class": "mb-2"
                },
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-3"},
                        "text": f"{log_entry.get('time', '')} - {log_entry.get('message', '')}"
                    }
                ]
            })
        
        # 队列状态
        queue_size = self._task_queue.qsize()
        active_workers = sum(1 for t in self._worker_threads if t.is_alive())
        queue_status = f"队列中: {queue_size} 个任务 | 工作线程: {active_workers}/{self._max_workers}"
        
        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "text": f"📊 {queue_status} | 提示：勾选'保存后立即运行一次'可在保存配置后立即执行扫描"
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": f"运行日志（共{len(self._logs)}条）"
                    },
                    {
                        "component": "VCardText",
                        "content": log_items if log_items else [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "text": "暂无日志"
                                }
                            }
                        ]
                    }
                ]
            }
        ]

    def _clear_logs(self):
        """清空日志"""
        self._logs = []
        self.save_data("logs", self._logs)
        logger.info("已清空转码日志")

    def _add_log(self, message: str):
        """添加日志"""
        log_entry = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": message
        }
        self._logs.append(log_entry)
        self.save_data("logs", self._logs)

    def stop_service(self):
        """停止服务"""
        logger.info("停止音频转码插件服务...")
        # 清空任务队列
        while not self._task_queue.empty():
            try:
                self._task_queue.get_nowait()
            except:
                pass
        # 停止所有工作线程
        for _ in range(len(self._worker_threads)):
            self._task_queue.put(None)
        logger.info("任务队列已清空，工作线程将停止")

    def _process_queue(self):
        """后台工作线程 - 处理任务队列"""
        thread_name = threading.current_thread().name
        logger.info(f"{thread_name} 启动，等待任务...")
        
        while True:
            try:
                # 从队列获取任务（阻塞等待）
                file_path = self._task_queue.get()
                
                if file_path is None:  # 退出信号
                    logger.info(f"{thread_name} 收到退出信号，停止")
                    break
                
                logger.info(f"{thread_name} 开始处理: {file_path.name}")
                
                # 处理文件
                self._process_file(file_path)
                
                logger.info(f"{thread_name} 任务完成: {file_path.name}")
                
                # 标记任务完成
                self._task_queue.task_done()
                
            except Exception as e:
                logger.error(f"{thread_name} 异常: {str(e)}", exc_info=True)

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        """监听文件整理完成事件"""
        if not self._enabled:
            return

        event_data = event.event_data or {}
        file_path = event_data.get("file_path")
        if not file_path:
            return

        file_path = Path(file_path)
        if not file_path.exists():
            return

        # 检查文件是否在监控目录中
        if not self._is_in_monitor_paths(file_path):
            return

        # 将任务加入队列，而不是直接处理
        self._add_log(f"检测到新文件，加入队列: {file_path.name}")
        self._task_queue.put(file_path)
        logger.info(f"文件已加入转码队列: {file_path.name} (队列大小: {self._task_queue.qsize()})")

    def _is_in_monitor_paths(self, file_path: Path) -> bool:
        """检查文件是否在监控目录中"""
        if not self._monitor_paths:
            return True  # 如果未设置监控目录，监控所有文件
        
        monitor_paths = [p.strip() for p in self._monitor_paths.split('\n') if p.strip()]
        for monitor_path in monitor_paths:
            try:
                monitor_path_obj = Path(monitor_path)
                file_path.relative_to(monitor_path_obj)
                return True
            except ValueError:
                continue
        return False

    def _run_once_scan(self):
        """后台执行一次扫描任务"""
        try:
            logger.info("开始后台扫描任务...")
            self._scan_monitor_paths()
            logger.info("后台扫描任务完成")
        except Exception as e:
            logger.error(f"后台扫描任务失败: {str(e)}", exc_info=True)
            self._add_log(f"后台扫描失败: {str(e)}")

    def _scan_monitor_paths(self):
        """扫描监控目录（手动模式）"""
        if not self._monitor_paths:
            logger.warning("未配置监控目录，跳过扫描")
            self._add_log("未配置监控目录，跳过扫描")
            return
        
        monitor_paths = [p.strip() for p in self._monitor_paths.split('\n') if p.strip()]
        total_files = 0
        
        for monitor_path in monitor_paths:
            try:
                monitor_path_obj = Path(monitor_path)
                if not monitor_path_obj.exists():
                    self._add_log(f"监控目录不存在: {monitor_path}")
                    continue
                
                self._add_log(f"开始扫描目录: {monitor_path}")
                logger.info(f"扫描目录: {monitor_path}")
                
                for video_file in monitor_path_obj.rglob("*"):
                    if self._is_video_file(video_file) and video_file.is_file():
                        total_files += 1
                        # 将任务加入队列
                        self._task_queue.put(video_file)
                        logger.info(f"文件已加入队列: {video_file.name} (队列大小: {self._task_queue.qsize()})")
                        
            except Exception as e:
                self._add_log(f"扫描目录失败 {monitor_path}: {str(e)}")
                logger.error(f"扫描目录失败 {monitor_path}: {str(e)}")
        
        if total_files > 0:
            self._add_log(f"扫描完成，共发现 {total_files} 个视频文件已加入队列")
            logger.info(f"扫描完成，共 {total_files} 个文件加入队列")
        else:
            self._add_log("扫描完成，未发现需要处理的视频文件")
            logger.info("扫描完成，未发现需要处理的文件")

    def _process_file(self, file_path: Path):
        """处理单个文件"""
        try:
            self._add_log(f"开始处理文件: {file_path.name}")

            if not self._is_video_file(file_path):
                return

            audio_info = self._get_audio_info(file_path)
            if not audio_info:
                self._add_log(f"无法获取音频信息: {file_path.name}")
                return

            if len(audio_info) != 1:
                self._add_log(f"多音轨文件，跳过: {file_path.name}")
                return

            audio_stream = audio_info[0]
            codec = audio_stream.get("codec_name", "").lower()
            codec_list = [c.strip() for c in self._audio_codecs.lower().split(',')]
            
            if codec not in codec_list:
                self._add_log(f"音轨格式 {codec} 不在转码列表中: {file_path.name}")
                return

            # 获取语言代码
            lang_code = audio_stream.get("tags", {}).get("language", "und")
            
            # 生成输出文件名
            aac_path = file_path.parent / f"{file_path.stem}.{lang_code}.aac"
            
            if aac_path.exists():
                self._add_log(f"已存在外挂音轨，跳过: {aac_path.name}")
                return

            self._transcode_to_aac(file_path, aac_path)

        except Exception as e:
            self._add_log(f"处理文件失败: {file_path.name} - {str(e)}")

    def _is_video_file(self, file_path: Path) -> bool:
        """检查是否为视频文件"""
        video_extensions = {".mp4", ".mkv", ".avi", ".m4v", ".mov", ".wmv", ".flv", ".ts"}
        return file_path.suffix.lower() in video_extensions

    def _get_audio_info(self, file_path: Path) -> List[Dict]:
        """使用ffprobe获取音频流信息"""
        try:
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                str(file_path)
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
            streams = data.get("streams", [])
            return [s for s in streams if s.get("codec_type") == "audio"]

        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"获取音频信息失败: {str(e)}")
            return []

    def _transcode_to_aac(self, input_path: Path, output_path: Path):
        """转码音频为AAC格式"""
        try:
            cmd = [
                "ffmpeg",
                "-i", str(input_path),
                "-vn",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ac", "2",
                "-y",
                str(output_path)
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                self._add_log(f"转码成功: {output_path.name}")
                logger.info(f"转码成功: {output_path}")
            else:
                self._add_log(f"转码失败: {input_path.name}")
                logger.error(f"转码失败: {result.stderr}")

        except subprocess.TimeoutExpired:
            self._add_log(f"转码超时: {input_path.name}")
        except FileNotFoundError:
            self._add_log("ffmpeg未找到，请确保已安装ffmpeg")
        except Exception as e:
            self._add_log(f"转码异常: {str(e)}")
