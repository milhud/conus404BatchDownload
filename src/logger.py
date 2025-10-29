"""Logger module for CONUS404 data download and processing."""

import datetime as dt
import os
import psutil
import threading
import time


class Logger:
    """Logger for download and process operations with memory monitoring."""
    
    def __init__(self, log_dir: str = "logs", memory_check_interval: int = 30):
        """
        Initialize logger with log directory and memory monitoring.
        
        Args:
            log_dir: Base directory for log files
            memory_check_interval: Seconds between memory checks (0 to disable)
        """
        os.makedirs(os.path.join(log_dir, "download"), exist_ok=True)
        os.makedirs(os.path.join(log_dir, "process"), exist_ok=True)
        os.makedirs(os.path.join(log_dir, "memory"), exist_ok=True)
        
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.download_log = os.path.join(log_dir, "download", f"download_{timestamp}.log")
        self.process_log = os.path.join(log_dir, "process", f"process_{timestamp}.log")
        self.memory_log = os.path.join(log_dir, "memory", f"memory_{timestamp}.log")
        
        # Memory monitoring
        self.memory_check_interval = memory_check_interval
        self.monitoring = False
        self.monitor_thread = None
        
        if memory_check_interval > 0:
            self.start_memory_monitoring()
    
    def _write_log(self, filepath: str, message: str):
        """
        Write a timestamped log message to file and console.
        
        Args:
            filepath: Path to log file
            message: Message to log
        """
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line)
        with open(filepath, "a") as f:
            f.write(log_line + "\n")
    
    def log_download(self, message: str):
        """
        Log download-related messages.
        
        Args:
            message: Download message to log
        """
        self._write_log(self.download_log, message)
    
    def log_process(self, message: str):
        """
        Log processing-related messages.
        
        Args:
            message: Process message to log
        """
        self._write_log(self.process_log, message)
    
    def log_memory(self, context: str = ""):
        """
        Log current memory usage statistics.
        
        Args:
            context: Optional context for the memory log
        """
        process = psutil.Process()
        mem_info = process.memory_info()
        virtual_mem = psutil.virtual_memory()
        
        # Memory in MB
        rss_mb = mem_info.rss / (1024 * 1024)
        vms_mb = mem_info.vms / (1024 * 1024)
        available_mb = virtual_mem.available / (1024 * 1024)
        total_mb = virtual_mem.total / (1024 * 1024)
        percent_used = virtual_mem.percent
        
        message = (
            f"Memory Stats{' - ' + context if context else ''}: "
            f"RSS={rss_mb:.1f}MB, VMS={vms_mb:.1f}MB, "
            f"Available={available_mb:.1f}MB/{total_mb:.1f}MB ({percent_used:.1f}% used)"
        )
        
        self._write_log(self.memory_log, message)
        
        # Warning if memory usage is high
        if percent_used > 85:
            warning = f"WARNING: High memory usage detected ({percent_used:.1f}%)"
            self._write_log(self.memory_log, warning)
            self.log_process(warning)
        
        return {
            "rss_mb": rss_mb,
            "vms_mb": vms_mb,
            "available_mb": available_mb,
            "total_mb": total_mb,
            "percent_used": percent_used
        }
    
    def _memory_monitor_loop(self):
        """Background thread function for periodic memory monitoring."""
        while self.monitoring:
            self.log_memory("Periodic Check")
            time.sleep(self.memory_check_interval)
    
    def start_memory_monitoring(self):
        """Start periodic memory monitoring in background thread."""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._memory_monitor_loop, daemon=True)
            self.monitor_thread.start()
            self.log_memory("Monitoring Started")

    def check_memory_critical(self) -> bool:
        """
        Check if memory usage is critical (>90%).
        
        Returns:
            True if memory is critical, False otherwise
        """
        virtual_mem = psutil.virtual_memory()
        return virtual_mem.percent > 90
    
    def stop_memory_monitoring(self):
        """Stop periodic memory monitoring."""
        if self.monitoring:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=self.memory_check_interval + 1)
            self.log_memory("Monitoring Stopped")
    
    def __del__(self):
        """Cleanup: stop monitoring on deletion."""
        self.stop_memory_monitoring()
