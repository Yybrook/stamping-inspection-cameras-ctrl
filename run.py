import subprocess
import sys
import os
# import signal
import time
from datetime import datetime

HIKROBOT_ROOT = r"C:\Users\yy\Documents\yyProjects\HikrobotCamera\hikrobot-camera"

def log(msg, level="INFO"):
    now = datetime.now().strftime("[%H:%M:%S]")
    print(f"{now} [{level}] {msg}")


def force_kill(pid):
    """强制终止一个进程及其子进程（Windows专用）"""
    try:
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        log(f"强制终止 PID={pid}")
    except subprocess.CalledProcessError:
        log(f"无法终止 PID={pid}，可能已退出", level="WARN")


def launch_script(script_rel_path, base_dir):
    """启动一个 Python 脚本于新的 CMD 控制台中"""
    script_path = os.path.join(base_dir, script_rel_path)
    env = os.environ.copy()

    global HIKROBOT_ROOT

    # === 构造 PYTHONPATH（顺序很重要） ===
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [
        base_dir,  # 当前项目根
        HIKROBOT_ROOT,  # HikrobotCamera 项目根
        env.get("PYTHONPATH"),  # 原有 PYTHONPATH
    ]))

    if not os.path.isfile(script_path):
        log(f"找不到脚本文件: {script_path}", level="ERROR")
        return None

    cmd = ['cmd', '/k', f'title {os.path.basename(os.path.dirname(script_rel_path))} & {sys.executable} {script_path}']

    try:
        proc = subprocess.Popen(
            cmd,
            env=env,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        log(f"启动脚本：{script_rel_path} | PID: {proc.pid}")
        return proc
    except Exception as e:
        log(f"启动脚本失败：{script_rel_path} | 错误: {e}", level="ERROR")
        return None


def run_all_scripts(scripts):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(base_dir)
    processes = []

    log("🚀 正在启动所有脚本...")

    for script in scripts:
        # ====== 处理 sleep 指令 ======
        if script.startswith("sleep:"):
            try:
                seconds = int(script.split(":", 1)[1])
                log(f"⏳ 延时 {seconds} 秒...")
                time.sleep(seconds)
            except ValueError:
                log(f"非法 sleep 指令: {script}", level="ERROR")
            continue

        # ====== 正常启动脚本 ======
        proc = launch_script(script, base_dir)
        if proc:
            processes.append(proc)

    log(f"✨ 共启动 {len(processes)} 个脚本。按 Ctrl+C 可终止所有子进程。")

    try:
        while processes:
            time.sleep(1)
            exited = []
            for p in processes:
                if p.poll() is not None:
                    log(f"子进程 PID={p.pid} 已退出，返回码={p.returncode}", level="WARN")
                    exited.append(p)
            for p in exited:
                processes.remove(p)

        log("🎉 所有子进程已退出。")
    except KeyboardInterrupt:
        log("⛔️ 检测到 Ctrl+C，正在终止所有子进程...", level="WARN")
        for p in processes:
            try:
                force_kill(p.pid)
            except Exception as e:
                log(f"终止 PID={p.pid} 失败: {e}", level="ERROR")
        log("✅ 所有子进程已尝试终止。")


if __name__ == "__main__":
    # 相对路径（基于此脚本所在目录）
    scripts = [
        "app/readerForPress/main.py",
        "app/camerasForShuttle/main_client.py",
        "sleep:10",
        "app/camerasForShuttle/main_server.py",
        "app/imageSaverForShuttle/main.py",
        "app/webViewer/main.py"
    ]
    run_all_scripts(scripts)
