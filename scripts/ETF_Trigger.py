from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import subprocess
import os

ETF_FILE = os.path.join(os.path.dirname(__file__), "../data/holdings/ivw.csv")

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == ETF_FILE:
            print(f"{ETF_FILE} changed, uploading to BigQuery...")
            subprocess.run(["python", os.path.join(os.path.dirname(__file__), "upload_ivw.py")])

observer = Observer()
observer.schedule(FileChangeHandler(), path=os.path.dirname(ETF_FILE), recursive=False)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
