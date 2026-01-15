"""
QuasarVaultage Watchdog - Automated Excel Processing Pipeline
Monitors [2]_Drop_xlsx_here folder and processes files through all 9 layers
"""

import os
import time
import shutil
import subprocess
import sys
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

class ExcelProcessorHandler(FileSystemEventHandler):
    def __init__(self, watch_dir, process_dir, output_dir):
        self.watch_dir = Path(watch_dir)
        self.process_dir = Path(process_dir)
        self.output_dir = Path(output_dir)
        self.upload_counter = self._get_next_upload_id()
        self.processing = False
        
    def _get_next_upload_id(self):
        """Determine the next Upload-X_ID folder number"""
        if not self.output_dir.exists():
            return 1
        
        existing = [d for d in self.output_dir.iterdir() if d.is_dir() and d.name.startswith('Upload-')]
        if not existing:
            return 1
        
        numbers = []
        for folder in existing:
            try:
                num = int(folder.name.split('-')[1].split('_')[0])
                numbers.append(num)
            except (IndexError, ValueError):
                continue
        
        return max(numbers) + 1 if numbers else 1
    
    def on_created(self, event):
        """Triggered when a new file is detected"""
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Only process Excel files
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            return
        
        # Avoid processing temporary Excel files
        if file_path.name.startswith('~$'):
            return
        
        print(f"\n{'='*70}")
        print(f" NEW FILE DETECTED: {file_path.name}")
        print(f"{'='*70}")
        
        # Wait for file to be completely written
        self._wait_for_file_ready(file_path)
        
        # Process the file
        self.process_file(file_path)
    
    def _wait_for_file_ready(self, file_path, timeout=30):
        """Wait until file is completely written and not locked"""
        print(f" Waiting for file to be ready...")
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < timeout:
            try:
                current_size = file_path.stat().st_size
                if current_size == last_size and current_size > 0:
                    # Try to open file to ensure it's not locked
                    with open(file_path, 'rb') as f:
                        f.read(1)
                    time.sleep(1)  # Extra safety delay
                    print(f" File ready ({current_size} bytes)")
                    return True
                last_size = current_size
                time.sleep(0.5)
            except (PermissionError, IOError):
                time.sleep(0.5)
        
        print(f" Warning: File may not be fully ready after {timeout}s")
        return False
    
    def process_file(self, file_path):
        """Main processing pipeline"""
        if self.processing:
            print(" Already processing a file. Skipping...")
            return
        
        self.processing = True
        start_time = time.time()
        
        try:
            # Step 1: Move file to Process folder
            dest_path = self.process_dir / file_path.name
            print(f"\n Step 1: Moving file to Process folder...")
            shutil.move(str(file_path), str(dest_path))
            print(f" Moved to: {dest_path}")
            
            # Step 2: Run all layer scripts sequentially
            print(f"\n Step 2: Running processing layers...")
            layer_scripts = [f"{i}_layer.py" for i in range(1, 10)]
            
            for i, script in enumerate(layer_scripts, 1):
                script_path = self.process_dir / script
                if not script_path.exists():
                    print(f" Warning: {script} not found. Skipping...")
                    continue
                
                print(f"\n  Running Layer {i}: {script}")
                result = self._run_layer_script(script_path, dest_path)
                
                if result:
                    print(f"  Layer {i} completed successfully")
                else:
                    print(f"  Layer {i} failed")
                    # Continue processing other layers even if one fails
            
            # Step 3: Collect all CSV outputs from Process folder
            print(f"\n Step 3: Collecting output CSV files...")
            csv_files = list(self.process_dir.glob("*.csv"))
            
            if not csv_files:
                print(" No CSV files found in Process folder")
                return
            
            # Step 4: Create Upload-X_ID folder and move files
            upload_folder = self.output_dir / f"Upload-{self.upload_counter}_ID"
            upload_folder.mkdir(parents=True, exist_ok=True)
            print(f"\n Step 4: Creating output folder: {upload_folder.name}")
            
            moved_count = 0
            for csv_file in csv_files:
                dest = upload_folder / csv_file.name
                shutil.move(str(csv_file), str(dest))
                moved_count += 1
                print(f"  Moved: {csv_file.name}")
            
            # Step 5: Summary
            elapsed_time = time.time() - start_time
            print(f"\n{'='*70}")
            print(f" PROCESSING COMPLETE!")
            print(f"{'='*70}")
            print(f" Input file: {file_path.name}")
            print(f" Output folder: {upload_folder.name}")
            print(f" CSV files generated: {moved_count}")
            print(f" Time elapsed: {elapsed_time:.2f} seconds")
            print(f"{'='*70}\n")
            
            # Increment counter for next file
            self.upload_counter += 1
            
        except Exception as e:
            print(f"\n ERROR during processing: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.processing = False
    
    def _run_layer_script(self, script_path, input_file):
        """Execute a layer script and return success status"""
        try:
            # Run the script with Python interpreter
            result = subprocess.run(
                [sys.executable, str(script_path), str(input_file)],
                cwd=str(self.process_dir),
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per layer
            )
            
            # Print script output if there's any
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    print(f"    {line}")
            
            if result.returncode != 0:
                print(f"    Script exited with code {result.returncode}")
                if result.stderr:
                    print(f"    Error: {result.stderr}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            print(f"    Script timed out after 5 minutes")
            return False
        except Exception as e:
            print(f"    Error running script: {e}")
            return False


def setup_directories(base_dir):
    """Create necessary directories if they don't exist"""
    watch_dir = base_dir / "[2]_Drop_xlsx_here"
    process_dir = base_dir / "[3]_Process"
    output_dir = base_dir / "[4]_output_csv_files"
    
    watch_dir.mkdir(exist_ok=True)
    process_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    return watch_dir, process_dir, output_dir


def main():
    """Main watchdog loop"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║          QuasarVaultage - Automated Processing System            ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Determine base directory (backend folder)
    script_dir = Path(__file__).parent.absolute()
    
    # Setup directories
    watch_dir, process_dir, output_dir = setup_directories(script_dir)
    
    print(f" Monitoring: {watch_dir}")
    print(f" Processing: {process_dir}")
    print(f" Output: {output_dir}")
    print(f"\n{'='*70}")
    print(" WATCHDOG ACTIVE - Waiting for Excel files...")
    print("   Drop .xlsx files into [2]_Drop_xlsx_here folder")
    print("   Press Ctrl+C to stop")
    print(f"{'='*70}\n")
    
    # Create event handler and observer
    event_handler = ExcelProcessorHandler(watch_dir, process_dir, output_dir)
    observer = Observer()
    observer.schedule(event_handler, str(watch_dir), recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n Stopping watchdog...")
        observer.stop()
        observer.join()
        print(" Watchdog stopped gracefully")


if __name__ == "__main__":
    main()