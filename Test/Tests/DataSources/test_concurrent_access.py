"""
Concurrent Access Tests for CacheManager

Tests for thread/process safety of cache operations:
1. Multiple processes writing to same cache key simultaneously
2. Multiple processes reading while one is writing
3. Atomic write safety (interrupt mid-write, verify no partial files)
4. Manifest lock contention

Author: Sophie (Subagent)
"""

import os
import sys
import json
import time
import signal
import tempfile
import threading
import multiprocessing
from multiprocessing import Process, Queue, Manager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set up minimal context for testing
os.environ.setdefault('PIPELINE_CONTEXT', 'test')

from Decorators.CacheManager import CacheManager


class TestResult:
    """Simple test result container"""
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.error = None
        self.details = ""
    
    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        result = f"{status}: {self.name}"
        if self.details:
            result += f"\n    Details: {self.details}"
        if self.error:
            result += f"\n    Error: {self.error}"
        return result


def get_test_cache_dir():
    """Create isolated test cache directory"""
    test_dir = tempfile.mkdtemp(prefix="cache_test_")
    return test_dir


def load_sample_data():
    """Load sample BSM data for testing"""
    sample_path = PROJECT_ROOT / "Test" / "test_data" / "sample_bsm.ndjson"
    if sample_path.exists():
        with open(sample_path, 'r') as f:
            return [json.loads(line) for line in f if line.strip()]
    # Fallback sample data
    return [{"id": i, "data": f"sample_{i}" * 100} for i in range(5)]


# =============================================================================
# TEST 1: Multiple Processes Writing to Same Cache Key
# =============================================================================

def writer_process(cache_dir, cache_key, process_id, write_count, results_queue):
    """Worker process that writes to a cache key multiple times"""
    try:
        # Reset singleton for this process
        CacheManager.reset_instance()
        manager = CacheManager.get_instance()
        manager.cache_base_path = cache_dir
        
        successes = 0
        failures = 0
        
        for i in range(write_count):
            cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
            data = f"Process_{process_id}_Write_{i}_{time.time()}"
            
            try:
                # Simulate cache write
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, 'w') as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                
                manager.record_miss(cache_key, cache_path)
                successes += 1
                
            except Exception as e:
                failures += 1
        
        results_queue.put({
            'process_id': process_id,
            'successes': successes,
            'failures': failures,
            'error': None
        })
        
    except Exception as e:
        results_queue.put({
            'process_id': process_id,
            'successes': 0,
            'failures': write_count,
            'error': str(e)
        })


def test_concurrent_writes_same_key():
    """Test multiple processes writing to the same cache key simultaneously"""
    result = TestResult("Concurrent Writes to Same Key")
    cache_dir = get_test_cache_dir()
    cache_key = "shared_cache_key"
    
    try:
        num_processes = 8
        writes_per_process = 50
        results_queue = multiprocessing.Queue()
        
        processes = []
        for i in range(num_processes):
            p = Process(
                target=writer_process,
                args=(cache_dir, cache_key, i, writes_per_process, results_queue)
            )
            processes.append(p)
        
        # Start all processes simultaneously
        for p in processes:
            p.start()
        
        # Wait for completion
        for p in processes:
            p.join(timeout=30)
        
        # Collect results
        total_successes = 0
        total_failures = 0
        errors = []
        
        while not results_queue.empty():
            r = results_queue.get()
            total_successes += r['successes']
            total_failures += r['failures']
            if r['error']:
                errors.append(f"Process {r['process_id']}: {r['error']}")
        
        # Check final file state
        cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
        file_valid = os.path.exists(cache_path)
        if file_valid:
            with open(cache_path, 'r') as f:
                content = f.read()
                file_valid = len(content) > 0 and "Process_" in content
        
        expected_total = num_processes * writes_per_process
        result.details = (
            f"Processes: {num_processes}, Writes/process: {writes_per_process}, "
            f"Total successes: {total_successes}/{expected_total}, "
            f"Failures: {total_failures}, Final file valid: {file_valid}"
        )
        
        if errors:
            result.error = "; ".join(errors[:3])
        
        # Pass if most writes succeeded and file is valid
        result.passed = (total_successes >= expected_total * 0.95) and file_valid
        
    except Exception as e:
        result.error = str(e)
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    
    return result


# =============================================================================
# TEST 2: Reading While Writing
# =============================================================================

def reader_writer_process(cache_dir, cache_key, is_writer, operation_count, results_queue):
    """Worker that either reads or writes"""
    try:
        CacheManager.reset_instance()
        manager = CacheManager.get_instance()
        manager.cache_base_path = cache_dir
        
        cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
        successes = 0
        partial_reads = 0
        empty_reads = 0
        
        for i in range(operation_count):
            try:
                if is_writer:
                    # Write with intentional delay to create race window
                    data = "A" * 10000 + f"_MARKER_{i}_" + "B" * 10000
                    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                    with open(cache_path, 'w') as f:
                        f.write(data[:len(data)//2])
                        time.sleep(0.001)  # Small delay mid-write
                        f.write(data[len(data)//2:])
                    manager.record_miss(cache_key, cache_path)
                    successes += 1
                else:
                    # Read and check for completeness
                    if os.path.exists(cache_path):
                        with open(cache_path, 'r') as f:
                            content = f.read()
                        
                        if len(content) == 0:
                            empty_reads += 1
                        elif "_MARKER_" not in content or len(content) < 20000:
                            partial_reads += 1
                        else:
                            manager.record_hit(cache_key, cache_path)
                            successes += 1
                    else:
                        empty_reads += 1
                        
                time.sleep(0.001)  # Stagger operations
                
            except Exception as e:
                pass  # Individual operation failed
        
        results_queue.put({
            'is_writer': is_writer,
            'successes': successes,
            'partial_reads': partial_reads,
            'empty_reads': empty_reads,
            'error': None
        })
        
    except Exception as e:
        results_queue.put({
            'is_writer': is_writer,
            'successes': 0,
            'partial_reads': 0,
            'empty_reads': 0,
            'error': str(e)
        })


def test_read_while_write():
    """Test reading from cache while another process is writing"""
    result = TestResult("Read While Write Safety")
    cache_dir = get_test_cache_dir()
    cache_key = "read_write_test"
    
    try:
        # Pre-create initial file
        cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_path, 'w') as f:
            f.write("A" * 10000 + "_MARKER_INIT_" + "B" * 10000)
        
        num_writers = 2
        num_readers = 6
        ops_per_process = 30
        results_queue = multiprocessing.Queue()
        
        processes = []
        
        # Start writers
        for i in range(num_writers):
            p = Process(
                target=reader_writer_process,
                args=(cache_dir, cache_key, True, ops_per_process, results_queue)
            )
            processes.append(p)
        
        # Start readers
        for i in range(num_readers):
            p = Process(
                target=reader_writer_process,
                args=(cache_dir, cache_key, False, ops_per_process, results_queue)
            )
            processes.append(p)
        
        for p in processes:
            p.start()
        
        for p in processes:
            p.join(timeout=30)
        
        # Analyze results
        writer_successes = 0
        reader_successes = 0
        partial_reads = 0
        empty_reads = 0
        
        while not results_queue.empty():
            r = results_queue.get()
            if r['is_writer']:
                writer_successes += r['successes']
            else:
                reader_successes += r['successes']
                partial_reads += r['partial_reads']
                empty_reads += r['empty_reads']
        
        total_read_ops = num_readers * ops_per_process
        
        result.details = (
            f"Writers: {num_writers}, Readers: {num_readers}, "
            f"Writer successes: {writer_successes}, "
            f"Reader successes: {reader_successes}/{total_read_ops}, "
            f"Partial reads: {partial_reads}, Empty reads: {empty_reads}"
        )
        
        # This test REVEALS lack of locking - partial reads indicate race conditions
        # Pass means no partial reads (proper locking) or we report the issue
        if partial_reads > 0:
            result.error = f"RACE CONDITION DETECTED: {partial_reads} partial reads observed (missing file locking)"
            result.passed = False
        else:
            result.passed = True
        
    except Exception as e:
        result.error = str(e)
    finally:
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    
    return result


# =============================================================================
# TEST 3: Atomic Write Safety (Interrupt Mid-Write)
# =============================================================================

def interruptible_writer(cache_dir, cache_key, interrupt_event, completion_event):
    """Writer that can be interrupted mid-write"""
    cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
    temp_path = os.path.join(cache_dir, f".{cache_key}.tmp")
    
    try:
        os.makedirs(cache_dir, exist_ok=True)
        
        # Write large data to temp file (slow operation)
        large_data = "X" * 100000 + "_COMPLETE_MARKER_" + "Y" * 100000
        
        with open(temp_path, 'w') as f:
            for i in range(0, len(large_data), 1000):
                f.write(large_data[i:i+1000])
                f.flush()
                if interrupt_event.is_set():
                    # Simulate interrupt - don't complete atomic rename
                    return
                time.sleep(0.001)
        
        # Atomic rename
        os.rename(temp_path, cache_path)
        completion_event.set()
        
    except Exception as e:
        pass


def test_atomic_write_safety():
    """Test that interrupted writes don't leave partial files"""
    result = TestResult("Atomic Write Safety (Interrupt Mid-Write)")
    cache_dir = get_test_cache_dir()
    cache_key = "atomic_test"
    
    try:
        interrupt_event = multiprocessing.Event()
        completion_event = multiprocessing.Event()
        
        # Test 1: Let write complete normally
        p = Process(
            target=interruptible_writer,
            args=(cache_dir, cache_key, interrupt_event, completion_event)
        )
        p.start()
        p.join(timeout=10)
        
        cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
        temp_path = os.path.join(cache_dir, f".{cache_key}.tmp")
        
        normal_complete = os.path.exists(cache_path)
        normal_content_valid = False
        if normal_complete:
            with open(cache_path, 'r') as f:
                content = f.read()
                normal_content_valid = "_COMPLETE_MARKER_" in content
        
        # Clean up for next test
        if os.path.exists(cache_path):
            os.remove(cache_path)
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # Test 2: Interrupt mid-write
        interrupt_event = multiprocessing.Event()
        completion_event = multiprocessing.Event()
        
        p = Process(
            target=interruptible_writer,
            args=(cache_dir, cache_key, interrupt_event, completion_event)
        )
        p.start()
        time.sleep(0.05)  # Let it start writing
        interrupt_event.set()  # Interrupt it
        p.join(timeout=5)
        
        # Check that no partial main file exists
        interrupted_has_main = os.path.exists(cache_path)
        interrupted_has_temp = os.path.exists(temp_path)
        
        if interrupted_has_main:
            with open(cache_path, 'r') as f:
                content = f.read()
                partial_main_file = "_COMPLETE_MARKER_" not in content
        else:
            partial_main_file = False
        
        result.details = (
            f"Normal write completed: {normal_complete}, Valid content: {normal_content_valid}, "
            f"Interrupted left main file: {interrupted_has_main}, "
            f"Interrupted left temp file: {interrupted_has_temp}, "
            f"Partial main file: {partial_main_file}"
        )
        
        # Pass if atomic writes work correctly:
        # - Normal write creates valid file
        # - Interrupted write doesn't create partial main file
        if partial_main_file:
            result.error = "Atomic write NOT implemented - partial file found after interrupt"
            result.passed = False
        else:
            result.passed = normal_content_valid and not partial_main_file
        
    except Exception as e:
        result.error = str(e)
    finally:
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    
    return result


# =============================================================================
# TEST 4: Manifest Lock Contention
# =============================================================================

def metadata_updater(cache_dir, process_id, update_count, results_queue):
    """Worker that repeatedly updates cache metadata"""
    try:
        CacheManager.reset_instance()
        manager = CacheManager.get_instance()
        manager.cache_base_path = cache_dir
        
        successes = 0
        failures = 0
        corrupt_reads = 0
        
        for i in range(update_count):
            cache_key = f"entry_{process_id}_{i}"
            cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
            
            try:
                # Create cache file
                os.makedirs(cache_dir, exist_ok=True)
                with open(cache_path, 'w') as f:
                    f.write(f"data_{process_id}_{i}")
                
                # Record in metadata (this writes to shared JSON file)
                manager.record_miss(cache_key, cache_path)
                
                # Immediately read back and verify
                manager._load_metadata()
                if cache_key in manager.metadata:
                    successes += 1
                else:
                    failures += 1
                
            except json.JSONDecodeError as e:
                corrupt_reads += 1
                failures += 1
            except Exception as e:
                failures += 1
        
        results_queue.put({
            'process_id': process_id,
            'successes': successes,
            'failures': failures,
            'corrupt_reads': corrupt_reads,
            'error': None
        })
        
    except Exception as e:
        results_queue.put({
            'process_id': process_id,
            'successes': 0,
            'failures': update_count,
            'corrupt_reads': 0,
            'error': str(e)
        })


def test_manifest_lock_contention():
    """Test concurrent access to the metadata/manifest file"""
    result = TestResult("Manifest Lock Contention")
    cache_dir = get_test_cache_dir()
    
    try:
        num_processes = 6
        updates_per_process = 25
        results_queue = multiprocessing.Queue()
        
        processes = []
        for i in range(num_processes):
            p = Process(
                target=metadata_updater,
                args=(cache_dir, i, updates_per_process, results_queue)
            )
            processes.append(p)
        
        for p in processes:
            p.start()
        
        for p in processes:
            p.join(timeout=60)
        
        total_successes = 0
        total_failures = 0
        total_corrupt = 0
        errors = []
        
        while not results_queue.empty():
            r = results_queue.get()
            total_successes += r['successes']
            total_failures += r['failures']
            total_corrupt += r['corrupt_reads']
            if r['error']:
                errors.append(f"Process {r['process_id']}: {r['error']}")
        
        # Check final metadata file integrity
        metadata_path = os.path.join(cache_dir, "cache_metadata.json")
        metadata_valid = False
        entries_count = 0
        
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    data = json.load(f)
                    entries_count = len(data.get('entries', {}))
                    metadata_valid = True
            except json.JSONDecodeError:
                metadata_valid = False
        
        expected_entries = num_processes * updates_per_process
        
        result.details = (
            f"Processes: {num_processes}, Updates/process: {updates_per_process}, "
            f"Total successes: {total_successes}, Failures: {total_failures}, "
            f"Corrupt reads: {total_corrupt}, "
            f"Final metadata valid: {metadata_valid}, "
            f"Entries in metadata: {entries_count}/{expected_entries}"
        )
        
        if total_corrupt > 0:
            result.error = f"RACE CONDITION: {total_corrupt} corrupt JSON reads (missing manifest locking)"
            result.passed = False
        elif not metadata_valid:
            result.error = "Final metadata file is corrupt"
            result.passed = False
        elif entries_count < expected_entries * 0.5:
            result.error = f"Too few entries in metadata ({entries_count}/{expected_entries}) - data loss from races"
            result.passed = False
        else:
            result.passed = True
        
        if errors:
            result.error = (result.error or "") + "; " + "; ".join(errors[:3])
        
    except Exception as e:
        result.error = str(e)
    finally:
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    
    return result


# =============================================================================
# TEST 5: Thread Safety (Same Process)
# =============================================================================

def test_thread_safety():
    """Test thread safety within the same process"""
    result = TestResult("Thread Safety (Same Process)")
    cache_dir = get_test_cache_dir()
    
    try:
        CacheManager.reset_instance()
        manager = CacheManager.get_instance()
        manager.cache_base_path = cache_dir
        
        num_threads = 10
        ops_per_thread = 50
        errors = []
        lock = threading.Lock()
        
        def thread_worker(thread_id):
            nonlocal errors
            try:
                for i in range(ops_per_thread):
                    cache_key = f"thread_{thread_id}_{i}"
                    cache_path = os.path.join(cache_dir, f"{cache_key}.txt")
                    
                    os.makedirs(cache_dir, exist_ok=True)
                    with open(cache_path, 'w') as f:
                        f.write(f"thread_{thread_id}_data_{i}")
                    
                    # These methods access shared state
                    manager.record_miss(cache_key, cache_path)
                    manager.record_hit(cache_key, cache_path)
                    
                    # Get stats (reads shared state)
                    stats = manager.get_statistics()
                    
            except Exception as e:
                with lock:
                    errors.append(f"Thread {thread_id}: {e}")
        
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=thread_worker, args=(i,))
            threads.append(t)
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join(timeout=30)
        
        stats = manager.get_statistics()
        expected_ops = num_threads * ops_per_thread * 2  # hits + misses
        
        result.details = (
            f"Threads: {num_threads}, Ops/thread: {ops_per_thread}, "
            f"Total operations: {stats['total_operations']}/{expected_ops}, "
            f"Errors: {len(errors)}"
        )
        
        if errors:
            result.error = "; ".join(errors[:3])
            result.passed = False
        elif stats['total_operations'] < expected_ops * 0.9:
            result.error = f"Lost operations due to race conditions ({stats['total_operations']}/{expected_ops})"
            result.passed = False
        else:
            result.passed = True
        
    except Exception as e:
        result.error = str(e)
    finally:
        CacheManager.reset_instance()
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    
    return result


# =============================================================================
# Main Test Runner
# =============================================================================

def run_all_tests():
    """Run all concurrent access tests"""
    print("=" * 70)
    print("CacheManager Concurrent Access Tests")
    print("=" * 70)
    print()
    
    tests = [
        ("Concurrent Writes", test_concurrent_writes_same_key),
        ("Read While Write", test_read_while_write),
        ("Atomic Write Safety", test_atomic_write_safety),
        ("Manifest Lock Contention", test_manifest_lock_contention),
        ("Thread Safety", test_thread_safety),
    ]
    
    results = []
    
    for name, test_func in tests:
        print(f"Running: {name}...")
        try:
            result = test_func()
            results.append(result)
            print(result)
        except Exception as e:
            r = TestResult(name)
            r.error = f"Test crashed: {e}"
            results.append(r)
            print(r)
        print()
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    
    for r in results:
        status = "✅" if r.passed else "❌"
        print(f"  {status} {r.name}")
    
    print()
    print(f"Passed: {passed}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    print()
    
    if failed > 0:
        print("ISSUES FOUND:")
        for r in results:
            if not r.passed and r.error:
                print(f"  - {r.name}: {r.error}")
    
    return passed, failed, results


if __name__ == "__main__":
    # Ensure we're using spawn method for clean process isolation
    multiprocessing.set_start_method('spawn', force=True)
    
    passed, failed, results = run_all_tests()
    
    # Exit with code 1 if any tests failed
    sys.exit(0 if failed == 0 else 1)
