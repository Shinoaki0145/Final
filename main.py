import time
import os
import shutil
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from arxiv_crawler import crawl_single_paper
from reference_extractor import extract_references_for_paper


# Global statistics
stats_lock = Lock()
stats = {
    "total_processed": 0,           # Tổng số paper đã xử lý
    "both_success": 0,              # Cả 2 phần thành công
    "only_crawler_success": 0,      # Chỉ crawler thành công
    "only_references_success": 0,   # Chỉ references thành công (không xảy ra do logic)
    "crawler_failed": 0,            # Crawler thất bại
    "references_failed": 0,         # References thất bại (crawler thành công)
    "both_failed": 0,               # Cả 2 phần thất bại
    
    # Paper size statistics
    "paper_sizes_before": [],       # Kích thước bài báo trước khi xóa hình (bytes)
    "paper_sizes_after": [],        # Kích thước bài báo sau khi xóa hình (bytes)
    
    # Reference statistics
    "reference_counts": [],         # Số lượng references mỗi paper
    "reference_scrape_successes": 0, # Số lần scrape references thành công
    "reference_scrape_attempts": 0, # Tổng số lần thử scrape references
    
    # Timing statistics
    "paper_processing_times": [],   # Thời gian xử lý mỗi paper (seconds)
    "entry_discovery_time": 0,      # Tổng thời gian entry discovery
    
    # Memory statistics
    "memory_samples": [],          # Các mẫu RAM usage (bytes)
    "max_memory_used": 0,           # RAM tối đa sử dụng (bytes)
    
    # Disk statistics
    "max_disk_used": 0,             # Dung lượng đĩa tối đa (bytes)
    "final_disk_size": 0,           # Kích thước output cuối cùng (bytes)
}


def get_folder_size(folder_path):
    """Calculate total size of a folder in bytes."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception as e:
        print(f"  Warning: Could not calculate folder size: {e}")
    return total_size


def get_paper_size_info(arxiv_id, save_dir):
    """
    Get paper size before and after figure removal.
    Returns: (size_before, size_after) in bytes, or (None, None) if not found
    """
    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    
    if not os.path.exists(paper_folder):
        return None, None
    
    # Size after (current folder size)
    size_after = get_folder_size(paper_folder)
    
    # Size before is harder to track - we'll estimate based on tar.gz files if they exist
    # For now, we'll use a placeholder or track it during extraction
    # This would need to be tracked in arxiv_crawler.py during extraction
    size_before = None  # Will be tracked during extraction
    
    return size_before, size_after


def get_reference_count(arxiv_id, save_dir):
    """Get number of references from references.json file."""
    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    references_path = os.path.join(paper_folder, "references.json")
    
    if os.path.exists(references_path):
        try:
            import json
            with open(references_path, 'r', encoding='utf-8') as f:
                references = json.load(f)
                return len(references) if isinstance(references, dict) else 0
        except Exception:
            return 0
    return 0


def process_paper(arxiv_id, save_dir="./23127238"):
    """
    Process a single paper: crawl data first, then extract references.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn
        save_dir: Directory to save data
    
    Returns:
        tuple: (arxiv_id, crawler_success, references_success, processing_time, paper_size_before, paper_size_after, ref_count)
    """
    print(f"\n{'='*80}")
    print(f"Processing paper: {arxiv_id}")
    print(f"{'='*80}")
    
    # Track memory before processing
    process = psutil.Process()
    memory_before = process.memory_info().rss
    
    # Track timing
    paper_start_time = time.time()
    
    # Step 1: Crawl paper data
    crawler_success, size_before_from_crawler = crawl_single_paper(arxiv_id, save_dir)
    
    # Step 2: Extract references (only if crawler succeeded)
    references_success = True
    reference_count = 0
    if crawler_success:
        references_success = extract_references_for_paper(arxiv_id, save_dir)
        if references_success:
            reference_count = get_reference_count(arxiv_id, save_dir)
    else:
        print(f"X [{arxiv_id}] Skipping reference extraction (crawler failed)")
    
    # Track timing
    processing_time = time.time() - paper_start_time
    
    # Track memory after processing
    memory_after = process.memory_info().rss
    memory_used = memory_after - memory_before
    
    # Get paper size information
    _, size_after = get_paper_size_info(arxiv_id, save_dir)
    # Use size_before from crawler if available, otherwise None
    size_before = size_before_from_crawler if size_before_from_crawler > 0 else None
    
    # Update statistics
    with stats_lock:
        stats["total_processed"] += 1
        stats["paper_processing_times"].append(processing_time)
        stats["memory_samples"].append(memory_after)
        
        if memory_after > stats["max_memory_used"]:
            stats["max_memory_used"] = memory_after
        
        if crawler_success and references_success:
            stats["both_success"] += 1
        elif crawler_success and not references_success:
            stats["only_crawler_success"] += 1
            stats["references_failed"] += 1
        elif not crawler_success:
            stats["crawler_failed"] += 1
            if references_success:  # Unlikely but handle it
                stats["only_references_success"] += 1
            else:
                stats["both_failed"] += 1
        
        # Track reference statistics
        if crawler_success:
            stats["reference_scrape_attempts"] += 1
            if references_success:
                stats["reference_scrape_successes"] += 1
                # Track all reference counts (including 0) for accurate average
                stats["reference_counts"].append(reference_count)
        
        # Track paper sizes
        if size_after is not None:
            stats["paper_sizes_after"].append(size_after)
        if size_before is not None:
            stats["paper_sizes_before"].append(size_before)
    
    return arxiv_id, crawler_success, references_success, processing_time, size_before, size_after, reference_count


def check_paper_exists(arxiv_id, save_dir="./23127238"):
    """
    Check if a paper exists by attempting to crawl it.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn
        save_dir: Directory to save data
    
    Returns:
        bool: True if paper exists, False otherwise
    """
    success, _ = crawl_single_paper(arxiv_id, save_dir)
    
    # If failed, clean up any created folders
    if not success:
        prefix, suffix = arxiv_id.split('.')
        paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
        if os.path.exists(paper_folder):
            try:
                shutil.rmtree(paper_folder)
                print(f"  Cleaned up folder for non-existent paper: {arxiv_id}")
            except Exception as e:
                print(f"  Warning: Could not clean up folder: {e}")
    
    return success


def find_last_valid_id(prefix, start_id, save_dir="./23127238"):
    """
    Find the last valid paper ID in a month by checking consecutive failures.
    
    Args:
        prefix: Month prefix (e.g., "2305")
        start_id: Starting ID to check from
        save_dir: Directory to save data
    
    Returns:
        int: Last valid ID found, or 0 if none found
    """
    discovery_start_time = time.time()
    
    consecutive_failures = 0
    max_consecutive_failures = 3
    current_id = start_id
    last_valid_id = start_id - 1
    
    print(f"\n{'='*80}")
    print(f"Finding last valid ID for {prefix}.xxxxx starting from {start_id}")
    print(f"{'='*80}")
    
    while consecutive_failures < max_consecutive_failures:
        arxiv_id = f"{prefix}.{current_id:05d}"
        print(f"\nProbing: {arxiv_id}")
        
        exists = check_paper_exists(arxiv_id, save_dir)
        
        if exists:
            consecutive_failures = 0
            last_valid_id = current_id
            print(f"✓ Found valid paper: {arxiv_id}")
        else:
            consecutive_failures += 1
            print(f"X Paper not found: {arxiv_id} (failure {consecutive_failures}/{max_consecutive_failures})")
        
        current_id += 1
        time.sleep(0.5)  # Be nice to arXiv
    
    discovery_time = time.time() - discovery_start_time
    
    # Update entry discovery time
    with stats_lock:
        stats["entry_discovery_time"] += discovery_time
    
    print(f"\n{'='*80}")
    print(f"Last valid ID found: {prefix}.{last_valid_id:05d}")
    print(f"{'='*80}\n")
    
    return last_valid_id


def generate_paper_ids(start_month, start_id, end_month, end_id, save_dir="./23127238"):
    """
    Generate list of arXiv IDs based on date range.
    
    Args:
        start_month: Start month in format "YYYY-MM"
        start_id: Starting ID number
        end_month: End month in format "YYYY-MM"
        end_id: Ending ID number
        save_dir: Directory to save data
    
    Returns:
        list: List of arXiv IDs in format "yymm.nnnnn"
    """
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    paper_ids = []
    
    if start_month == end_month:
        # Same month - simple range
        print(f"Single month mode: {start_prefix}.{start_id:05d} → {start_prefix}.{end_id:05d}")
        for i in range(start_id, end_id + 1):
            paper_ids.append(f"{start_prefix}.{i:05d}")
    else:
        # Different months - need to find last valid ID in start month
        print(f"Multi-month mode: {start_prefix}.{start_id:05d} → {end_prefix}.{end_id:05d}")
        
        # Find last valid ID in start month
        last_valid_start_month = find_last_valid_id(start_prefix, start_id, save_dir)
        
        # Add papers from start month
        for i in range(start_id, last_valid_start_month + 1):
            paper_ids.append(f"{start_prefix}.{i:05d}")
        
        # Add papers from end month (from 1 to end_id)
        print(f"\nAdding papers from end month: {end_prefix}.00001 → {end_prefix}.{end_id:05d}")
        for i in range(1, end_id + 1):
            paper_ids.append(f"{end_prefix}.{i:05d}")
    
    return paper_ids


def print_progress_report():
    """Print current statistics."""
    with stats_lock:
        print(f"\n{'='*80}")
        print("CURRENT PROGRESS:")
        print(f"  Total processed          : {stats['total_processed']}")
        print(f"  Both success             : {stats['both_success']}")
        print(f"  Only crawler success     : {stats['only_crawler_success']}")
        print(f"  Only references success  : {stats['only_references_success']}")
        print(f"  Crawler failed           : {stats['crawler_failed']}")
        print(f"  References failed        : {stats['references_failed']}")
        print(f"  Both failed              : {stats['both_failed']}")
        print(f"{'='*80}\n")


def format_bytes(bytes_value):
    """Format bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def get_disk_usage(path):
    """Get disk usage of a directory in bytes."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception:
        pass
    return total_size


def print_final_report(save_dir="./23127238"):
    """Print final statistics with percentages and all metrics."""
    total = stats['total_processed']
    
    # Calculate success rates
    overall_success_rate = ((stats['both_success'] + stats['only_crawler_success']) / total * 100) if total > 0 else 0
    both_success_rate = (stats['both_success'] / total * 100) if total > 0 else 0
    phase2_fail_rate = (stats['references_failed'] / total * 100) if total > 0 else 0
    
    # Calculate average paper sizes
    avg_size_before = sum(stats['paper_sizes_before']) / len(stats['paper_sizes_before']) if stats['paper_sizes_before'] else 0
    avg_size_after = sum(stats['paper_sizes_after']) / len(stats['paper_sizes_after']) if stats['paper_sizes_after'] else 0
    
    # Calculate average reference count
    avg_references = sum(stats['reference_counts']) / len(stats['reference_counts']) if stats['reference_counts'] else 0
    
    # Calculate reference scrape success rate
    ref_success_rate = (stats['reference_scrape_successes'] / stats['reference_scrape_attempts'] * 100) if stats['reference_scrape_attempts'] > 0 else 0
    
    # Calculate timing statistics
    avg_processing_time = sum(stats['paper_processing_times']) / len(stats['paper_processing_times']) if stats['paper_processing_times'] else 0
    
    # Calculate memory statistics
    avg_memory = sum(stats['memory_samples']) / len(stats['memory_samples']) if stats['memory_samples'] else 0
    
    # Get disk usage
    final_disk_size = get_disk_usage(save_dir)
    
    print(f"\n{'='*80}")
    print("FINAL REPORT - RELEVANT STATISTICS:")
    print(f"{'='*80}")
    print(f"  Total processed                    : {stats['total_processed']}")
    print(f"  Papers scraped successfully        : {stats['both_success'] + stats['only_crawler_success']}")
    print(f"  Overall success rate               : {overall_success_rate:.2f}%")
    print(f"  Both phases success                : {stats['both_success']} ({both_success_rate:.2f}%)")
    print(f"  Only crawler success               : {stats['only_crawler_success']}")
    print(f"  Crawler failed                     : {stats['crawler_failed']}")
    print(f"  References failed                  : {stats['references_failed']} ({phase2_fail_rate:.2f}%)")
    print(f"  Both failed                        : {stats['both_failed']}")
    
    print(f"\n{'='*80}")
    print("PAPER SIZE STATISTICS:")
    print(f"{'='*80}")
    if stats['paper_sizes_before']:
        print(f"  Average size before removing figures: {format_bytes(avg_size_before)}")
    else:
        print(f"  Average size before removing figures: N/A (not tracked)")
    if stats['paper_sizes_after']:
        print(f"  Average size after removing figures : {format_bytes(avg_size_after)}")
    else:
        print(f"  Average size after removing figures : N/A")
    
    print(f"\n{'='*80}")
    print("REFERENCE STATISTICS:")
    print(f"{'='*80}")
    if stats['reference_counts']:
        print(f"  Average references per paper        : {avg_references:.2f}")
    else:
        print(f"  Average references per paper        : N/A (no references scraped)")
    print(f"  Reference scrape success rate      : {ref_success_rate:.2f}% ({stats['reference_scrape_successes']}/{stats['reference_scrape_attempts']})")
    
    print(f"\n{'='*80}")
    print("SCRAPER PERFORMANCE - RUNNING TIME:")
    print(f"{'='*80}")
    if stats['paper_processing_times']:
        total_time = sum(stats['paper_processing_times'])
        print(f"  Total processing time               : {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print(f"  Average time per paper              : {avg_processing_time:.2f} seconds")
    else:
        print(f"  Average time per paper              : N/A")
    if stats['entry_discovery_time'] > 0:
        print(f"  Total entry discovery time          : {stats['entry_discovery_time']:.2f} seconds ({stats['entry_discovery_time']/60:.2f} minutes)")
    
    print(f"\n{'='*80}")
    print("SCRAPER PERFORMANCE - MEMORY FOOTPRINT:")
    print(f"{'='*80}")
    print(f"  Maximum RAM used                    : {format_bytes(stats['max_memory_used'])}")
    if stats['memory_samples']:
        print(f"  Average RAM consumption             : {format_bytes(avg_memory)}")
    else:
        print(f"  Average RAM consumption             : N/A")
    print(f"  Maximum disk storage required       : {format_bytes(stats['max_disk_used'])}")
    print(f"  Final output storage size            : {format_bytes(final_disk_size)}")
    
    print(f"{'='*80}\n")


def run_parallel_processing(start_month, start_id, end_month, end_id, 
                            max_parallels=5, save_dir="./23127238"):
    """
    Main function to run parallel processing of papers.
    
    Args:
        start_month: Start month in format "YYYY-MM"
        start_id: Starting ID number
        end_month: End month in format "YYYY-MM"
        end_id: Ending ID number
        max_parallels: Number of parallel threads (default: 5)
        save_dir: Directory to save data
    """
    # Reset stats
    with stats_lock:
        for key in stats:
            stats[key] = 0
    
    # Generate paper IDs
    paper_ids = generate_paper_ids(start_month, start_id, end_month, end_id, save_dir)
    total_papers = len(paper_ids)
    
    print(f"\n{'='*80}")
    print("STARTING PARALLEL PROCESSING")
    print(f"{'='*80}")
    print(f"Range: {start_month} ID {start_id} → {end_month} ID {end_id}")
    print(f"Total papers to process: {total_papers}")
    print(f"Parallel threads: {max_parallels}")
    print(f"Output directory: {save_dir}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    
    # Track initial disk usage
    initial_disk = get_disk_usage(save_dir)
    
    # Process papers in parallel
    with ThreadPoolExecutor(max_workers=max_parallels) as executor:
        futures = {
            executor.submit(process_paper, arxiv_id, save_dir): arxiv_id 
            for arxiv_id in paper_ids
        }
        
        completed = 0
        for future in as_completed(futures):
            arxiv_id = futures[future]
            completed += 1
            
            try:
                result = future.result()
                paper_id, crawler_ok, refs_ok, proc_time, size_before, size_after, ref_count = result
                status = "✓✓" if (crawler_ok and refs_ok) else \
                         "✓X" if (crawler_ok and not refs_ok) else \
                         "XX"
                print(f"\n[{completed}/{total_papers}] {status} {paper_id} (Time: {proc_time:.2f}s)")
                
                # Track maximum disk usage
                current_disk = get_disk_usage(save_dir)
                with stats_lock:
                    if current_disk > stats["max_disk_used"]:
                        stats["max_disk_used"] = current_disk
                
                # Print progress every 10 papers
                if completed % 10 == 0:
                    print_progress_report()
                    
            except Exception as e:
                print(f"\n[{completed}/{total_papers}] !! {arxiv_id} - Error: {e}")
    
    elapsed_time = time.time() - start_time
    
    # Update final disk size
    final_disk = get_disk_usage(save_dir)
    with stats_lock:
        stats["final_disk_size"] = final_disk
    
    # Print final report with percentages
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE!")
    print(f"{'='*80}")
    print(f"Total time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    if total_papers > 0:
        print(f"Average time per paper: {elapsed_time/total_papers:.2f} seconds")
    print_final_report(save_dir)


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    # === CONFIGURATION ===
    
    # # Đạt 1
    # START_MONTH = "2023-04"
    # START_ID = 14607
    # END_MONTH = "2023-04"
    # END_ID = 15010
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./36"
    
    # # Đạt 2
    # START_MONTH = "2023-05"
    # START_ID = 1
    # END_MONTH = "2023-05"
    # END_ID = 2596
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./36"
    
    # lỗi từ 3715
    # Nam Việt 

    # START_ID = 2597
    # END_ID = 5596
    # Nam Viet range moi: 2305.12000-2305.014596
    # 2597-2636
    # START_ID = 9595
    START_MONTH = "2023-05"
    START_ID = 9595
    END_MONTH = "2023-05"
    END_ID = 9600
    MAX_PARALLELS = 3
    SAVE_DIR = "./4444"
    
    # # Nhân 1
    # START_MONTH = "2023-05"
    # START_ID = 5597
    # END_MONTH = "2023-05"
    # END_ID = 8596
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./23127238"
    
    # # Nhân 2
    # START_MONTH = "2023-05"
    # START_ID = 8597
    # END_MONTH = "2023-05"
    # END_ID = 11596
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./23127238"
    
    
    # # Nhân 3
    # START_MONTH = "2023-05"
    # START_ID = 11597
    # END_MONTH = "2023-05"
    # END_ID = 14596
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./23127238"
    
    
    # START_MONTH = "2023-04"
    # START_ID = 1
    # END_MONTH = "2023-04"
    # END_ID = 15
    # MAX_PARALLELS = 3
    # SAVE_DIR = "./36"
    
    run_parallel_processing(
        start_month=START_MONTH,
        start_id=START_ID,
        end_month=END_MONTH,
        end_id=END_ID,
        max_parallels=MAX_PARALLELS,
        save_dir=SAVE_DIR
    )
