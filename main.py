import time
import os
import shutil
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
}


def process_paper(arxiv_id, save_dir="./23127238"):
    """
    Process a single paper: crawl data first, then extract references.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn
        save_dir: Directory to save data
    
    Returns:
        tuple: (arxiv_id, crawler_success, references_success)
    """
    print(f"\n{'='*80}")
    print(f"Processing paper: {arxiv_id}")
    print(f"{'='*80}")
    
    # Step 1: Crawl paper data
    crawler_success = crawl_single_paper(arxiv_id, save_dir)
    
    # Step 2: Extract references (only if crawler succeeded)
    references_success = False
    if crawler_success:
        references_success = extract_references_for_paper(arxiv_id, save_dir)
    else:
        print(f"X [{arxiv_id}] Skipping reference extraction (crawler failed)")
    
    # Update statistics
    with stats_lock:
        stats["total_processed"] += 1
        
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
    
    return arxiv_id, crawler_success, references_success


def check_paper_exists(arxiv_id, save_dir="./23127238"):
    """
    Check if a paper exists by attempting to crawl it.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn
        save_dir: Directory to save data
    
    Returns:
        bool: True if paper exists, False otherwise
    """
    success = crawl_single_paper(arxiv_id, save_dir)
    
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


def print_final_report():
    """Print final statistics with percentages."""
    total = stats['total_processed']
    
    # Calculate success rates
    both_success_rate = (stats['both_success'] / total * 100) if total > 0 else 0
    phase2_fail_rate = (stats['references_failed'] / total * 100) if total > 0 else 0
    
    print(f"\n{'='*80}")
    print("FINAL REPORT:")
    print(f"{'='*80}")
    print(f"  Total processed          : {stats['total_processed']}")
    print(f"  Both success             : {stats['both_success']}")
    print(f"  Only crawler success     : {stats['only_crawler_success']}")
    print(f"  Only references success  : {stats['only_references_success']}")
    print(f"  Crawler failed           : {stats['crawler_failed']}")
    print(f"  References failed (404)  : {stats['references_failed']}")
    print(f"  Both failed              : {stats['both_failed']}")
    print(f"\n{'='*80}")
    print("SUCCESS RATES:")
    print(f"{'='*80}")
    print(f"  Both phases success rate : {both_success_rate:.2f}%")
    print(f"  Phase 2 (references) fail: {phase2_fail_rate:.2f}%")
    print(f"{'='*80}")


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
                paper_id, crawler_ok, refs_ok = future.result()
                status = "✓✓" if (crawler_ok and refs_ok) else \
                         "✓X" if (crawler_ok and not refs_ok) else \
                         "XX"
                print(f"\n[{completed}/{total_papers}] {status} {paper_id}")
                
                # Print progress every 10 papers
                if completed % 10 == 0:
                    print_progress_report()
                    
            except Exception as e:
                print(f"\n[{completed}/{total_papers}] !! {arxiv_id} - Error: {e}")
    
    elapsed_time = time.time() - start_time
    
    # Print final report with percentages
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE!")
    print(f"{'='*80}")
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Average time per paper: {elapsed_time/total_papers:.2f} seconds" if total_papers > 0 else "")
    print_final_report()


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

    # END_ID = 5596
    START_MONTH = "2023-05"
    START_ID = 2597
    END_MONTH = "2023-05"
    END_ID = 2633
    MAX_PARALLELS = 3
    SAVE_DIR = "./1111"
    
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
