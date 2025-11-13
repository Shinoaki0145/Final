import os
import re
import time
import tarfile
import shutil
import subprocess
import gzip
import concurrent.futures
import requests


START_MONTH = "2023-05"
START_ID = 13437
END_MONTH = "2023-05"
END_ID = 14136
MAX_PARALLELS = 3
SAVE_DIR = "./namviet3"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

RETRY_DELAY = 5
RETRY_DELAY_MAX_CAP = 60



def detect_and_fix_filetype(tar_path):

    try:
        result = subprocess.run(["file", tar_path], capture_output=True, text=True, errors='ignore')
        output = result.stdout.strip()
    except FileNotFoundError:
        print("X 'file' command not found. Install 'file' utility.")
        return tar_path, "unknown", None
    except Exception as e:
        print(f"X Error running 'file': {e}")
        return tar_path, "unknown", None

    if "PDF document" in output:
        print(f"   -> Detected PDF: {os.path.basename(tar_path)}")
        return tar_path, "pdf", None
    elif "gzip compressed data" in output:
        match = re.search(r', was "([^"]+)"', output)
        if match:
            return tar_path, "gz", os.path.basename(match.group(1))
        else:
            return tar_path, "tar.gz", None
    elif "tar archive" in output:
        return tar_path, "tar.gz", None
    else:
        print(f"   Unknown format: {output}")
        return tar_path, "unknown", None


def extract_and_clean(tar_path, dest_folder, base_name):

    fixed_path, filetype, orig_name = detect_and_fix_filetype(tar_path)
    
    extract_path = os.path.join(dest_folder, base_name)
    os.makedirs(extract_path, exist_ok=True)
    deleted = 0

    if filetype == "pdf":
        return (os.path.basename(tar_path), True, 0, "pdf")
    if filetype == "unknown":
        return (os.path.basename(tar_path), False, 0, "unknown")

    try:
        if filetype == "tar.gz":
            with tarfile.open(fixed_path, 'r:*') as tar:
                tar.extractall(path=extract_path)
        elif filetype == "gz":
            out_name = orig_name or f"{base_name}.file"
            out_path = os.path.join(extract_path, out_name)
            with gzip.open(fixed_path, 'rb') as fin, open(out_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout)
                
    except Exception as e:
        print(f"X Extract error: {e}")
        shutil.rmtree(extract_path, ignore_errors=True)
        return (os.path.basename(tar_path), False, 0, "extract_fail")


    for root, _, files in os.walk(extract_path):
        for f in files:
            if not f.lower().endswith(('.tex', '.bib')):
                try:
                    os.remove(os.path.join(root, f))
                    deleted += 1
                except:
                    pass
                    
    return (os.path.basename(tar_path), True, deleted, "ok")


def process_paper_versions_bs4(arxiv_id, save_dir):

    versions_processed = 0


    if '.' not in arxiv_id:
        print(f"X Invalid arxiv_id: {arxiv_id}")
        return False

    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    tex_folder = os.path.join(paper_folder, "tex")
    os.makedirs(tex_folder, exist_ok=True)

    print(f"[{arxiv_id}] Checking versions v1 through v10...")


    consecutive_404_count = 0 


    for v in range(1, 11): 
        version_id_str = f"{arxiv_id}v{v}"
        version_folder_name = f"{prefix}-{suffix}v{v}"
        
        temp_tar = os.path.join(paper_folder, f"{version_id_str}.tar.gz")
        download_url = f"https://arxiv.org/e-print/{version_id_str}"

        download_success = False
        was_404 = False 
        current_attempt = 0

        while True: 
            current_attempt += 1
            try:
                print(f"   [{arxiv_id}] Downloading {version_id_str} (Attempt {current_attempt})...")
                
                with requests.get(download_url, headers=HEADERS, stream=True, timeout=30) as r:
                    
                    if r.status_code == 404:
                        if current_attempt == 1:
                            print(f"   [{arxiv_id}] Skipping {version_id_str}: No source file (404).")
                        else:
                            print(f"   [{arxiv_id}] Skipping {version_id_str}: File not found (404) after retry.")
                        
                        was_404 = True
                        break  
                    
                    r.raise_for_status() 
                    
                    with open(temp_tar, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): 
                            f.write(chunk)
                    
                    download_success = True 
                    break 

            except requests.exceptions.RequestException as e:
                print(f"X [{arxiv_id}] Error downloading {version_id_str}: {e}.")
                wait_time = min(RETRY_DELAY * (2 ** (current_attempt - 1)), RETRY_DELAY_MAX_CAP) 
                print(f"   [{arxiv_id}] Retrying in {wait_time}s...")
                time.sleep(wait_time)
            
            except Exception as e:
                print(f"X [{arxiv_id}] Unexpected error {version_id_str}: {e}. Skipping version.")
                break 


        if was_404:
            consecutive_404_count += 1
        else:
            consecutive_404_count = 0

        if consecutive_404_count >= 2:
            print(f"   [{arxiv_id}] Stopping check: Found 2 consecutive 404s (at v{v-1} and v{v}).")
            break 



        if download_success:
            try:
                file_name, success, deleted_count, ftype = extract_and_clean(
                    temp_tar, tex_folder, version_folder_name
                )
                if success:
                    versions_processed += 1
                    print(f"   [{arxiv_id}] Extracted & cleaned: {version_id_str} ({deleted_count} files removed, type: {ftype})")
                else:
                    print(f"X [{arxiv_id}] Failed to extract {version_id_str} (type: {ftype})")
            except Exception as e:
                print(f"X [{arxiv_id}] Extraction error {version_id_str}: {e}")


        if os.path.exists(temp_tar):
            try:
                os.remove(temp_tar)
            except OSError as e:
                print(f"X [{arxiv_id}] Warning: Could not delete temp file {temp_tar}: {e}")

        if download_success:
            time.sleep(3) 
        elif not was_404:
            time.sleep(1) 



    success = (versions_processed > 0)
    if success:
        print(f"✓ [{arxiv_id}] COMPLETED (Processed {versions_processed} versions from v1-v10 check)")
    else:
        print(f"✓ [{arxiv_id}] COMPLETED (0 versions extracted, likely all PDF/no-source)")
        return True 

    return success


def generate_arxiv_ids(start_month, start_id, end_month, end_id):
    
    if start_month != end_month:
        print("Just in the same month.")

    month_prefix = start_month.split('-')[0][2:] + start_month.split('-')[1]
    
    ids_to_process = []
    for i in range(start_id, end_id + 1):
        id_suffix = str(i).zfill(5)
        ids_to_process.append(f"{month_prefix}.{id_suffix}")
        
    return ids_to_process


def run_crawl():
    
    print(f"Starting crawl (Brute-force v1-v10, Stop on 2 consecutive 404s)...")
    print(f"Range: {START_MONTH}.{str(START_ID).zfill(5)} to {END_MONTH}.{str(END_ID).zfill(5)}")
    print(f"Max parallel jobs: {MAX_PARALLELS}")
    print(f"Save directory: {SAVE_DIR}")
    print("-" * 30)

    os.makedirs(SAVE_DIR, exist_ok=True)
    
    arxiv_ids = generate_arxiv_ids(START_MONTH, START_ID, END_MONTH, END_ID)
    total_papers = len(arxiv_ids)
    print(f"Generated {total_papers} arXiv IDs to process.")

    success_count = 0
    fail_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLELS) as executor:
        
        future_to_id = {
            executor.submit(process_paper_versions_bs4, arxiv_id, SAVE_DIR): arxiv_id 
            for arxiv_id in arxiv_ids
        }

        for i, future in enumerate(concurrent.futures.as_completed(future_to_id)):
            arxiv_id = future_to_id[future]
            try:
                result_success = future.result() 
                if result_success:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as exc:
                print(f"X [{arxiv_id}] Generated an unhandled exception: {exc}")
                fail_count += 1
            
            print(f"--- Progress: {i+1}/{total_papers} | Success: {success_count} | Failed: {fail_count} ---")

    print("\n" + "=" * 30)
    print("Crawl Finished.")
    print(f"Total Successful: {success_count}")
    print(f"Total Failed: {fail_count}")
    print("=" * 30)


if __name__ == "__main__":
    run_crawl()