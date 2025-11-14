import arxiv
import os
import re
import json
import time
import tarfile
import shutil
import subprocess
import gzip

SAVE_DIR = "./23127238"


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
        print(f"  -> Detected PDF: {os.path.basename(tar_path)}")
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
        print(f"  Unknown format: {output}")
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

    # Clean: keep only .tex and .bib
    for root, _, files in os.walk(extract_path):
        for f in files:
            if not f.lower().endswith(('.tex', '.bib')):
                try:
                    os.remove(os.path.join(root, f))
                    deleted += 1
                except:
                    pass
    return (os.path.basename(tar_path), True, deleted, "ok")


def crawl_single_paper(arxiv_id, save_dir=SAVE_DIR):
    """
    Download and process a single arXiv paper with all its versions.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn (e.g., "2305.04793")
        save_dir: Directory to save the paper data
    
    Returns:
        bool: True if successful, False otherwise
    """
    client = arxiv.Client()
    paper_folder = None
    tex_folder = None
    versions_processed = 0
    latest_version = 0

    # Validate and split ID
    if '.' not in arxiv_id:
        print(f"X Invalid arxiv_id: {arxiv_id}")
        return False

    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    tex_folder = os.path.join(paper_folder, "tex")
    os.makedirs(tex_folder, exist_ok=True)

    # Get latest version from v1
    try:
        search = arxiv.Search(id_list=[arxiv_id])
        base_paper = next(client.results(search))
        match = re.search(r'v(\d+)$', base_paper.entry_id)
        latest_version = int(match.group(1)) if match else 1
        print(f"[{arxiv_id}] Found {latest_version} version(s)")
    except StopIteration:
        print(f"X [{arxiv_id}] Paper not found")
        return False
    except Exception as e:
        print(f"X [{arxiv_id}] Error finding latest version: {e}")
        return False

    # --- Collect metadata from v1 ---
    title = base_paper.title
    authors = [a.name for a in base_paper.authors]
    submission_date = base_paper.published.strftime("%Y-%m-%d") if base_paper.published else None
    publication_venue = base_paper.journal_ref if base_paper.journal_ref else None
    categories = base_paper.categories
    abstract = base_paper.summary.replace("\n", " ").strip()
    pdf_url = base_paper.pdf_url
    revised_dates = []

    # Get revised dates for v2..vN
    if latest_version > 1:
        for v in range(2, latest_version + 1):
            try:
                vid = f"{arxiv_id}v{v}"
                search_v = arxiv.Search(id_list=[vid])
                paper_v = next(client.results(search_v))
                revised_dates.append(paper_v.updated.strftime("%Y-%m-%d") if paper_v.updated else None)
            except:
                revised_dates.append(None)

    metadata = {
        "arxiv_id": arxiv_id.replace('.', '-'),
        "paper_title": title,
        "authors": authors,
        "submission_date": submission_date,
        "revised_dates": revised_dates,
        "publication_venue": publication_venue,
        "latest_version": latest_version,
        "categories": categories,
        "abstract": abstract,
        "pdf_url": pdf_url,
    }

    metadata_path = os.path.join(paper_folder, "metadata.json")
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        print(f"  [{arxiv_id}] Saved metadata.json")
    except Exception as e:
        print(f"X [{arxiv_id}] Failed to save metadata: {e}")
        return False

    # Download all versions into tex folder
    for v in range(1, latest_version + 1):
        version_id = f"{arxiv_id}v{v}"
        version_folder_name = f"{prefix}-{suffix}v{v}"
        temp_tar = os.path.join(paper_folder, f"{version_id}.tar.gz")

        try:
            search_v = arxiv.Search(id_list=[version_id])
            paper_v = next(client.results(search_v))

            print(f"  [{arxiv_id}] Downloading {version_id}...")
            paper_v.download_source(dirpath=paper_folder, filename=f"{version_id}.tar.gz")

            # Extract & Clean into tex folder
            file_name, success, deleted_count, ftype = extract_and_clean(temp_tar, tex_folder, version_folder_name)

            if success:
                versions_processed += 1
                print(f"  [{arxiv_id}] Extracted & cleaned: {version_id} ({deleted_count} files removed)")
            else:
                print(f"X [{arxiv_id}] Failed to extract {version_id}")

            # Delete .tar.gz
            try:
                os.remove(temp_tar)
            except:
                pass

            time.sleep(0.3)  # Be nice to arXiv

        except StopIteration:
            print(f"X [{arxiv_id}] Version {version_id} not found")
            continue
        except Exception as e:
            print(f"X [{arxiv_id}] Download error {version_id}: {e}")
            continue

    # Final check
    success = (versions_processed > 0)
    if success:
        print(f"✓ [{arxiv_id}] COMPLETED ({versions_processed}/{latest_version} versions)")
    else:
        print(f"X [{arxiv_id}] FAILED - no versions downloaded")

    return success
