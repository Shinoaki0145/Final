import requests
import json
import os
import time
import re


def format_arxiv_id_for_key(arxiv_id):
    """
    Convert arXiv ID to folder format (yymm-nnnnn).
    Examples:
        "2305.04793" -> "2305-04793"
        "2305.04793v1" -> "2305-04793"
    """
    # Remove version suffix if present
    clean_id = re.sub(r'v\d+$', '', arxiv_id)
    # Replace dot with dash
    return clean_id.replace('.', '-')


def get_paper_references(arxiv_id, delay=2):
    """
    Fetch references for a paper from Semantic Scholar API.
    Retries indefinitely until success or 404.
    
    Args:
        arxiv_id: arXiv ID (format: YYMM.NNNNN or YYMM.NNNNNvN)
        delay: delay between retries in seconds
    
    Returns:
        tuple: (list of references, total_found_count) or (None, 0) if 404 error
    """
    # Clean arxiv_id (remove version suffix if present)
    clean_id = re.sub(r'v\d+$', '', arxiv_id)
    url = f"https://api.semanticscholar.org/graph/v1/paper/arXiv:{clean_id}"
    params = {
        "fields": "references,references.title,references.authors,references.year,references.venue,references.externalIds,references.publicationDate"
    }
    
    while True:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                references = data.get("references", [])
                total_found = len(references) if references else 0
                return references, total_found
            elif response.status_code == 429:
                print(f"  [{arxiv_id}] Rate limit hit. Waiting {delay}s...")
                time.sleep(delay)
            elif response.status_code == 404:
                print(f"  [{arxiv_id}] Paper not found in Semantic Scholar (404)")
                return None, 0  # Return None to indicate 404 error
            else:
                print(f"  [{arxiv_id}] API returned status {response.status_code}, retrying in {delay}s...")
                time.sleep(delay)
        except requests.exceptions.RequestException as e:
            print(f"  [{arxiv_id}] Request error: {e}, retrying in {delay}s...")
            time.sleep(delay)


def convert_to_references_dict(references):
    """
    Convert Semantic Scholar references to the required format:
    Dictionary with arXiv IDs as keys (in "yyyymm-id" format) for papers with arXiv IDs.
    
    Args:
        references: List of references from Semantic Scholar API
    
    Returns:
        dict: Dictionary with paper IDs as keys and metadata as values
    """
    result = {}
    
    for ref in references:
        # Skip if reference is None or empty
        if not ref:
            continue
        
        # Extract external IDs (may be None)
        external_ids = ref.get("externalIds", {})
        if external_ids is None:
            external_ids = {}
        
        arxiv_id = external_ids.get("ArXiv", "")
        
        # Only keep references that have arXiv_id
        if not arxiv_id:
            continue
        
        # Use arXiv ID in yyyymm-id format
        key = format_arxiv_id_for_key(arxiv_id)
        
        # Extract authors
        authors_list = ref.get("authors", [])
        authors = [author.get("name", "") for author in authors_list if author.get("name")]
        
        # Extract dates (use publicationDate if available)
        publication_date = ref.get("publicationDate", "")
        year = ref.get("year")
        
        # If no publication date but have year, create an ISO-like format
        if not publication_date and year:
            publication_date = f"{year}-01-01"  # Use Jan 1st as placeholder
        
        # Build metadata dictionary with required fields
        metadata = {
            "paper_title": ref.get("title", ""),
            "authors": authors,
            "submission_date": publication_date if publication_date else "",
            "semantic_scholar_id": ref.get("paperId"),
            "year": year
        }
        
        result[key] = metadata
    
    return result


def extract_references_for_paper(arxiv_id, save_dir="./23127238"):
    """
    Extract references for a paper and save to references.json.
    
    Args:
        arxiv_id: arXiv ID in format yymm.nnnnn (e.g., "2305.04793")
        save_dir: Base directory containing paper folders
    
    Returns:
        bool: True if successful (found and saved references), False otherwise
    """
    # Convert arxiv_id to folder format
    paper_id_key = format_arxiv_id_for_key(arxiv_id)
    paper_folder = os.path.join(save_dir, paper_id_key)
    
    # Check if the folder exists
    if not os.path.exists(paper_folder):
        print(f"X [{arxiv_id}] Paper folder not found: {paper_folder}")
        return False

    print(f"[{arxiv_id}] Fetching references...")

    try:
        json_path = os.path.join(paper_folder, "references.json")
        references, total_found = get_paper_references(arxiv_id)

        # If we got None (404 error), save empty file and return failure
        if references is None:
            print(f"X [{arxiv_id}] Failed to fetch references from Semantic Scholar (404)")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2, ensure_ascii=False)
            return False
        
        if not references or total_found == 0:
            print(f"X [{arxiv_id}] No references found (total_found: 0)")
            # Save empty dict but return False
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2, ensure_ascii=False)
            return False

        references_dict = convert_to_references_dict(references)
        total_saved = len(references_dict)
        
        # Save only the references dict (no statistics in JSON)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(references_dict, f, indent=2, ensure_ascii=False)
        
        # Log statistics to console only
        print(f"✓ [{arxiv_id}] Found {total_found} references, saved {total_saved} (with arXiv IDs) to references.json")
        return True
        
    except Exception as e:
        print(f"X [{arxiv_id}] Error extracting references: {e}")
        return False