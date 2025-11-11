# arXiv Paper Crawler

An automated tool for downloading and processing papers from arXiv, including metadata extraction, source file extraction, and reference extraction.

## Features

- **Download papers from arXiv**: Automatically download all versions of a paper from arXiv
- **Extract metadata**: Save information about title, authors, submission date, revision dates, venue, categories, abstract, etc.
- **Extract source files**: Download and extract source files (.tex, .bib) from different versions
- **Extract references**: Use Semantic Scholar API to fetch references that have arXiv IDs
- **Parallel processing**: Support for processing multiple papers concurrently to improve speed
- **Detailed statistics**: Track and report processing progress

## Environment Setup

### System Requirements

- Python 3.7 or higher
- `file` utility (on Linux/Mac) - optional but recommended for better file type detection

### Installation

1. **Clone or download this repository**

2. **Create a virtual environment (recommended)**

   ```bash
   # On Windows
   python -m venv venv
   venv\Scripts\activate

   # On Linux/Mac
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

   This will install:
   - `arxiv` - Python library for interacting with arXiv API
   - `requests` - HTTP library for making API calls to Semantic Scholar

## Project Structure

```
.
├── main.py                  # Main entry point to run the crawler
├── arxiv_crawler.py         # Module for downloading papers from arXiv
├── reference_extractor.py   # Module for extracting references from Semantic Scholar
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Code Execution

### Configuration

Open `main.py` and modify the configuration parameters at the bottom of the file:

```python
START_MONTH = "2023-04"      # Start month (format: YYYY-MM)
START_ID = 14607             # Starting ID
END_MONTH = "2023-05"        # End month (format: YYYY-MM)
END_ID = 14596               # Ending ID
MAX_PARALLELS = 3            # Number of parallel threads
SAVE_DIR = "./23127238"      # Output directory
```

**Parameters:**
- `START_MONTH` / `END_MONTH`: Date range in YYYY-MM format
- `START_ID` / `END_ID`: arXiv paper ID range (e.g., 2597 means paper 2305.02597)
- `MAX_PARALLELS`: Number of concurrent threads (recommended: 3-5)
- `SAVE_DIR`: Directory where downloaded papers will be saved

### Running the Program

```bash
python main.py
```

The program will:
1. Generate a list of arXiv IDs based on the date range
2. Process each paper in parallel:
   - Download all versions of the paper
   - Extract metadata and save to `metadata.json`
   - Extract source files (.tex, .bib) to `tex/` folder
   - Fetch references from Semantic Scholar and save to `references.json`
3. Display progress reports every 10 papers
4. Show final statistics upon completion

### Output Structure

Each paper is saved in its own directory with the following structure:

```
SAVE_DIR/
└── yymm-nnnnn/              # Example: 2305-04793
    ├── metadata.json        # Paper metadata
    ├── references.json      # List of references (only papers with arXiv IDs)
    └── tex/                 # Source files
        ├── yymm-nnnnnv1/    # Source files for version 1
        ├── yymm-nnnnnv2/    # Source files for version 2
        └── ...
```

### Output File Formats

#### metadata.json

```json
{
    "arxiv_id": "2305-04793",
    "paper_title": "Title of the paper",
    "authors": ["Author 1", "Author 2"],
    "submission_date": "2023-05-08",
    "revised_dates": ["2023-06-15"],
    "publication_venue": "Journal Name",
    "latest_version": 2,
    "categories": ["cs.AI", "cs.LG"],
    "abstract": "Abstract text...",
    "pdf_url": "https://arxiv.org/pdf/2305.04793.pdf"
}
```

#### references.json

```json
{
    "2305-01234": {
        "paper_title": "Referenced Paper Title",
        "authors": ["Author 1", "Author 2"],
        "submission_date": "2023-05-01",
        "semantic_scholar_id": "123456789",
    },
    ...
}
```

## Main Functions

### 1. `crawl_single_paper(arxiv_id, save_dir)`
Downloads a paper from arXiv:
- Downloads all versions of the paper
- Extracts and saves metadata
- Extracts source files (.tex, .bib)
- Removes unnecessary files

### 2. `extract_references_for_paper(arxiv_id, save_dir)`
Extracts reference list:
- Fetches references from Semantic Scholar API
- Only keeps references that have arXiv IDs
- Saves to `references.json`

### 3. `process_paper(arxiv_id, save_dir)`
Processes a complete paper:
- Runs crawler first
- Then extracts references (if crawler succeeded)

### 4. `run_parallel_processing(...)`
Processes multiple papers in parallel:
- Automatically generates arXiv ID list from date range
- Processes in parallel with configurable thread count
- Tracks and reports progress

## Important Notes

- **Rate limiting**: The program includes delays between requests to avoid rate limiting
- **Error handling**: The program handles errors such as non-existent papers, API errors, etc.
- **Retry logic**: Semantic Scholar API has automatic retry logic for rate limits
- **404 handling**: If a paper is not found in Semantic Scholar, an empty `references.json` file is saved
- **File detection**: The `file` utility (Linux/Mac) helps detect file types more accurately, but the program works without it

## Statistics

The program displays:
- Total number of papers processed
- Number of papers successful in both phases
- Number of papers successful only in crawler phase
- Number of failed papers
- Success rates

## Troubleshooting

- **Import errors**: Make sure all dependencies are installed: `pip install -r requirements.txt`
- **Rate limit errors**: Reduce `MAX_PARALLELS` or increase delays in the code
- **Missing papers**: Some papers may not exist in the specified range - this is normal
- **Semantic Scholar 404**: Some papers may not be indexed in Semantic Scholar - empty references.json will be saved

## License

This project is released under the MIT License (or your chosen license).

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
