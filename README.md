# SHRUG API

## Project Overview

This repository contains a collection of APIs designed to provide easy access to the SHRUG (Socio-economic High-resolution Rural-Urban Geographic) dataset for India. SHRUG is a comprehensive database that offers high-resolution socio-economic data for rural and urban areas in India. Our APIs allow researchers, policymakers, and developers to efficiently query and analyze this rich dataset.

## Features

- Multiple APIs for different aspects of the SHRUG dataset
- Census data API covering the years 1991, 2001, and 2011
- Efficient data storage and retrieval using Parquet files and DuckDB
- FastAPI-based implementation for high performance

## Repository Structure

```
SHRUG-API/
├── data/
│   ├── raw/         # Raw CSV files (not tracked in git)
│   ├── processed/   # Processed Parquet files (not tracked in git)
│   └── convert-to-parquet.py
├── apis/
│   ├── shared/      # Shared utilities for APIs
│   ├── census/      # Census data API
│   └── [other_apis] # Other API directories
├── requirements.txt
├── README.md
└── .gitignore
```

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/your-username/SHRUG-API.git
   cd SHRUG-API
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Data Preparation

To convert the SHRUG CSV files to Parquet format for efficient API querying:

1. Place the SHRUG CSV files in the `data/raw/` directory.
2. Run the conversion script:
   ```
   python data/convert-to-parquet.py
   ```
3. Processed Parquet files will be saved in `data/processed/`.

## Available APIs

### Census API

The Census API provides access to population census data from the SHRUG dataset for the years 1991, 2001, and 2011.

For detailed information on using the Census API, please refer to the [Census API README](apis/census/README.md).

### [Other APIs]

[Brief description of other APIs, with links to their respective READMEs]

## Usage

[Provide general usage instructions or examples for the APIs]

## Development

[Instructions for developers who want to contribute to or extend the APIs]

## Deployment

[Information about deploying these APIs, e.g., using AWS Lambda and API Gateway]

## Data Source

This API provides access to the SHRUG dataset. For more information about SHRUG, please visit the [SHRUG website](https://www.devdatalab.org/shrug).

## License

This API for the SHRUG dataset is released under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License (CC BY-NC-SA 4.0), in alignment with the original SHRUG dataset's copyleft noncommercial use sharealike license.

Under this license, you are free to:

- **Share** — copy and redistribute the material in any medium or format
- **Adapt** — remix, transform, and build upon the material

Under the following terms:

- **Attribution** — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
- **NonCommercial** — You may not use the material for commercial purposes.
- **ShareAlike** — If you remix, transform, or build upon the material, you must distribute your contributions under the same license as the original.

No additional restrictions — You may not apply legal terms or technological measures that legally restrict others from doing anything the license permits.

For the full license text, please visit: [Creative Commons BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)

For more information on the SHRUG dataset and its original license, please visit [INSERT LINK TO SHRUG WEBSITE/LICENSE].

## Contact

For questions, suggestions, or issues related to this API, you have two options:

1. **GitHub Issues**: Please feel free to open an issue on this GitHub repository. This is the preferred method for bug reports, feature requests, or general questions about the API's functionality.

2. **Email**: For more specific or private inquiries, you can reach out to the maintainer directly at ejt@gwu.edu.

We appreciate your feedback and contributions to improving this API for the SHRUG dataset.