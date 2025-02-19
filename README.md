# Real-World JSON Schema Analysis

This project analyzes the adherence of real-world JSON documents to their referenced JSON schemas. By evaluating inconsistencies such as missing or extra fields, the study aims to identify patterns and provide insights for improving schema design and standardization.

## Initial Setup

This project collects and processes JSON schema files from public GitHub repositories using the **Sourcegraph API**. The dataset is built in multiple steps:

1. **Finding JSON Schema Files**  
   - The Sourcegraph API searches for `.json` files containing `"$schema": "https://json-schema.org/"`.  
   - Extracted file paths are saved in `repos.csv`.  
   
   ```sh
   pipenv run python slurp.py --outfile repos.csv
   ```

2. **Fetching Version History**  
   - Commit history for each schema file is retrieved using the GitHub API.  
   - Output is stored in `commits.json`.  

   ```sh
   pipenv run python fetch_history.py > commits.json
   ```

3. **Downloading JSON Schemas**  
   - JSON schema files are fetched and stored in the `data/` directory.  

   ```sh
   ./fetch_files.sh
   ```

4. **Validating JSON Schemas**  
   - Validates schemas, ensuring they conform to the JSON Schema standard.  
   - Valid schemas are stored in `valid_data/`.  

   ```sh
   pipenv run python validate_schemas.py
   ```

5. **Retrieving Metadata**  
   - Uses **FastText** for language detection and the GitHub API for license retrieval.  

   ```sh
   pipenv run python get_languages.py > languages.json
   pipenv run python get_licenses.py > licenses.json
   ```

6. **Splitting Data**  
   - The dataset is divided into train, test, and validation sets, ensuring related schemas remain in the same set.  

   ```sh
   pipenv run python train_split.py
   ```

## Project Overview

JSON Schema is widely used for defining the structure of JSON data. However, real-world JSON documents often **deviate from their schemas**, causing validation errors, disrupted workflows, and unreliable data. This project aims to **quantify these discrepancies** by:

- Analyzing **50,000+ JSON documents** referencing known schemas.  
- Identifying missing or extra fields in real-world data.  
- Evaluating **schema complexity** and adherence trends.  
- Proposing **schema design improvements** to enhance validation accuracy.

## Features

- **Schema Adherence Analysis**: Compares JSON documents with their referenced schemas to detect missing or extra fields.  
- **Field Usage Patterns**: Identifies commonly omitted or additional fields across multiple schemas.  
- **Schema Complexity Evaluation**: Analyzes the number of properties per schema and their impact on adherence.  
- **Statistical Insights**: Uses **histograms, box plots, and scatter plots** to visualize schema usage trends.  

## Technologies Used

- **Python (Pandas, NumPy, JSON5)**: Data processing and schema validation.  
- **Sourcegraph & GitHub API**: Collecting JSON schemas and associated commit histories.  
- **FastText**: Detecting the primary language used in schemas.  
- **Matplotlib & Seaborn**: Visualizing schema complexity and adherence trends.  

## Results and Insights

- **Schema Adherence Trends**: Many real-world JSON documents do not strictly follow their referenced schemas, with missing or extra fields being common.  
- **Schema Complexity vs. Adherence**: Simpler schemas tend to have better adherence, while complex schemas often result in missing fields.  
- **Recommendations for Schema Design**: Simplified schemas with better-defined required fields improve validation accuracy.  

By analyzing real-world schema usage, this project provides valuable insights for **developers, data engineers, and API designers** to create more practical and reliable JSON schemas.
