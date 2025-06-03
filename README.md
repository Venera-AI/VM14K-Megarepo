# VM14K-Megarepo: Medical Data Processing and LLM Benchmarking Pipeline
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)
This repository contains the complete pipeline for the paper "VM14K: First Vietnamese Medical Benchmark". It implements a comprehensive workflow from medical data acquisition to large language model (LLM) evaluation, including data scraping, cleaning, deduplication, inference benchmarking, and performance assessment. 

<a href="https://venera-ai.github.io/VM14K/" target="_blank" style="display: inline-block; padding: 6px 10px; background-color: #0d6efd; color: white; text-decoration: none; border-radius: 8px; font-weight: bold; text-shadow: 0 0 5px rgba(255,255,255,0.5); box-shadow: 0 0 15px rgba(13, 110, 253, 0.7);">🌟 Website</a>
## Table of Contents
- [License](#license)
- [Repository Structure](#repository-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [LLM Benchmarking](#llm-benchmarking)
- [Evaluation](#evaluation)
- [Citation](#citation)
- [Contributing](#contributing)
- [Contact](#contact)
## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE.md) file for details.
## Repository Structure
```
VM14K-Megarepo/
    > DataPlatformEtlService       # Medical data scraping pipeline
    > DataCleaning        # Cleaning using LLM
    > Deduplication       # Deduplication process
    > Inference            # Run Inference baseline on difference models
    > Evaluation # Run evaluation metric 
    README.md
```
## DataPlatformEtlService
The complete data processing workflow includes:
1. Web scraping of medical sources

## DataCleaning 
The cleaning process uiltilize LLM to remove the extra data like html tag,index,... from the benchmark data. It also reformat all question into the same format
## Deduplication
This process remove the duplicate question from previous step
## Inference
The benchmarking framework supports:
- Multiple LLM providers (OpenAI,Gemini LLaMA, etc.)
- Parallel inference
- Cost tracking
## Evaluation
The evaluation suite includes:
- F1 accuracy assessment
- Pass@k metric
<!-- ## Citation
If you use this repository in your research, please cite our paper:
```bibtex
@article{yourcitationkey,
  title={Your Paper Title},
  author={Author List},
  journal={Journal Name},
  year={2023},
  publisher={Publisher}
}
``` -->
<!-- ## Contributing
Contributions are welcome. Please open an issue first to discuss proposed changes. -->
<!-- ## Contact
For questions about this repository, please contact:
- [Your Name] (your.email@institution.edu)
- [Co-author Name] (coauthor.email@institution.edu) -->
