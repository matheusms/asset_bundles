# Pipeline `validate_wkf.yml` (in `build_validate`)

The `validate_wkf.yml` file defines an Azure DevOps pipeline that runs whenever a **Pull Request (PR)** is opened. The purpose of this pipeline is to automatically validate the workflows included or modified in the PR, ensuring they meet the expected standards before being merged into the main branch.

## Purpose

This pipeline automates the validation of workflows by using a Python script (`validate_wkf.py`) to analyze workflow files and verify that:  
- The structure complies with the expected standard.  
- Mandatory configurations are included.  
- There are no formatting errors or inconsistencies.

If any issues are detected, the pipeline will display detailed messages highlighting the errors and providing guidance on what needs to be fixed before the Pull Request can be approved.

---

## Pipeline Structure

### Disable Automatic Triggers

```yaml
trigger: none
