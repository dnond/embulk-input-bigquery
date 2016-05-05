# Bigquery input plugin for Embulk

This plugin is embulk input from BigQuery.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no


## Configuration

- **service_account_file** : p12 file
- **project_id** : project_id of target_table
- **dataset_id** : dataset_id of target_table
- **table_id** : tablet_id of target_table
- **service_email** : servive_email


## Example

```yaml
in:
  type: bigquery
  service_account_file: /Users/my/my_project.p12
  project_id: my_project
  dataset_id: test_dataset
  table_id: test_table
  service_email: 1234567890-aaaabbbcccddd@apps.googleusercontent.com
```


## Build

```
$ rake
```
