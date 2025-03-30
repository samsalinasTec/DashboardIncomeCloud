The files generated from processing in the [Transform Main script](./TablesTransformMain.py) are saved as CSV files within the VM. These files are then accessed by the [LoadFilesBQ](./LoadFilesBQ.py) to upload them to our cloud repository.

This script mainly consists of four sections:

- Loading credentials for repository access (previously configured through the Google Cloud Computing terminal).
- Loading CSV files and preparing them by formatting data types specifically required by the repository.
- Creating configuration settings to specify data types for each table.
- Uploading tables to our repository using these defined configurations.

When reviewing the script, you’ll notice it removes previously loaded tables (uploaded the previous day). Given that the cost of loading and storing data in BigQuery is very low, we have chosen this approach to simplify the correction of potential errors in previous uploads and to enable a complete reset in case the process malfunctions—allowing us to delete and reload all data if necessary.

However, from a theoretical standpoint, this approach is not optimal. Ideally, only new records should be appended to existing tables, and a separate module should provide functionality for a complete (hard) reset of the tables in the repository when needed. This functionality is still under development, and the script will be updated accordingly once this improvement has been implemented.