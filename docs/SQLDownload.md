These scripts primarily function to download data directly from our databases, saving it without any processing. The data is stored immediately for subsequent post-processing performed by other modules in separate scripts.

Keeping modules in separate scripts simplifies maintenance and enhances readability, as each script often includes complex queries that occupy a significant portion of the code. In these examples, the download scripts typically consist of three main sections:

- Credential loading and database access

- Query implementation (previously developed and tested)

- Saving retrieved data into a DataFrame (with minor pre-processing in some cases)

For credential loading, environment variables are used to avoid hardcoding sensitive information directly into the script, ensuring better security. The queries themselves are designed for Oracle databases (PL/SQL).

Each module represents a distinct query and is executed individually through a main script [SQL Download Main module](./SQLDownloadMain.py). This modular structure significantly aids code maintenance, improves readability, and makes it straightforward to add new modules when required.
