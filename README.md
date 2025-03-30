This project was developed for a marketing company; however, for security reasons, all sensitive information has been replaced, simulating a cloud services company. Additionally, for each section of the “ETL,” only examples based on the original code have been used (since it contained explicit references to important company information).

However, the code is essentially the same as what was used for the real ETL. If you want a deeper understanding of the project—whether to resolve very specific questions or to learn how certain problems were solved in order to replicate this project—feel free to contact me.

In general, this project represents a comprehensive application of knowledge in databases, data analysis, programming, data structures and modeling, ETL workflows, data visualization, and the use of KPIs for business decision-making.

Below, I will provide a general overview of how the project works—from data extraction to visualization—as well as describe how each script relates to its purpose. Additionally, I have created specific sections for each part of the data flow, which I'll refer to for more detailed explanations.

The core purpose of this project is to provide essential business information to support day-to-day decision-making. To accomplish this effectively, the most viable solution is to visualize key indicators within a dashboard, highlighting relevant business aspects. This dashboard must be capable of retrieving updated data daily; consequently, all underlying data sources must also be updated on a daily basis.

At first glance, the problem seems straightforward: we simply need to retrieve data from our databases and create visualizations and measures in the dashboard to track performance indicators. However, the data required to generate meaningful insights is not directly available without first applying transformations. Furthermore, a significant portion of our database is outdated, resulting in inefficient data modeling. Therefore, aside from transforming the data, we must establish a robust data model/structure that clearly represents the interactions between different data sources and is scalable for future additions. With this in mind, a star schema data model was developed. Thus, data extraction and transformations become even more critical, as they enable the creation of this structured model, ultimately providing the information necessary for informed business decision-making

The process begins with the [SQL Download Main module](./SQLDownloadMain.py) scripts, which are responsible for extracting information from our databases. These scripts load the credentials needed to access the databases, execute queries, download the data, and save it into CSV files.

Once the data has been downloaded, the [Transform Main script](./TablesTransformMain.py) scripts take over. These scripts clean the data, create new columns, and perform the necessary transformations to meet both the technical requirements of our data model and the specific informational goals we aim to achieve. In this way, the raw files generated by the [SQL Download Main module](./SQLDownloadMain.py) scripts become the final tables required for our data model.

The final step of the ETL process involves loading the structured data into a database. In our case, we chose BigQuery as our cloud data repository. The loading [Load Files to BQ](./LoadFilesBQ.py) scripts format data types appropriately and upload the processed data into BigQuery.

With our data now stored in the cloud repository, we can directly access it using data processing and visualization tools like Power BI. By querying BigQuery from Power BI, we can retrieve these tables and create visualizations that provide clear and immediate insights into the performance of predefined KPIs. Once the data modeling has been set up in Power BI, it only remains necessary to create specific measures for particular sections of our dashboard. Additionally, special attention was given to UX and UI design, incorporating intuitive layouts and interactive buttons wherever possible.

It's important to mention that the use of a cloud repository significantly resolves challenges related to automating the entire process. To enable automated updates in Power BI, we needed to avoid limitations associated with directly querying traditional databases (such as restrictions on simultaneous sessions), querying local CSV files, managing access keys, and handling slow download speeds. BigQuery effectively addresses many of these challenges.
