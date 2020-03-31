=======================
Bodyport Data Challenge
=======================

--------------------
1. Data Organization
--------------------

    Bodyport:

    This dataset must be accessible across multiple data teams. How would you organize the raw data to make it available as efficiently as possible?

    Design a schema that would allow you to access subject information and raw data.

    This dataset was recently provided by a clinic of subjects who have recently come in for check ups. In the near future, another dataset will be provided that may or may not contain the same subjects and
    needs to be organized with this first dataset provided. Ensure your implementation can handle any future data coming in.

        Sepehr:

        There are 3 distinct needs implied here:

        1) Data Lake -- a filesystem to store raw, structured, and unstructured data blobs of any type
        2) Data Catalog -- crawled metadata aout the directory structure, loaded into a relational database, because filesystem searches are inefficient
        3) Data Warehouse -- reconciling new data with old in a heavily normalized relational database. Includes processed data elements.

        :underline: `1. THE DATA LAKE`

        Goal: enable safe programmatic access to raw data in a well-organized directory structure

        S3 is often the tool of choice here for storing raw incoming data due to its inexpensiveness and excellent quality.

        Data Lake design starts with organizing data in such a way as to anticipate likely changes to your incoming data patterns.

        I may recommend the following top-level S3 bucket for this dataset:

        ``s3://bodyport-data-lake/incoming/clinic={clinic_id}/measurement=ecg/latest/`` -- effectively, this is the bucket that clinics in question
        will have access to in order to upoad their latest files.

        Let's go over each part of the URL:

        `bodyport-data-lake` - This is the parent bucket for all of Bodyport's raw data -- unique in all of S3.

        `/incoming/` - indicates this bucket will contain data uploaded from outside sources and is not processed

        `/clinic={clinic_id}/` - I'm adding the additional anticipation of partnering with new clinics or entities to receive data.
        This way this clinic can have its own bucket. We'll call the current clinic `sf_state` in this example.
        The `clinic=` part serves the dual purpose of a) making it easy to programmatically extract
        the clinic ID knowing only the run path.

        `/measurement=ecg/` is included to anticipate other biomarker data in the future.

        `/latest/` Best practice for incoming data is for the provider to always upload into the same bucket, and our internal
        orchestrator (e.g. Airflow or AWS Pipeline) will listen for new data, and immediately move them into a newly bucket, uniquely named bucket, then deleting
        the content of `/latest/`.

        NB: This data structure is reproduced locally in this package under `./data`.

        ``bodyport/data/incoming/clinic=sf_state/measurement=ecg/2020-01-01/``
        (contains data received for this assignment)

        ``bodyport/data/incoming/clinic=sf_state/measurement=ecg/2021-01-01/``
        (contains new example data we will pretend is coming in in the future to ensure our logic
        for building the data warehouse works. it contains one new subject and one existing subject)

        The existing data structure makes "lookup" operations perfectly efficient-- i.e. given Subject X and Run Y, any team can
        programmatically fetch the raw data by constructing a URL:

        ``s3://bodyport-data-lake/incoming/clinic_bpm/ecg/latest/subject_{X}/run_{Y}.csv``

        and

        ``s3://bodyport-data-lake/incoming/clinic_bpm/ecg/latest/subject_{X}/run_{Y}_header.json``

        Of course, searching the filesystem is slow and inefficient, particularly in a cloud environment, for just about any other query type than a basic lookup.

        This limitation is addressed by maintaining a data catalog.

        :underline: 2) THE DATA CATALOG:

        To enable our team and others to ask more complex questions about the data we have, we'll want to crawl the filesystem periodically,
        generate some metadata about the data we have, and put that metadata in a database. Then we can apply all of the magic of SQL
        to systematically query our filesystem.

        We can then answer
        1) How many runs do we get per upload from clinic X?
        2) Which runs have come in after the last time we populated the data warehouse?

        I'll create a simple data catalog here in SQLite.

        At a minimum, we could use a single table called `run_metadata`:

        +-----------------+-----------+
        | raw_path        | VARCHAR   |
        +-----------------+-----------+
        | last_crawled_at | TIMESTAMP |
        +-----------------+-----------+
        | processed_at    | TIMESTAMP |
        +-----------------+-----------+

        :underline: `3) THE DATA WAREHOUSE`

        Reconciling new data with old is one of the jobs of a Data Warehouse.

        The Data Warehouse can also host engineered features and transformed data elements.

        For example, age and sex are provided in Run metadata (header.json files), and ideally we would like these
        at the  "subject" level so we can quickly answer questions like:

        1) How old is subject X?
        2) WHat's the average age of subjects?
        3) How many male and female subjects do we have?
        4) And, if we have done advanced feature engineering in, for example, python: Does the average heartbeat of men and women differ?

        Our ETL solution would extract this information and load into 2 tables:
        - `subject`
        - `run`

        Since SQL is not powerful enough to usefully analyze ECG timeseries data, I would recommend not storing
        it in the Data Warehouse. Lower dimensional representations or statistical summaries, such as average beats-per-minute, etc.
        can be stored, however.


2. Data aggregation:​
    The organized data needs to be able to get queried for technical and non-technical purposes.
    Describe the tools you would create in order to query the structured dataset.
    Implement two of these tools to query the data.
    **ORM**
    **CREATE DB SCHEMA**

3. Data preprocessing:​
    The raw data may require some level of preprocessing to make it easier to analyze.
    What methods would you use to clean the signals?
    Implement your method to produce a filtered set of signals.
    Organize the filtered data according to your implemented data schema from part 1.
    **GET MIN/MAX and apply filter (butterworth)**

4. Data interpretation and visualization:​
    Describe some of the key information contained in this filtered data.
    For instance, what are some prominent features that have been revealed in each time series that might be useful
    for further analysis and model development?
    How would you visualize this data? What plotting techniques would you use for this data set?
    **box plot of raw values, **

5. Data modeling:​
    How would you approach the question: “How can I distinguish between different individuals given only their ECG data?”
    Consider if there is any variation across an individual’s records, or across individuals that may be used.
    **tsfresh generate lots of features, view variations between and within individuals**

---



However, `header.json` files have information that pertain to the subject: age and sex.
Assuming `age` is measured at `date` given in `header.json`, we can infer the year of birth and place it at the patient level.

We'll want to pull these out and make them available at the subject level.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
