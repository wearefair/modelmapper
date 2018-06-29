# Model Mapper 0.1.0

## Deterministic Data Driven Schema Modeling

Auto generate ORM models, infer normalization and cleaning functionality directly from your csv files in a **deterministic** way.

# Why?

If you have ever dealt with CSVs as your data delivery method for the same model, you might have seen:

- Field names that change over time
- Field that are added or removed
- Different variations of CSV with different formats for Null, Boolean, Datetime, Decimal and etc. values.
- CSVs with hundreds of fields
- Handcrafting ORM and APIs based on all these variations.
- Handcrafting API endpoint for the models.

And the list goes on.

ModelMapper aims to solve all these problems by inferring the model from your CSVs.


# Install

`pip install modelmapper`


# How?

1. Import every training CSV one by one
2. Normalize the field names based on the rules defined in settings: `field_name_full_conversion` and `field_name_part_conversion`
3. Analyze all the values per field per CSV to infer the type of the data and the functionality needed to clean and convert the data to proper formats for the database.
4. Write the analysis results per CSV into individual TOML files. Up to this point no comparison between the CSVs are made.
5. Combine the results between different CSVs to decide what should be the final decision for a field.
6. Prompt the user if the system does not have high confidence in certain fields.
7. The user is provided with option to override field info in a seperate overrides TOML file.
8. Make the final decision about the field type and write into the ORM model file.
9. The user can go ahead and verify the fields that were inserted into the ORM model are correct.
10. Now the user can make Alembic migration files by doing alembic autogenerate.
11. ModelMapper provides the functionality to clean each row of data before inserting into database. However it is left up to the user to use that functionality.


# Workflow

1. Install modelmapper

    `pip install modelmapper`

2. Initiate the setup for a model

    `modelmapper init mymodel`

    The wizard will guide you for configuration.

3. Copy the training csv files to the same folder

4. Git commit so you can see the diff of what will be generated.

5. Generate the SQLAlchemy model and everything that is needed for cleaning your data!

    `modelmapper run mymodel_setup.toml`

6. Verify the generated models

7. Run Alembic Autogenerate to create the database migration files

8. Migrate the database

9. Import the data via modelmapper: Initiate the Mapper with the path to your setup TOML file and read clean the CSVs via get_csv_data_cleaned function.

10. It is left up to the user how to insert the cleaned data it into the database.

11. You have new fields in the CSV or something changed? DO NOT MODIFY THE GENERATED MODELS DIRECTLY. Instead, add this csv to the list of training csvs in your settings TOML file. Re-train the system. Use git diff to see what has been changed.
