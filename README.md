# Model Mapper 0.1.0

## Data Driven Schema Modeling

Auto generate ORM models and GRPC Prorotbufs from your csv files!

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

9. Import the data via commandline or by import modelmapper
