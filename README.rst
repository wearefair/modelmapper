Model Mapper 0.4.5
==================

|CircleCI|

Deterministic Data Driven Schema Modeling
-----------------------------------------

Your data should define your database schema, not the other way!

Auto generate ORM models, infer normalization and cleaning functionality
directly from your csv files in a **deterministic** way.

Why?
====

If you have ever dealt with CSVs as your data delivery method for the
same model, you might have seen:

-  Field names that change over time
-  Field that are added or removed
-  Different variations of CSV with different formats for Null, Boolean,
   Datetime, Decimal and etc. values.
-  CSVs with hundreds of fields
-  Handcrafting ORM and APIs based on all these variations.
-  Handcrafting API endpoint for the models.

And the list goes on.

ModelMapper aims to solve all these problems by inferring the model from
your CSVs.

Install
=======

``pip install modelmapper``

Note: ModelMapper requires Python 3.6 or higher.

How?
====

1.  Import every training CSV one by one
2.  Normalize the field names based on the rules defined in settings:
    ``field_name_full_conversion`` and ``field_name_part_conversion``
3.  Analyze all the values per field per CSV to infer the type of the
    data and the functionality needed to clean and convert the data to
    proper formats for the database.
4.  Write the analysis results per CSV into individual TOML files. Up to
    this point no comparison between the CSVs are made.
5.  Combine the results between different CSVs to decide what should be
    the final decision for a field.
6.  Prompt the user if the system does not have high confidence in
    certain fields.
7.  The user is provided with option to override field info in a
    seperate overrides TOML file.
8.  Make the final decision about the field type and write into the ORM
    model file.
9.  The user can go ahead and verify the fields that were inserted into
    the ORM model are correct.
10. Now the user can make Alembic migration files by doing alembic
    autogenerate.
11. ModelMapper provides the functionality to clean each row of data
    before inserting into database. However it is left up to the user to
    use that functionality.

Workflow
========

1.  Install modelmapper

    ``pip install modelmapper``

2.  Initiate the setup for a model

    ``modelmapper init mymodel``

    The wizard will guide you for configuration.

3.  Copy the training csv files to the same folder

4.  Git commit so you can see the diff of what will be generated.

5.  Generate the SQLAlchemy model and everything that is needed for
    cleaning your data!

    ``modelmapper run mymodel_setup.toml``

6.  Verify the generated models

7.  Run Alembic Autogenerate to create the database migration files

8.  Migrate the database

9.  Import the data via modelmapper: Initiate the Mapper with the path
    to your setup TOML file and read clean the CSVs via
    get_csv_data_cleaned function.

10. It is left up to the user how to insert the cleaned data it into the
    database.

11. You have new fields in the CSV or something changed? DO NOT MODIFY
    THE GENERATED MODELS DIRECTLY. Instead, add this csv to the list of
    training csvs in your settings TOML file. Re-train the system. Use
    git diff to see what has been changed.

Settings
========

The power of ModelMapper lies in how you can easily change the settings,
train the model, look at the results, change the settings, add new
training csvs, etc and quickly iterate through your model.

The settings are initialized for you by running
``modelmapper init [identifier]``

Example:

.. code:: toml

    [settings]
    null_values = ["\\n", "", "na", "unk", "null", "none", "nan", "1/0/00", "1/0/1900", "-"]  # Any string that should be considered null
    boolean_true = ["true", "t", "yes", "y", "1"]  # Any string that should be considered boolean True
    boolean_false = ["false", "f", "no", "n", "0"]  # Any string that should be considered boolean False
    dollar_to_cent = true  # If yes, then when a field is marked as money field, the values in csv will be multiplied by 100 to be stored as cents in integer field. Even if the original data is decimal.
    percent_to_decimal = true  # If yes, then when a field is marked as percent values, the csv values will be divided by 100 to be put in database. Example: 10 becomes 0.10
    add_digits_to_decimal_field = 2  # This is for padding decimal fields. If the biggest decimal size in your training csvs is for example xx.xxx, then padding of 2 on each side will define a database field that can fit xxxx.xxxxx
    add_to_string_length = 32  # Padding for string fields. If the biggest string length in your training csvs is X, then the db field size will be X + padding.
    datetime_allowed_characters = "0123456789/:-"  # If a string value in your training csv has characters that are subset of characters in datetime_allowed_characters, then that string value will be evaluated for possibility of having datetime value.
    datetime_formats = ["%m/%d/%y", "%m/%d/%Y", "%Y%m%d"]  # The list of any possible datetime formats in all your training csvs.
    field_name_full_conversion = [] # Use this to tell ModelMapper which field names should be considered to be the same field. This is useful if you have field names changing across different csvs. Example: [['field 1', 'field a'], ['field 2', 'field b']]
    field_name_part_conversion = [["#", "num"], [" (e)", ""], ["(y/n)", ""], [" (s)", ""], [" (e,s)", ""], ["yyyymmdd", ""], [")", ""], ["(", ""], [": ", "_"], [" ", "_"], ["/", "_"], [".", "_"], ["-", "_"], ["%", "_percent"], ["?", ""], ["!", ""], [",", ""], ["'", ""], ["&", "_and_"], ["@", "_at_"], ["$", "_dollar_"], [">=", "_bigger_or_equal_"], [">", "_bigger_"], ["<=", "_less_or_equal_"], ["<", "_less_"], ["=", "_equal_"], ["___", "_"], ["__", "_"]]  # list of words in field name that should be replaced by another word.
    dollar_value_if_word_in_field_name = []  # If the field name has any of these words, consider it as money field. It only matters if dollar_to_cent is True
    non_string_fields_are_all_nullable = true  # If yes, any non string field will be automatically nullable. Otherwise only if you have null values in your training csv, then it will be marked as nullable.
    string_fields_can_be_nullable = false  # Normally string fields should not be nullable since they can be just empty. If you set it to True, then if there are null values inside the string field in any of the training csvs, it will mark the field is nullable.
    training_csvs = []  # The list of relative paths to the training csvs
    output_model_file = ''  # The relative path to the ORM model file that the output generated model will be inserted into.

    [settings.max_int]
    32767 = "SmallInteger"  # An integer field with ALL numbers below this in your training csv will be marked as SmallInteger. If you don't want any SmallIntegerfields, then remove this line.
    2147483647 = "Integer"  # An integer field with ALL numbers below this but at least one above SmallInteger in your training csv will be marked as Integer
    9223372036854775807 = "BigInteger"  # An integer field with ALL numbers below this but at least one above Integer in your training csv will be marked as BigInteger

F.A.Q
=====

Is ModelMapper a one-off tool?
------------------------------

No. ModelMapper is designed to be deterministic. If it does not infer
any data type changes in your training CSVs, it should keep your model
intact. The idea is that your data should define your model, not the
other way. ModelMapper will update your model ONLY if it infers from
your data that a change in your ORM schema is needed.

I have certain fields in my ORM model that are not in the training CSVs. How does that work?
--------------------------------------------------------------------------------------------

ModelMapper only deals with the chunk in your ORM file that is inbetween
ModelMapper’s markers. You can have any other field and functionality
outside those markers and ModelMapper won’t touch them.

Seems like ModelMapper is susceptible to SQL injection
------------------------------------------------------

The training of ModelMapper should NEVER happen on a live server.
ModelMapper is ONLY intended for the development time. All it focuses on
is to help the developer make the right choices in automatic fashion. It
has no need to even think about SQL injection. You have to use your
ORM’s recommended methods to escape the data before putting it into your
database.

.. |CircleCI| image:: https://circleci.com/gh/wearefair/modelmapper.svg?style=svg
   :target: https://circleci.com/gh/wearefair/modelmapper
