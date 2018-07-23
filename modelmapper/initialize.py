import os
from modelmapper.ui import get_user_choice, get_user_input, YES_NO_CHOICES, split_user_input
from modelmapper.misc import _validate_file_has_start_and_end_lines, load_toml, write_settings
from modelmapper.base import OVERRIDES_FILE_NAME

TRAINING_CSV_MSG = """
Please provide the relative path to the training csv files.
example: training1.csv
example2: "training1.csv","training2.csv"
"""


def _is_valid_path(user_input, setup_dir):
    full_path = os.path.join(setup_dir, user_input)
    return os.path.exists(full_path)


def _validate_csv_path(user_input, setup_dir):
    user_inputs = split_user_input(user_input)
    for _inp in user_inputs:
        if not _is_valid_path(_inp, setup_dir):
            return False
    return True


def initialize(path):
    """
    Initialize a ModelMapper setup for a model
    This creates the setup template that you can use to train your model.
    """

    identifier = os.path.basename(path)
    setup_dir = os.path.dirname(path)
    setup_path = os.path.join(setup_dir, f'{identifier}_setup.toml')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.toml')
    settings = load_toml(template_setup_path)['settings']
    overrides_file_name = OVERRIDES_FILE_NAME.format(identifier)
    overrides_path = os.path.join(setup_dir, overrides_file_name)

    if os.path.exists(setup_path):
        get_user_choice(f'{setup_path} already exists. Do you want to overwrite it?', choices=YES_NO_CHOICES)
    if os.path.exists(overrides_path):
        get_user_choice(f'{overrides_path} already exists. Do you want to overwrite it?', choices=YES_NO_CHOICES)

    with open(overrides_path, 'w') as the_file:
        the_file.write('# Overrides filse. You can add your overrides for any fields here.')
    output_model_file = get_user_input('Please provide the relative path to the existing ORM model file.',
                                       validate_func=_is_valid_path, setup_dir=setup_dir)
    settings['output_model_file'] = output_model_file
    output_model_path = os.path.join(setup_dir, output_model_file)
    if not _validate_file_has_start_and_end_lines(user_input=None, path=output_model_path, identifier=identifier):
        get_user_input(f'Please add the lines in a proper place in {output_model_file} code and enter continue',
                       _validate_file_has_start_and_end_lines, path=output_model_path, identifier=identifier)

    training_csvs = get_user_input(TRAINING_CSV_MSG,
                                   validate_func=_validate_csv_path, setup_dir=setup_dir)

    settings['training_csvs'] = split_user_input(training_csvs)

    write_settings(setup_path, settings)
    print(f'{setup_path} is written.')
    print(f'Please verify the generated settings and then train the model by running:\nmodelmapper run {identifier}_setup.toml')  # NOQA
